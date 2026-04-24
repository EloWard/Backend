#!/usr/bin/env node
/**
 * ONE-TIME PEAK-RANK REPAIR — DELETE AFTER VERIFICATION.
 *
 * Walks every row in lol_ranks, re-scrapes the user's op.gg profile with the
 * FIXED JSON-parsing scraper (test-opgg-scraper.js), and writes
 * peak_rank_tier / peak_rank_division / peak_lp via direct D1 UPDATE.
 *
 * Mode (default: MONOTONIC):
 *   Only writes if the freshly computed peak is STRICTLY HIGHER than what's
 *   in the DB. This repairs the Spankers-class bug (stored peak wrongly
 *   below true peak because the old scraper silently demoted it) without
 *   destroying rows where the stored peak is legitimately higher than what
 *   op.gg's high_rank_info currently exposes -- op.gg doesn't always have
 *   hover-peak data for a season, so end-of-season rank_info can understate
 *   a mid-season LP peak that was captured correctly in the original
 *   seeding. Monotonic is safer and still fixes the bug we care about.
 *
 * Mode --force:
 *   Overwrites unconditionally, even if the new value is lower. Use only if
 *   you've inspected dry-run output and are willing to lose some legitimate
 *   higher peaks. Bypasses the rank-worker monotonic guard on purpose.
 *
 * Usage:
 *   node one-time-peak-repair.js --dry-run            # preview, no writes
 *   node one-time-peak-repair.js                      # live, monotonic
 *   node one-time-peak-repair.js --force              # live, overwrite all
 *   node one-time-peak-repair.js --reset              # wipe checkpoint
 *   node one-time-peak-repair.js --force --dry-run    # preview force mode
 *
 * Artifacts created next to this file (delete when done):
 *   .peak-repair-progress.json   — resumable checkpoint, written every user
 *   peak-repair.log              — human-readable audit log with before/after
 */

const fs = require('fs');
const path = require('path');
const { OpGGScraper } = require('./test-opgg-scraper.js');

// ---------- env ----------

// .env.local lives at Backend/.env.local (one level up from this script).
const ENV_PATH = path.join(__dirname, '..', '.env.local');
if (!fs.existsSync(ENV_PATH)) {
  console.error(`FATAL: missing env file at ${ENV_PATH}`);
  console.error('Expected CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_DATABASE_ID / CLOUDFLARE_API_TOKEN.');
  process.exit(1);
}
for (const line of fs.readFileSync(ENV_PATH, 'utf8').split('\n')) {
  const t = line.trim();
  if (!t || t.startsWith('#')) continue;
  const eq = t.indexOf('=');
  if (eq <= 0) continue;
  process.env[t.slice(0, eq).trim()] = t.slice(eq + 1).trim();
}

const { CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_DATABASE_ID, CLOUDFLARE_API_TOKEN } = process.env;
if (!CLOUDFLARE_ACCOUNT_ID || !CLOUDFLARE_DATABASE_ID || !CLOUDFLARE_API_TOKEN) {
  console.error('FATAL: one of CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_DATABASE_ID / CLOUDFLARE_API_TOKEN is missing.');
  process.exit(1);
}

const D1_ENDPOINT =
  `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}` +
  `/d1/database/${CLOUDFLARE_DATABASE_ID}/query`;

// ---------- config ----------

// Region mapping must stay in sync with riotauth-worker.ts REGION_MAPPING.
const REGION_MAPPING = {
  na1: 'na', euw1: 'euw', eun1: 'eune', kr: 'kr', br1: 'br',
  jp1: 'jp', la1: 'lan', la2: 'las', oc1: 'oce', tr1: 'tr',
  ru: 'ru', me1: 'me', sg2: 'sea', sea: 'sea', tw2: 'tw', vn2: 'vn'
};

// op.gg doesn't publish rate limits, so we pace conservatively. Randomized
// delay trades peak-throughput for being less bot-obvious.
const MIN_DELAY_MS = 2500;
const MAX_DELAY_MS = 5500;

// Per-user retry budget for transient errors (network hiccups, 5xx). 429 is
// handled separately with a longer pause.
const MAX_RETRIES = 3;
const RATE_LIMIT_BACKOFF_MS = 60_000;

// Abort threshold: 5 straight 403s means op.gg has started serving
// bot-detection pages, not a blip. Continuing digs the hole deeper.
const MAX_CONSECUTIVE_403 = 5;

const PROGRESS_FILE = path.join(__dirname, '.peak-repair-progress.json');
const LOG_FILE = path.join(__dirname, 'peak-repair.log');

// ---------- helpers ----------

async function d1(sql, params = []) {
  const res = await fetch(D1_ENDPOINT, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ sql, params })
  });
  if (!res.ok) throw new Error(`D1 ${res.status}: ${await res.text()}`);
  const json = await res.json();
  if (!json.success) throw new Error(`D1 query failed: ${json.errors?.[0]?.message || 'unknown'}`);
  return json.result?.[0]?.results || [];
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Mirrors formatPeakRankForDatabase in riotauth-worker.ts so repaired rows
// land in the exact same shape a live /riot/auth seed would produce.
function formatPeakForDb(peak) {
  const tier = String(peak.tier).toUpperCase();
  let division = null;
  let lp = parseInt(peak.lp, 10) || 0;
  if (tier === 'MASTER' || tier === 'GRANDMASTER' || tier === 'CHALLENGER') {
    division = 'I';
  } else if (tier === 'UNRANKED') {
    division = null;
    lp = 0;
  } else if (peak.division && String(peak.division).trim() !== '') {
    const map = { '1': 'I', '2': 'II', '3': 'III', '4': 'IV' };
    division = map[peak.division] || String(peak.division).toUpperCase();
  }
  return { tier, division, lp };
}

function buildOpGgUrl(riotId, region) {
  const opggRegion = REGION_MAPPING[(region || '').toLowerCase()];
  if (!opggRegion) throw new Error(`unknown_region:${region}`);
  const [name = '', tag] = (riotId || '').split('#');
  return (
    `https://op.gg/lol/summoners/${opggRegion}/` +
    `${encodeURIComponent(name)}-${tag || region.toUpperCase()}?queue_type=SOLORANKED`
  );
}

const logLines = [];
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  logLines.push(line);
  if (logLines.length >= 20) flushLog();
}
function flushLog() {
  if (logLines.length === 0) return;
  fs.appendFileSync(LOG_FILE, logLines.join('\n') + '\n');
  logLines.length = 0;
}

function loadProgress(reset) {
  if (reset || !fs.existsSync(PROGRESS_FILE)) return { completed: [], failed: [] };
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
  } catch {
    log('WARN: progress file unreadable, starting fresh');
    return { completed: [], failed: [] };
  }
}
function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ---------- main ----------

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const reset = process.argv.includes('--reset');
  const force = process.argv.includes('--force');

  log(`START mode=${dryRun ? 'DRY-RUN' : 'LIVE'} strategy=${force ? 'FORCE' : 'MONOTONIC'} reset=${reset}`);

  const progress = loadProgress(reset);
  const completed = new Set(progress.completed);
  const failed = new Set(progress.failed.map((f) => f.puuid));
  log(`checkpoint: ${completed.size} completed, ${failed.size} failed (carried over)`);

  // Pull every row once. lol_ranks is small (single-digit thousands); one
  // SELECT is simpler than pagination and avoids drift while we run.
  const users = await d1(
    'SELECT riot_puuid, riot_id, region, twitch_username, ' +
    'rank_tier, rank_division, lp, ' +
    'peak_rank_tier, peak_rank_division, peak_lp ' +
    'FROM lol_ranks ORDER BY riot_puuid'
  );
  log(`loaded ${users.length} users from lol_ranks`);

  // Skip rows we already processed successfully. Failed rows get retried every
  // run unless --reset is passed (letting transient failures resolve naturally).
  const queue = users.filter((u) => !completed.has(u.riot_puuid));
  log(`${queue.length} users to process this run`);
  if (queue.length === 0) {
    log('DONE (nothing to do)');
    flushLog();
    return;
  }

  const scraper = new OpGGScraper();
  const stats = {
    rewrote: 0,
    unchanged: 0,
    rejected_not_higher: 0,
    skipped_404: 0,
    skipped_no_data: 0,
    skipped_bad_region: 0,
    skipped_bad_id: 0,
    failed: 0,
    start: Date.now()
  };
  let consecutive403 = 0;

  for (let i = 0; i < queue.length; i++) {
    const u = queue[i];
    const tag = `[${i + 1}/${queue.length}] ${u.twitch_username || '?'} (${u.riot_id || '?'})`;

    if (!u.riot_id || !u.riot_id.includes('#')) {
      log(`${tag} -> SKIP: missing or malformed riot_id`);
      progress.failed.push({ puuid: u.riot_puuid, reason: 'bad_riot_id' });
      stats.skipped_bad_id++;
      saveProgress(progress);
      continue;
    }

    let attempt = 0;
    let settled = false;
    while (!settled) {
      try {
        const url = buildOpGgUrl(u.riot_id, u.region);
        const html = await scraper.scrapeUrl(url);
        consecutive403 = 0; // got a 200

        const candidates = scraper.extractAllRanksFromHTML(html);
        // Fold in the stored current rank so the repaired peak is never lower
        // than the user's live rank. Uses DB values to avoid a Riot API call
        // per user -- Riot's data already flows into lol_ranks via normal
        // refresh paths and is the same source seedPeakRankAsync would use.
        const current = scraper.normalizeRiotCurrentRank({
          tier: u.rank_tier,
          rank: u.rank_division,
          leaguePoints: u.lp
        });
        if (current) candidates.push(current);

        if (candidates.length === 0) {
          log(`${tag} -> SKIP: op.gg has no rank data and no stored current`);
          progress.failed.push({ puuid: u.riot_puuid, reason: 'no_rank_data' });
          stats.skipped_no_data++;
          settled = true;
          break;
        }

        const peak = scraper.findHighestRank(candidates);
        if (!peak) {
          log(`${tag} -> SKIP: could not determine peak`);
          progress.failed.push({ puuid: u.riot_puuid, reason: 'peak_undetermined' });
          stats.skipped_no_data++;
          settled = true;
          break;
        }

        const fmt = formatPeakForDb(peak);
        const before = `${u.peak_rank_tier || 'NULL'} ${u.peak_rank_division || 'NULL'} ${u.peak_lp ?? 'NULL'}LP`;
        const after = `${fmt.tier} ${fmt.division || 'NULL'} ${fmt.lp}LP`;
        const unchanged =
          u.peak_rank_tier === fmt.tier &&
          (u.peak_rank_division || null) === (fmt.division || null) &&
          (u.peak_lp ?? 0) === fmt.lp;

        // In monotonic mode, reuse the scraper's own findHighestRank to check
        // whether the new peak is strictly higher than the stored peak. This
        // keeps ordering semantics identical to the production seeding path.
        let shouldWrite;
        if (unchanged) {
          shouldWrite = false;
        } else if (force) {
          shouldWrite = true;
        } else {
          // Represent the stored peak in the scraper's candidate shape. Apex
          // tiers store division as 'I' but the scraper's comparator expects
          // Arabic '1'; normalizeRiotCurrentRank handles both.
          const storedCandidate = scraper.normalizeRiotCurrentRank({
            tier: u.peak_rank_tier,
            rank: u.peak_rank_division,
            leaguePoints: u.peak_lp || 0
          });
          const winner = storedCandidate
            ? scraper.findHighestRank([peak, storedCandidate])
            : peak;
          // winner === peak iff new peak is >= stored, but reduce() returns
          // the first of equals; since unchanged is already handled above,
          // winner === peak here means strictly higher.
          shouldWrite = winner === peak;
        }

        if (dryRun) {
          const label = unchanged ? 'NOOP' : shouldWrite ? 'REWRITE' : 'REJECT_NOT_HIGHER';
          log(`${tag} -> ${label}: ${before} -> ${after}`);
        } else if (unchanged) {
          log(`${tag} -> ALREADY CORRECT: ${after}`);
          stats.unchanged++;
        } else if (!shouldWrite) {
          log(`${tag} -> SKIP NOT HIGHER: stored ${before} beats computed ${after}`);
          stats.rejected_not_higher++;
        } else {
          await d1(
            'UPDATE lol_ranks SET peak_rank_tier = ?, peak_rank_division = ?, peak_lp = ? WHERE riot_puuid = ?',
            [fmt.tier, fmt.division, fmt.lp, u.riot_puuid]
          );
          log(`${tag} -> REWROTE: ${before} -> ${after}`);
          stats.rewrote++;
        }

        progress.completed.push(u.riot_puuid);
        settled = true;
      } catch (err) {
        const msg = err.message || String(err);

        if (msg.startsWith('unknown_region')) {
          log(`${tag} -> SKIP: ${msg}`);
          progress.failed.push({ puuid: u.riot_puuid, reason: msg });
          stats.skipped_bad_region++;
          settled = true;
          break;
        }

        if (msg.includes('404')) {
          log(`${tag} -> SKIP: op.gg profile not found (keeping existing peak)`);
          progress.failed.push({ puuid: u.riot_puuid, reason: 'op.gg_404' });
          stats.skipped_404++;
          consecutive403 = 0;
          settled = true;
          break;
        }

        if (msg.includes('403')) {
          consecutive403++;
          log(`${tag} -> 403 (consecutive=${consecutive403})`);
          if (consecutive403 >= MAX_CONSECUTIVE_403) {
            log(`ABORT: ${MAX_CONSECUTIVE_403} straight 403s, likely bot-detected. Stopping.`);
            progress.failed.push({ puuid: u.riot_puuid, reason: '403_bot_detection' });
            saveProgress(progress);
            flushLog();
            process.exit(2);
          }
          await sleep(RATE_LIMIT_BACKOFF_MS);
          continue; // retry same user without incrementing attempt
        }

        if (msg.includes('429')) {
          log(`${tag} -> 429 rate-limited, sleeping ${RATE_LIMIT_BACKOFF_MS / 1000}s then retrying`);
          await sleep(RATE_LIMIT_BACKOFF_MS);
          continue; // retry same user
        }

        attempt++;
        if (attempt >= MAX_RETRIES) {
          log(`${tag} -> FAILED after ${MAX_RETRIES} retries: ${msg}`);
          progress.failed.push({ puuid: u.riot_puuid, reason: msg });
          stats.failed++;
          settled = true;
          break;
        }
        const backoff = 2000 * attempt;
        log(`${tag} -> ERR ${msg} | retry ${attempt}/${MAX_RETRIES} after ${backoff}ms`);
        await sleep(backoff);
      }
    }

    saveProgress(progress);

    if (i < queue.length - 1) {
      const delay = MIN_DELAY_MS + Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS);
      await sleep(delay);
    }
  }

  const elapsedMin = ((Date.now() - stats.start) / 60_000).toFixed(1);
  log('DONE');
  log(`  rewrote:               ${stats.rewrote}`);
  log(`  unchanged:             ${stats.unchanged}`);
  log(`  rejected (not higher): ${stats.rejected_not_higher}`);
  log(`  skipped (404):         ${stats.skipped_404}`);
  log(`  skipped (no data):     ${stats.skipped_no_data}`);
  log(`  skipped (region):      ${stats.skipped_bad_region}`);
  log(`  skipped (riot_id):     ${stats.skipped_bad_id}`);
  log(`  failed (transient):    ${stats.failed}`);
  log(`  elapsed:               ${elapsedMin} min`);
  flushLog();
}

// Flush on crash/exit so partial progress isn't lost.
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    log(`received ${sig}, flushing and exiting`);
    flushLog();
    process.exit(130);
  });
}

main().catch((err) => {
  log(`FATAL: ${err.stack || err.message || err}`);
  flushLog();
  process.exit(1);
});
