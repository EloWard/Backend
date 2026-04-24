#!/usr/bin/env node

/**
 * Op.gg Rank Scraper - Optimized Production Version
 * 
 * Extracts current, peak, and historical ranks from op.gg ranked solo/duo pages
 * 
 * Usage: node test-opgg-scraper.js <opgg_url>
 * Example: node test-opgg-scraper.js "https://op.gg/lol/summoners/na/Spankers-CN1?queue_type=SOLORANKED"
 */

// Rank hierarchy for comparison
const RANK_ORDER = {
  'IRON': 1, 'BRONZE': 2, 'SILVER': 3, 'GOLD': 4,
  'PLATINUM': 5, 'EMERALD': 6, 'DIAMOND': 7,
  'MASTER': 8, 'GRANDMASTER': 9, 'CHALLENGER': 10
};

// Non-apex division ordering. Apex tiers (Master+) have no division -- LP
// alone decides ordering within the tier, so division is ignored there.
const DIVISION_ORDER = { '4': 1, '3': 2, '2': 3, '1': 4 };

const VALID_TIERS = new Set(Object.keys(RANK_ORDER));

// Roman division strings from Riot API -> Arabic strings op.gg emits. Keeping
// one internal representation avoids silently mis-ordering cross-source data.
const ROMAN_TO_ARABIC = { I: '1', II: '2', III: '3', IV: '4' };

class OpGGScraper {
  constructor() {
    this.userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
    this.defaultHeaders = {
      'User-Agent': this.userAgent,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/avif,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'Cache-Control': 'no-cache',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Connection': 'keep-alive'
    };
  }

  async fetchWithRetry(url, options = {}, retries = 3) {
    const headers = { ...this.defaultHeaders, ...options.headers };

    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(url, {
          method: 'GET',
          headers,
          ...options
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const html = await response.text();
        return html;
      } catch (error) {
        if (i === retries - 1) throw error;

        // Wait before retry with exponential backoff
        await this.sleep(1000 * Math.pow(2, i));
      }
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async scrapeUrl(url) {
    return await this.fetchWithRetry(url);
  }

  /**
   * Extract every usable rank from an op.gg solo-queue profile page.
   *
   * op.gg renders with Next.js RSC streaming: the real per-season peak data
   * lives inside self.__next_f.push([1,"<escaped-JSON>"]) blobs, not in the
   * HTML markup. For each season the payload contains:
   *   rank_entries.high_rank_info -- peak reached mid-season (hover tooltip).
   *                                  tier="" / lp=null when op.gg didn't track
   *                                  hover-peak for that season.
   *   rank_entries.rank_info      -- end-of-season placement (always present
   *                                  when the user played ranked).
   * We emit every valid block; findHighestRank picks the overall best.
   */
  extractAllRanksFromHTML(html) {
    const payloads = this._collectNextPushPayloads(html);
    if (payloads.length === 0) return [];

    let decoded;
    try {
      decoded = JSON.parse('"' + payloads.join('') + '"');
    } catch {
      return [];
    }

    const ranks = [];
    const kinds = [['high_rank_info', 'high'], ['rank_info', 'ended']];
    for (const season of this._collectSeasonObjects(decoded)) {
      const entries = season.rank_entries || {};
      for (const [field, type] of kinds) {
        const info = entries[field];
        if (!info || typeof info.tier !== 'string' || !info.tier || info.lp == null) continue;
        const r = this._normalizeRank(info.tier, info.lp, season.season, type);
        if (r) ranks.push(r);
      }
    }
    return ranks;
  }

  _collectNextPushPayloads(html) {
    const re = /self\.__next_f\.push\(\[1,"([\s\S]*?)"\]\)/g;
    const out = [];
    let m;
    while ((m = re.exec(html)) !== null) out.push(m[1]);
    return out;
  }

  _collectSeasonObjects(text) {
    const needle = '{"season":"';
    const out = [];
    let idx = 0;
    while ((idx = text.indexOf(needle, idx)) !== -1) {
      const end = this._matchBalancedBrace(text, idx);
      if (end === -1) break;
      const slice = text.substring(idx, end + 1);
      idx = end + 1;
      let obj;
      try { obj = JSON.parse(slice); } catch { continue; }
      if (obj && typeof obj.season === 'string' && obj.rank_entries) out.push(obj);
    }
    return out;
  }

  // Given text[start] === '{', return the index of the matching '}', or -1.
  // Skips over JSON string literals so braces inside strings don't confuse us.
  _matchBalancedBrace(text, start) {
    if (text[start] !== '{') return -1;
    let depth = 0;
    for (let i = start; i < text.length; i++) {
      const c = text[i];
      if (c === '"') {
        i++;
        while (i < text.length && text[i] !== '"') {
          if (text[i] === '\\') i++;
          i++;
        }
        continue;
      }
      if (c === '{') depth++;
      else if (c === '}') {
        depth--;
        if (depth === 0) return i;
      }
    }
    return -1;
  }

  _normalizeRank(tierStr, lpStr, season, type) {
    const parts = String(tierStr).trim().toLowerCase().split(/\s+/);
    const tier = (parts[0] || '').toUpperCase();
    if (!VALID_TIERS.has(tier)) return null;
    const division = parts[1] || null;
    const lp = parseInt(String(lpStr).replace(/[^0-9]/g, ''), 10) || 0;
    return { tier, division, lp, season: (season || '').trim(), type };
  }

  isValidTier(tier) {
    return VALID_TIERS.has(tier);
  }

  // Convert a Riot API league entry ({tier, rank, leaguePoints}) into the same
  // shape the scraper emits, so it can participate in findHighestRank.
  normalizeRiotCurrentRank(current) {
    if (!current || !current.tier) return null;
    const tier = String(current.tier).toUpperCase();
    if (!VALID_TIERS.has(tier)) return null;
    const rawDiv = current.rank ? String(current.rank).toUpperCase() : null;
    const division = rawDiv ? (ROMAN_TO_ARABIC[rawDiv] || rawDiv) : null;
    return {
      tier,
      division,
      lp: parseInt(current.leaguePoints, 10) || 0,
      season: 'CURRENT',
      type: 'current_riot'
    };
  }

  findHighestRank(ranks) {
    if (!ranks || ranks.length === 0) return null;
    return ranks.reduce((best, cur) => (this._isRankHigher(cur, best) ? cur : best));
  }

  // Mirrors isRankHigher in rank-worker.js so seeding and the live rank-write
  // path agree on ordering.
  _isRankHigher(a, b) {
    if (!b || !b.tier) return true;
    if (!a || !a.tier) return false;
    const aTier = RANK_ORDER[a.tier] || 0;
    const bTier = RANK_ORDER[b.tier] || 0;
    if (aTier === 0 || bTier === 0) return false;
    if (aTier !== bTier) return aTier > bTier;
    if (aTier >= 8) return (a.lp || 0) > (b.lp || 0); // Master+: LP only.
    const aDiv = DIVISION_ORDER[a.division] || 0;
    const bDiv = DIVISION_ORDER[b.division] || 0;
    if (aDiv !== bDiv) return aDiv > bDiv;
    return (a.lp || 0) > (b.lp || 0);
  }
}

// Test function
async function testOpGGScraper(opggUrl) {
  const scraper = new OpGGScraper();

  try {
    const html = await scraper.scrapeUrl(opggUrl);
    const allRanks = scraper.extractAllRanksFromHTML(html);
    
    if (allRanks.length === 0) {
      console.log('No ranks found');
      return null;
    }

    // Find peak rank
    const peakRank = scraper.findHighestRank(allRanks);
    const division = (peakRank.tier === 'MASTER' || peakRank.tier === 'GRANDMASTER' || peakRank.tier === 'CHALLENGER') ? null : peakRank.division;
    
    const result = {
      tier: peakRank.tier,
      division: division,
      lp: peakRank.lp
    };

    console.log(result);
    return result;
  } catch (error) {
    console.error('Error:', error.message);
    return null;
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log('Usage: node test-opgg-scraper.js <opgg_url>');
    process.exit(1);
  }
  
  const opggUrl = args[0];
  
  if (!opggUrl.includes('op.gg')) {
    console.error('Invalid URL: Must be an op.gg URL');
    process.exit(1);
  }
  
  try {
    const result = await testOpGGScraper(opggUrl);
    process.exit(result ? 0 : 1);
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { OpGGScraper, testOpGGScraper };
