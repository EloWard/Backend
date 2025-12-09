/**
 * EloWard Channel Statistics Worker
 *
 * Runs daily at 07:10 UTC to compute all-time channel statistics
 * Aggregates viewer data from channel_viewers_daily and lol_ranks tables
 * Updates channel_stats_cache and channel_daily_snapshots tables
 */

const statsWorker = {
  /**
   * Main fetch handler (for manual testing/health checks)
   */
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        worker: 'eloward-stats',
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Manual trigger endpoint (for testing)
    if (url.pathname === '/trigger' && request.method === 'POST') {
      try {
        await runDailyAggregation(env, ctx);
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (error) {
        return new Response(JSON.stringify({
          error: error.message,
          stack: error.stack
        }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }

    return new Response('EloWard Stats Worker', { status: 200 });
  },

  /**
   * Scheduled cron handler - runs daily at 07:10 UTC
   */
  async scheduled(event, env, ctx) {
    try {
      console.log(`[StatsCron] Started at ${new Date().toISOString()}`);
      await runDailyAggregation(env, ctx);
      console.log(`[StatsCron] Completed at ${new Date().toISOString()}`);
    } catch (error) {
      console.error(`[StatsCron] Error during scheduled aggregation:`, error);
      // Don't throw - let the cron complete and try again next time
    }
  }
};

export default statsWorker;

/**
 * Main aggregation function - processes all channels
 * Runs every 3 hours and updates stats for the current day
 */
async function runDailyAggregation(env, ctx) {
  const currentDay = getCurrentWindow();

  console.log(`[StatsCron] Processing stat_date=${currentDay}`);

  // Get all leaderboard streamers (from previous run) to exclude them as viewers
  // This prevents streamers from counting as viewers for other channels
  const streamerUsernames = await getLeaderboardStreamerUsernames(env);
  console.log(`[StatsCron] Excluding ${streamerUsernames.size} leaderboard streamers from viewer counts`);

  // Get all unique channels that have EVER had viewers
  const channelsResult = await env.DB.prepare(`
    SELECT DISTINCT channel_twitch_id
    FROM channel_viewers_daily
  `).all();

  const channels = channelsResult.results || [];
  console.log(`[StatsCron] Found ${channels.length} channels with lifetime viewer data`);

  if (channels.length === 0) {
    console.log(`[StatsCron] No channels to process, exiting`);
    return;
  }

  // Process channels in batches to avoid timeout
  const BATCH_SIZE = 50;
  let processed = 0;
  let errors = 0;

  for (let i = 0; i < channels.length; i += BATCH_SIZE) {
    const batch = channels.slice(i, i + BATCH_SIZE);

    // Process batch in parallel
    const results = await Promise.allSettled(
      batch.map(({ channel_twitch_id }) =>
        computeChannelStats(env, channel_twitch_id, currentDay, streamerUsernames)
      )
    );

    // Count successes and failures
    results.forEach((result, idx) => {
      if (result.status === 'fulfilled') {
        processed++;
      } else {
        errors++;
        console.error(`[StatsCron] Error processing ${batch[idx].channel_twitch_id}:`, result.reason);
      }
    });

    console.log(`[StatsCron] Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(channels.length / BATCH_SIZE)} complete (${processed} processed, ${errors} errors)`);
  }

  console.log(`[StatsCron] Aggregation complete: ${processed} channels processed, ${errors} errors`);
}

/**
 * Compute all-time stats for a single channel
 * @param {Set<string>} streamerUsernames - Set of leaderboard streamer usernames to exclude
 */
async function computeChannelStats(env, channelTwitchId, statDate, streamerUsernames) {
  // 1. Get all unique viewers for this channel (ALL TIME)
  const uniqueViewersResult = await env.DB.prepare(`
    SELECT DISTINCT riot_puuid
    FROM channel_viewers_daily
    WHERE channel_twitch_id = ?
  `).bind(channelTwitchId).all();

  const viewerPuuids = uniqueViewersResult.results || [];

  if (viewerPuuids.length === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has no viewers, skipping`);
    return;
  }

  // 2. Get rank data for all viewers, respecting show_peak preference
  // Note: Some viewers may not have rank data - we'll filter them out
  const rankedViewers = await getRankedViewers(env, viewerPuuids.map(v => v.riot_puuid));

  // 3. Get channel owner's PUUID and filter out:
  //    - Channel owner (self)
  //    - Any leaderboard streamers
  // This ensures only genuine non-streamer viewers are counted
  const ownerPuuid = await getChannelOwnerPuuid(env, channelTwitchId);
  const filteredViewers = rankedViewers.filter(v => {
    // Exclude channel owner
    if (ownerPuuid && v.riot_puuid === ownerPuuid) return false;
    // Exclude leaderboard streamers
    if (streamerUsernames.has(v.twitch_username)) return false;
    return true;
  });

  // 4. Use filtered count as the total (only non-streamer viewers)
  const totalViewers = filteredViewers.length;

  if (totalViewers === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has viewers but none qualify (excluding self and streamers), skipping`);
    // Update cache to show channel exists but has no qualified viewers
    await env.DB.prepare(`
      INSERT INTO channel_stats_cache
        (channel_twitch_id, total_unique_viewers, avg_rank_score, avg_rank_tier,
         avg_rank_division, top_viewers_json, last_updated, last_computed_stat_date, is_eligible)
      VALUES (?, ?, NULL, NULL, NULL, '[]', ?, ?, 0)
      ON CONFLICT(channel_twitch_id) DO UPDATE SET
        total_unique_viewers = excluded.total_unique_viewers,
        avg_rank_score = excluded.avg_rank_score,
        avg_rank_tier = excluded.avg_rank_tier,
        avg_rank_division = excluded.avg_rank_division,
        top_viewers_json = excluded.top_viewers_json,
        last_updated = excluded.last_updated,
        last_computed_stat_date = excluded.last_computed_stat_date,
        is_eligible = excluded.is_eligible
    `).bind(
      channelTwitchId,
      0, // totalViewers = 0 since no one qualifies
      Math.floor(Date.now() / 1000),
      statDate
    ).run();
    return;
  }

  // 5. Calculate scores for each viewer (excluding self and streamers)
  const viewerScores = filteredViewers.map(viewer => ({
    puuid: viewer.riot_puuid,
    twitch_username: viewer.twitch_username,
    tier: viewer.effective_tier,
    division: viewer.effective_division,
    lp: viewer.effective_lp || 0,
    score: calculateRankScore(viewer.effective_tier, viewer.effective_division, viewer.effective_lp || 0)
  }));

  // 6. Calculate average and median scores
  const avgScore = viewerScores.reduce((sum, v) => sum + v.score, 0) / viewerScores.length;
  const avgRank = scoreToRank(avgScore); // This now returns { tier, division, lp }

  // Calculate median score (middle value when sorted)
  const sortedScores = [...viewerScores].sort((a, b) => a.score - b.score);
  const medianScore = sortedScores.length % 2 === 0
    ? (sortedScores[sortedScores.length / 2 - 1].score + sortedScores[sortedScores.length / 2].score) / 2
    : sortedScores[Math.floor(sortedScores.length / 2)].score;
  const medianRank = scoreToRank(medianScore);

  // 7. Get top 10 viewers by score (excluding self and streamers)
  const top10 = viewerScores
    .sort((a, b) => b.score - a.score)
    .slice(0, 10)
    .map(v => ({
      twitch_username: v.twitch_username,
      rank_tier: v.tier,
      rank_division: v.division,
      lp: v.lp,
      score: Math.round(v.score * 10) / 10 // Round to 1 decimal
    }));

  // 8. Update channel_stats_cache
  await env.DB.prepare(`
    INSERT INTO channel_stats_cache
      (channel_twitch_id, total_unique_viewers, avg_rank_score, avg_rank_tier,
       avg_rank_division, avg_lp, median_rank_score, median_rank_tier,
       median_rank_division, median_lp, top_viewers_json, last_updated, last_computed_stat_date, is_eligible)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(channel_twitch_id) DO UPDATE SET
      total_unique_viewers = excluded.total_unique_viewers,
      avg_rank_score = excluded.avg_rank_score,
      avg_rank_tier = excluded.avg_rank_tier,
      avg_rank_division = excluded.avg_rank_division,
      avg_lp = excluded.avg_lp,
      median_rank_score = excluded.median_rank_score,
      median_rank_tier = excluded.median_rank_tier,
      median_rank_division = excluded.median_rank_division,
      median_lp = excluded.median_lp,
      top_viewers_json = excluded.top_viewers_json,
      last_updated = excluded.last_updated,
      last_computed_stat_date = excluded.last_computed_stat_date,
      is_eligible = excluded.is_eligible
  `).bind(
    channelTwitchId,
    totalViewers,
    avgScore,
    avgRank.tier,
    avgRank.division,
    avgRank.lp,
    medianScore,
    medianRank.tier,
    medianRank.division,
    medianRank.lp,
    JSON.stringify(top10),
    Math.floor(Date.now() / 1000),
    statDate,
    totalViewers >= 10 ? 1 : 0
  ).run();

  // 9. Compute daily snapshot for yesterday
  await computeDailySnapshot(env, channelTwitchId, statDate, avgScore, avgRank.lp, medianScore, medianRank.lp, totalViewers, streamerUsernames);

  console.log(`[StatsCron] ${channelTwitchId}: ${totalViewers} non-streamer viewers, avg=${avgRank.tier}${avgRank.division ? ' ' + avgRank.division : ''}`);
}

/**
 * Compute daily snapshot metrics for a specific day
 * @param {Set<string>} streamerUsernames - Set of leaderboard streamer usernames to exclude
 */
async function computeDailySnapshot(env, channelTwitchId, statDate, alltimeAvgScore, alltimeAvgLp, alltimeMedianScore, alltimeMedianLp, alltimeViewerCount, streamerUsernames) {
  // Get viewers who qualified on THIS SPECIFIC DAY
  const dailyViewersResult = await env.DB.prepare(`
    SELECT DISTINCT riot_puuid
    FROM channel_viewers_daily
    WHERE channel_twitch_id = ? AND stat_date = ?
  `).bind(channelTwitchId, statDate).all();

  const dailyPuuids = dailyViewersResult.results || [];

  if (dailyPuuids.length === 0) {
    // No viewers on this specific day, don't create snapshot
    return;
  }

  // Get ranks for viewers who watched THIS DAY
  const dailyRankedViewers = await getRankedViewers(env, dailyPuuids.map(v => v.riot_puuid));

  // Filter out channel owner and leaderboard streamers from daily statistics
  const ownerPuuid = await getChannelOwnerPuuid(env, channelTwitchId);
  const filteredDailyViewers = dailyRankedViewers.filter(v => {
    // Exclude channel owner
    if (ownerPuuid && v.riot_puuid === ownerPuuid) return false;
    // Exclude leaderboard streamers
    if (streamerUsernames.has(v.twitch_username)) return false;
    return true;
  });

  if (filteredDailyViewers.length === 0) {
    // No ranked viewers on this day (excluding self and streamers)
    return;
  }

  // Calculate daily average and median scores (excluding self and streamers)
  const dailyScores = filteredDailyViewers.map(v =>
    calculateRankScore(v.effective_tier, v.effective_division, v.effective_lp || 0)
  );
  const dailyAvgScore = dailyScores.reduce((sum, s) => sum + s, 0) / dailyScores.length;
  const dailyRank = scoreToRank(dailyAvgScore);
  const dailyAvgLp = dailyRank.lp;

  // Calculate daily median
  const sortedDailyScores = [...dailyScores].sort((a, b) => a - b);
  const dailyMedianScore = sortedDailyScores.length % 2 === 0
    ? (sortedDailyScores[sortedDailyScores.length / 2 - 1] + sortedDailyScores[sortedDailyScores.length / 2]) / 2
    : sortedDailyScores[Math.floor(sortedDailyScores.length / 2)];
  const dailyMedianRank = scoreToRank(dailyMedianScore);
  const dailyMedianLp = dailyMedianRank.lp;

  // Insert/update daily snapshot
  await env.DB.prepare(`
    INSERT INTO channel_daily_snapshots
      (stat_date, channel_twitch_id, daily_viewer_count, daily_avg_rank_score, daily_avg_lp,
       daily_median_rank_score, daily_median_lp, alltime_avg_rank_score, alltime_avg_lp,
       alltime_median_rank_score, alltime_median_lp, alltime_viewer_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stat_date, channel_twitch_id) DO UPDATE SET
      daily_viewer_count = excluded.daily_viewer_count,
      daily_avg_rank_score = excluded.daily_avg_rank_score,
      daily_avg_lp = excluded.daily_avg_lp,
      daily_median_rank_score = excluded.daily_median_rank_score,
      daily_median_lp = excluded.daily_median_lp,
      alltime_avg_rank_score = excluded.alltime_avg_rank_score,
      alltime_avg_lp = excluded.alltime_avg_lp,
      alltime_median_rank_score = excluded.alltime_median_rank_score,
      alltime_median_lp = excluded.alltime_median_lp,
      alltime_viewer_count = excluded.alltime_viewer_count
  `).bind(
    statDate,
    channelTwitchId,
    dailyPuuids.length,
    dailyAvgScore,
    dailyAvgLp,
    dailyMedianScore,
    dailyMedianLp,
    alltimeAvgScore,
    alltimeAvgLp,
    alltimeMedianScore,
    alltimeMedianLp,
    alltimeViewerCount
  ).run();
}

/**
 * Get all channel usernames that are currently on the leaderboard
 * These are streamers who shouldn't count as viewers for other channels
 * Returns a Set of twitch usernames for efficient lookup
 */
async function getLeaderboardStreamerUsernames(env) {
  const result = await env.DB.prepare(`
    SELECT channel_twitch_id
    FROM channel_stats_cache
    WHERE is_eligible = 1
  `).all();

  return new Set((result.results || []).map(r => r.channel_twitch_id));
}

/**
 * Get the channel owner's riot_puuid (if they have a linked League account)
 * Returns null if the channel owner doesn't have a linked account
 */
async function getChannelOwnerPuuid(env, channelTwitchId) {
  const result = await env.DB.prepare(`
    SELECT riot_puuid
    FROM lol_ranks
    WHERE twitch_username = ?
  `).bind(channelTwitchId).first();

  return result ? result.riot_puuid : null;
}

/**
 * Get rank data for viewers in batches (respects show_peak flag)
 * Handles large viewer lists by batching IN clauses
 */
async function getRankedViewers(env, puuids) {
  if (puuids.length === 0) {
    return [];
  }

  const BATCH_SIZE = 500; // SQLite limit is 999, use 500 for safety
  const allViewers = [];

  for (let i = 0; i < puuids.length; i += BATCH_SIZE) {
    const batch = puuids.slice(i, i + BATCH_SIZE);
    const placeholders = batch.map(() => '?').join(',');

    const result = await env.DB.prepare(`
      SELECT
        riot_puuid,
        twitch_username,
        CASE
          WHEN show_peak = 1 AND peak_rank_tier IS NOT NULL
            THEN peak_rank_tier
          ELSE rank_tier
        END as effective_tier,
        CASE
          WHEN show_peak = 1 AND peak_rank_tier IS NOT NULL
            THEN peak_rank_division
          ELSE rank_division
        END as effective_division,
        CASE
          WHEN show_peak = 1 AND peak_rank_tier IS NOT NULL
            THEN peak_lp
          ELSE lp
        END as effective_lp
      FROM lol_ranks
      WHERE riot_puuid IN (${placeholders})
    `).bind(...batch).all();

    allViewers.push(...(result.results || []));
  }

  return allViewers;
}

/**
 * Calculate numeric score from League of Legends rank
 *
 * Clean linear system: 100 LP = 100 points
 * - Iron IV 0 LP = 0
 * - Each division = 100 points (0-99 LP)
 * - Each tier (Iron-Diamond) = 400 points (4 divisions)
 *
 * Master+ tiers (no divisions):
 * - Diamond I 100 LP promotes to Master 0 LP
 * - Master starts at 2800 (Diamond I maxes at 2799)
 * - All Master+ ranks use continuous LP (no divisions)
 * - GM/Challenger are competitive slots but we treat them as LP thresholds for calculation
 */
function calculateRankScore(tier, division, lp) {
  const tierBase = {
    'IRON': 0,        // 0-399
    'BRONZE': 400,    // 400-799
    'SILVER': 800,    // 800-1199
    'GOLD': 1200,     // 1200-1599
    'PLATINUM': 1600, // 1600-1999
    'EMERALD': 2000,  // 2000-2399
    'DIAMOND': 2400,  // 2400-2799 (Diamond I 99 LP = 2799)
    'MASTER': 2800,   // 2800+ (no upper bound, but GM typically starts ~3000+)
    'GRANDMASTER': 2800, // Same as Master since they share LP pool
    'CHALLENGER': 2800   // Same as Master since they share LP pool
  };

  const divisionOffset = {
    'IV': 0,
    'III': 100,
    'II': 200,
    'I': 300
  };

  if (!tier) {
    return 0;
  }

  const tierUpper = tier.toUpperCase();
  let score = tierBase[tierUpper] || 0;

  // For Iron-Diamond: add division offset
  if (division && divisionOffset[division.toUpperCase()] !== undefined) {
    score += divisionOffset[division.toUpperCase()];
  }

  // Add LP (full value)
  score += (lp || 0);

  return score;
}

/**
 * Convert numeric score back to human-readable rank with LP
 *
 * Score ranges:
 * - 0-2799: Iron IV - Diamond I (with divisions)
 * - 2800+: Master/Grandmaster/Challenger (continuous LP, no divisions)
 *
 * For display purposes, we use rough LP thresholds:
 * - Master: 2800-2999 (0-199 LP)
 * - Grandmaster: 3000-3499 (200-699 LP, typical threshold ~200)
 * - Challenger: 3500+ (500+ LP, typical threshold ~500)
 */
function scoreToRank(score) {
  // Master+ tiers (2800+)
  if (score >= 2800) {
    const lpInMaster = score - 2800;

    // Rough thresholds for display (actual GM/Challenger are competitive slots)
    if (lpInMaster >= 700) {
      return { tier: 'CHALLENGER', division: null, lp: Math.round(lpInMaster) };
    } else if (lpInMaster >= 200) {
      return { tier: 'GRANDMASTER', division: null, lp: Math.round(lpInMaster) };
    } else {
      return { tier: 'MASTER', division: null, lp: Math.round(lpInMaster) };
    }
  }

  // Iron-Diamond tiers (0-2799)
  const tiers = [
    { name: 'DIAMOND', min: 2400 },
    { name: 'EMERALD', min: 2000 },
    { name: 'PLATINUM', min: 1600 },
    { name: 'GOLD', min: 1200 },
    { name: 'SILVER', min: 800 },
    { name: 'BRONZE', min: 400 },
    { name: 'IRON', min: 0 }
  ];

  for (const tier of tiers) {
    if (score >= tier.min) {
      const scoreInTier = score - tier.min;

      // Each division is 100 points (0-99 LP)
      const divisionIndex = Math.floor(scoreInTier / 100);
      const lpInDivision = Math.round(scoreInTier % 100);

      const divisions = ['IV', 'III', 'II', 'I'];
      const division = divisions[Math.min(divisionIndex, 3)];

      return { tier: tier.name, division, lp: lpInDivision };
    }
  }

  return { tier: 'IRON', division: 'IV', lp: 0 };
}

/**
 * Get current stat_date window
 * Windows reset at 07:00 UTC, so this accounts for that
 * If it's before 07:00 UTC, we're still in yesterday's window
 */
function getCurrentWindow() {
  const now = new Date();
  const utcHour = now.getUTCHours();

  // If before 07:00 UTC, we're still in yesterday's window
  if (utcHour < 7) {
    now.setUTCDate(now.getUTCDate() - 1);
  }

  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}
