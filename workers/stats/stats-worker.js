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
 */
async function runDailyAggregation(env, ctx) {
  const yesterday = getYesterdayWindow();

  console.log(`[StatsCron] Processing stat_date=${yesterday}`);

  // Get all unique channels that have EVER had viewers (not just yesterday)
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
        computeChannelStats(env, channel_twitch_id, yesterday)
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
 */
async function computeChannelStats(env, channelTwitchId, statDate) {
  // 1. Get all unique viewers for this channel (ALL TIME)
  const uniqueViewersResult = await env.DB.prepare(`
    SELECT DISTINCT riot_puuid
    FROM channel_viewers_daily
    WHERE channel_twitch_id = ?
  `).bind(channelTwitchId).all();

  const viewerPuuids = uniqueViewersResult.results || [];
  const totalViewers = viewerPuuids.length;

  if (totalViewers === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has no viewers, skipping`);
    return;
  }

  // 2. Get rank data for all viewers, respecting show_peak preference
  // Note: Some viewers may not have rank data - we'll filter them out
  const rankedViewers = await getRankedViewers(env, viewerPuuids.map(v => v.riot_puuid));

  if (rankedViewers.length === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has ${totalViewers} viewers but none have rank data, skipping`);
    // Update cache to show channel exists but has no ranked viewers
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
      totalViewers,
      Math.floor(Date.now() / 1000),
      statDate
    ).run();
    return;
  }

  // 3. Calculate scores for each viewer
  const viewerScores = rankedViewers.map(viewer => ({
    puuid: viewer.riot_puuid,
    twitch_username: viewer.twitch_username,
    tier: viewer.effective_tier,
    division: viewer.effective_division,
    lp: viewer.effective_lp || 0,
    score: calculateRankScore(viewer.effective_tier, viewer.effective_division, viewer.effective_lp || 0)
  }));

  // 4. Calculate average score
  const avgScore = viewerScores.reduce((sum, v) => sum + v.score, 0) / viewerScores.length;
  const avgRank = scoreToRank(avgScore);

  // 5. Get top 10 viewers by score
  const top10 = viewerScores
    .sort((a, b) => b.score - a.score)
    .slice(0, 10)
    .map(v => ({
      twitch_username: v.twitch_username,
      rank_tier: v.tier,
      rank_division: v.division,
      score: Math.round(v.score * 10) / 10 // Round to 1 decimal
    }));

  // 6. Update channel_stats_cache
  await env.DB.prepare(`
    INSERT INTO channel_stats_cache
      (channel_twitch_id, total_unique_viewers, avg_rank_score, avg_rank_tier,
       avg_rank_division, top_viewers_json, last_updated, last_computed_stat_date, is_eligible)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    totalViewers,
    avgScore,
    avgRank.tier,
    avgRank.division,
    JSON.stringify(top10),
    Math.floor(Date.now() / 1000),
    statDate,
    totalViewers >= 10 ? 1 : 0
  ).run();

  // 7. Compute daily snapshot for yesterday
  await computeDailySnapshot(env, channelTwitchId, statDate, avgScore, totalViewers);

  console.log(`[StatsCron] ${channelTwitchId}: ${totalViewers} viewers (${rankedViewers.length} ranked), avg=${avgRank.tier}${avgRank.division ? ' ' + avgRank.division : ''}`);
}

/**
 * Compute daily snapshot metrics for a specific day
 */
async function computeDailySnapshot(env, channelTwitchId, statDate, alltimeAvgScore, alltimeViewerCount) {
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

  if (dailyRankedViewers.length === 0) {
    // No ranked viewers on this day
    return;
  }

  // Calculate daily average score
  const dailyScores = dailyRankedViewers.map(v =>
    calculateRankScore(v.effective_tier, v.effective_division, v.effective_lp || 0)
  );
  const dailyAvgScore = dailyScores.reduce((sum, s) => sum + s, 0) / dailyScores.length;

  // Insert/update daily snapshot
  await env.DB.prepare(`
    INSERT INTO channel_daily_snapshots
      (stat_date, channel_twitch_id, daily_viewer_count, daily_avg_rank_score,
       alltime_avg_rank_score, alltime_viewer_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(stat_date, channel_twitch_id) DO UPDATE SET
      daily_viewer_count = excluded.daily_viewer_count,
      daily_avg_rank_score = excluded.daily_avg_rank_score,
      alltime_avg_rank_score = excluded.alltime_avg_rank_score,
      alltime_viewer_count = excluded.alltime_viewer_count
  `).bind(
    statDate,
    channelTwitchId,
    dailyPuuids.length,
    dailyAvgScore,
    alltimeAvgScore,
    alltimeViewerCount
  ).run();
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
 * Based on spec: Iron=0, Bronze=200, Silver=400, Gold=600, Plat=800,
 *                Emerald=1000, Diamond=1200, Master=1400, GM=1600, Challenger=1800
 * Division offsets: I=+150, II=+100, III=+50, IV=+0
 * LP bonus: +floor(LP/10) capped at +10
 */
function calculateRankScore(tier, division, lp) {
  const tierBase = {
    'IRON': 0,
    'BRONZE': 200,
    'SILVER': 400,
    'GOLD': 600,
    'PLATINUM': 800,
    'EMERALD': 1000,
    'DIAMOND': 1200,
    'MASTER': 1400,
    'GRANDMASTER': 1600,
    'CHALLENGER': 1800
  };

  const divisionOffset = {
    'I': 150,
    'II': 100,
    'III': 50,
    'IV': 0
  };

  if (!tier) {
    return 0;
  }

  let score = tierBase[tier.toUpperCase()] || 0;

  // Add division offset (only for Iron-Diamond)
  if (division && divisionOffset[division.toUpperCase()] !== undefined) {
    score += divisionOffset[division.toUpperCase()];
  }

  // Add LP bonus (capped at +10)
  const lpBonus = Math.min(Math.floor((lp || 0) / 10), 10);
  score += lpBonus;

  return score;
}

/**
 * Convert numeric score back to human-readable rank
 */
function scoreToRank(score) {
  const tiers = [
    { name: 'CHALLENGER', min: 1800, divisions: false },
    { name: 'GRANDMASTER', min: 1600, divisions: false },
    { name: 'MASTER', min: 1400, divisions: false },
    { name: 'DIAMOND', min: 1200, divisions: true },
    { name: 'EMERALD', min: 1000, divisions: true },
    { name: 'PLATINUM', min: 800, divisions: true },
    { name: 'GOLD', min: 600, divisions: true },
    { name: 'SILVER', min: 400, divisions: true },
    { name: 'BRONZE', min: 200, divisions: true },
    { name: 'IRON', min: 0, divisions: true }
  ];

  for (const tier of tiers) {
    if (score >= tier.min) {
      if (!tier.divisions) {
        return { tier: tier.name, division: null };
      }

      const scoreInTier = score - tier.min;
      if (scoreInTier >= 150) return { tier: tier.name, division: 'I' };
      if (scoreInTier >= 100) return { tier: tier.name, division: 'II' };
      if (scoreInTier >= 50) return { tier: tier.name, division: 'III' };
      return { tier: tier.name, division: 'IV' };
    }
  }

  return { tier: 'IRON', division: 'IV' };
}

/**
 * Get yesterday's stat_date window
 * Windows reset at 07:00 UTC, so this accounts for that
 */
function getYesterdayWindow() {
  const now = new Date();
  const utcHour = now.getUTCHours();

  // If before 07:00 UTC, we're still in yesterday's window
  if (utcHour < 7) {
    now.setUTCDate(now.getUTCDate() - 1);
  }

  // Subtract one more day to get yesterday's window
  now.setUTCDate(now.getUTCDate() - 1);

  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}
