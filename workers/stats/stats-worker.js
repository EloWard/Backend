/**
 * EloWard Channel Statistics Worker
 *
 * Runs every 6 hours to compute all-time channel statistics
 * Aggregates viewer data from channel_viewers_daily and lol_ranks tables
 * Updates channel_stats_cache and channel_daily_snapshots tables
 */

const statsWorker = {
  /**
   * Main fetch handler
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

    if (url.pathname === '/trigger' && request.method === 'POST') {
      try {
        const summary = await queueAllChannels(env, ctx);
        return new Response(JSON.stringify({
          success: true,
          message: 'Channels queued for processing',
          ...summary
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (error) {
        return new Response(JSON.stringify({
          success: false,
          error: error.message
        }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }

    return new Response('EloWard Stats Worker', { status: 200 });
  },

  /**
   * Scheduled cron handler - runs every 6 hours at :10 minutes past the hour
   * Uses Queue to process all channels across multiple invocations
   */
  async scheduled(event, env, ctx) {
    try {
      console.log(`[StatsCron] Started at ${new Date().toISOString()}`);
      await queueAllChannels(env, ctx);
      console.log(`[StatsCron] Queued all channels for processing`);
    } catch (error) {
      console.error(`[StatsCron] Error during scheduled aggregation:`, error);
      // Don't throw - let the cron complete and try again next time
    }
  },

  /**
   * Queue consumer - processes batches of channels
   */
  async queue(batch, env) {
    const messages = batch.messages;
    console.log(`[QueueConsumer] Processing ${messages.length} message(s)`);

    for (const message of messages) {
      try {
        const payload = message.body;
        const { channels, statDate, streamerUsernames, batchNumber, totalBatches } = payload;
        
        console.log(`[QueueConsumer] Processing batch ${batchNumber}/${totalBatches} with ${channels.length} channels`);
        
        await processChannelBatch(env, channels, statDate, streamerUsernames, {
          batchNumber,
          totalBatches
        });
        
        message.ack();
      } catch (error) {
        console.error(`[QueueConsumer] Error processing batch:`, error);
        if (message.attempts < 3) {
          message.retry();
        } else {
          console.error(`[QueueConsumer] Max retries reached, acking to prevent infinite loop`);
          message.ack();
        }
      }
    }
  }
};

export default statsWorker;

/**
 * Create a D1 database wrapper that tracks query count to stay within per-invocation limits
 * D1 has a hard limit of 1,000 queries per Worker invocation
 * 
 * @param {Object} env - Worker environment with DB
 * @param {number} maxQueries - Maximum queries allowed (default: 1000)
 */
function createDbWithCounter(env, maxQueries = 1000) {
  let queryCount = 0;
  const db = env.DB;
  const MAX_D1_QUERIES_PER_INVOCATION = maxQueries;

  function checkBudget() {
    if (queryCount >= MAX_D1_QUERIES_PER_INVOCATION) {
      throw new Error(
        `[StatsCron] D1 query budget exhausted (queryCount=${queryCount}, limit=${MAX_D1_QUERIES_PER_INVOCATION})`
      );
    }
  }

  return {
    getQueryCount() {
      return queryCount;
    },

    prepare(statement) {
      checkBudget();
      const prepared = db.prepare(statement);
      
      let currentStmt = prepared;
      
      const wrapper = {
        bind(...args) {
          currentStmt = currentStmt.bind(...args);
          return wrapper;
        },

        async first(...args) {
          queryCount++;
          checkBudget();
          return currentStmt.first(...args);
        },

        async run(...args) {
          queryCount++;
          checkBudget();
          return currentStmt.run(...args);
        },

        async all(...args) {
          queryCount++;
          checkBudget();
          return currentStmt.all(...args);
        },
      };
      return wrapper;
    },
  };
}

/**
 * Queue all channels for processing across multiple invocations
 * This allows processing all channels even though D1 has a 1,000 query limit per invocation
 */
async function queueAllChannels(env, ctx) {
  const currentDay = getCurrentWindow();
  console.log(`[StatsCron] Queuing channels for stat_date=${currentDay}`);

  // Create DB wrapper with query counter (only for getting channel list)
  const db = createDbWithCounter(env, 1000);

  // Get all eligible leaderboard streamers to exclude them as viewers
  const streamerUsernames = await getLeaderboardStreamerUsernames(db);
  console.log(`[StatsCron] Excluding ${streamerUsernames.size} leaderboard streamers from viewer counts`);

  // Get all unique channels that have EVER had viewers
  const channelsResult = await db.prepare(`
    SELECT DISTINCT channel_twitch_id
    FROM channel_viewers_daily
  `).all();

  const channels = channelsResult.results || [];
  console.log(`[StatsCron] Found ${channels.length} channels to process`);

  if (channels.length === 0) {
    console.log(`[StatsCron] No channels to process, exiting`);
    return;
  }

  const CHANNELS_PER_BATCH = 120;
  const batches = [];

  for (let i = 0; i < channels.length; i += CHANNELS_PER_BATCH) {
    const batch = channels.slice(i, i + CHANNELS_PER_BATCH);
    batches.push(batch);
  }

  console.log(`[StatsCron] Creating ${batches.length} queue batches (${CHANNELS_PER_BATCH} channels per batch)`);

  // Send each batch to the queue
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    await env.CHANNEL_STATS_QUEUE.send({
      channels: batch,
      statDate: currentDay,
      streamerUsernames: Array.from(streamerUsernames),
      batchNumber: i + 1,
      totalBatches: batches.length
    });
  }

  console.log(`[StatsCron] Queued ${batches.length} batches for processing`);
  return {
    totalChannels: channels.length,
    batchesQueued: batches.length,
    channelsPerBatch: CHANNELS_PER_BATCH
  };
}

/**
 * Process a batch of channels (called by queue consumer)
 */
async function processChannelBatch(env, channels, statDate, streamerUsernames, batchInfo = {}) {
  const db = createDbWithCounter(env, 1000);
  const streamerUsernamesSet = new Set(streamerUsernames);
  const CONCURRENT_SIZE = 6;
  
  let processed = 0;
  let errors = 0;
  const queriesAtStart = db.getQueryCount();

  console.log(
    `[QueueBatch] Processing batch ${batchInfo.batchNumber || '?'}/${batchInfo.totalBatches || '?'}: ${channels.length} channels`
  );

  for (let i = 0; i < channels.length; i += CONCURRENT_SIZE) {
    const currentQueries = db.getQueryCount();
    
    if (currentQueries >= 1000) {
      console.log(
        `[QueueBatch] Hit query limit (used=${currentQueries}, processed=${processed}, remaining=${channels.length - i})`
      );
      break;
    }

    const chunk = channels.slice(i, i + CONCURRENT_SIZE);
    const queriesBeforeChunk = db.getQueryCount();

    // Process concurrent chunk in parallel
    const results = await Promise.allSettled(
      chunk.map(({ channel_twitch_id }) =>
        computeChannelStats(db, channel_twitch_id, statDate, streamerUsernamesSet)
      )
    );

    const queriesAfterChunk = db.getQueryCount();

    let budgetExhausted = false;
    results.forEach((result, idx) => {
      if (result.status === 'fulfilled') {
        processed++;
      } else {
        errors++;
        const reason = result.reason || result;
        const errorMsg = reason?.message || String(reason);
        if (errorMsg.includes('query budget') || errorMsg.includes('Too many API requests')) {
          budgetExhausted = true;
        } else {
          console.error(`[QueueBatch] Error processing ${chunk[idx].channel_twitch_id}:`, reason);
        }
      }
    });

    if (budgetExhausted || queriesAfterChunk >= 1000) {
      console.log(
        `[QueueBatch] Stopping due to query budget (used=${queriesAfterChunk}, processed=${processed}, errors=${errors})`
      );
      break;
    }

    if (i + CONCURRENT_SIZE < channels.length) {
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  const totalQueries = db.getQueryCount();
  const queriesUsedForProcessing = totalQueries - queriesAtStart;
  const avgQueriesPerChannel = processed > 0 ? queriesUsedForProcessing / processed : 0;

  console.log(
    `[QueueBatch] Batch complete: ${processed}/${channels.length} processed, ${errors} errors, ${totalQueries} queries (avg ${avgQueriesPerChannel.toFixed(1)}/channel)`
  );

  return {
    processed,
    errors,
    totalChannels: channels.length,
    queriesUsed: totalQueries,
    queriesUsedForProcessing,
    avgQueriesPerChannel: parseFloat(avgQueriesPerChannel.toFixed(2))
  };
}


/**
 * Compute all-time stats for a single channel
 * @param {Object} db - D1 database wrapper with query counter
 * @param {string} channelTwitchId - Channel Twitch ID
 * @param {string} statDate - Current stat date window
 * @param {Set<string>} streamerUsernames - Set of eligible leaderboard streamer usernames to exclude
 */
async function computeChannelStats(db, channelTwitchId, statDate, streamerUsernames) {
  const uniqueViewersResult = await db.prepare(`
    SELECT DISTINCT riot_puuid
    FROM channel_viewers_daily
    WHERE channel_twitch_id = ?
  `).bind(channelTwitchId).all();

  const viewerPuuids = uniqueViewersResult.results || [];

  if (viewerPuuids.length === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has no viewers, skipping`);
    return;
  }

  const rankedViewers = await getRankedViewers(db, viewerPuuids.map(v => v.riot_puuid));
  const ownerPuuids = await getChannelOwnerPuuids(db, channelTwitchId);

  const filteredViewers = rankedViewers.filter(v => {
    if (ownerPuuids.has(v.riot_puuid)) {
      return false;
    }

    const viewerUsernameLower = v.twitch_username?.toLowerCase();
    if (viewerUsernameLower && streamerUsernames.has(viewerUsernameLower)) {
      return false;
    }

    return true;
  });
  const totalViewers = filteredViewers.length;

  if (totalViewers === 0) {
    console.log(`[StatsCron] Channel ${channelTwitchId} has viewers but none qualify (excluding self and streamers), skipping`);
    const displayName = await getChannelDisplayName(db, channelTwitchId);
    await db.prepare(`
      INSERT INTO channel_stats_cache
        (channel_twitch_id, channel_display_name, total_unique_viewers, avg_rank_score, avg_rank_tier,
         avg_rank_division, top_viewers_json, last_updated, last_computed_stat_date, is_eligible)
      VALUES (?, ?, ?, NULL, NULL, NULL, '[]', ?, ?, 0)
      ON CONFLICT(channel_twitch_id) DO UPDATE SET
        channel_display_name = excluded.channel_display_name,
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
      displayName,
      0,
      Math.floor(Date.now() / 1000),
      statDate
    ).run();
    return;
  }

  const viewerScores = filteredViewers.map(viewer => ({
    puuid: viewer.riot_puuid,
    twitch_username: viewer.twitch_username,
    tier: viewer.effective_tier,
    division: viewer.effective_division,
    lp: viewer.effective_lp || 0,
    score: calculateRankScore(viewer.effective_tier, viewer.effective_division, viewer.effective_lp || 0)
  }));

  const avgScore = viewerScores.reduce((sum, v) => sum + v.score, 0) / viewerScores.length;
  const avgRank = scoreToRank(avgScore);
  const sortedScores = [...viewerScores].sort((a, b) => a.score - b.score);
  const medianScore = sortedScores.length % 2 === 0
    ? (sortedScores[sortedScores.length / 2 - 1].score + sortedScores[sortedScores.length / 2].score) / 2
    : sortedScores[Math.floor(sortedScores.length / 2)].score;
  const medianRank = scoreToRank(medianScore);

  const top10 = viewerScores
    .sort((a, b) => b.score - a.score)
    .slice(0, 10)
    .map(v => ({
      twitch_username: v.twitch_username,
      rank_tier: v.tier,
      rank_division: v.division,
      lp: v.lp,
      score: Math.round(v.score * 10) / 10
    }));

  const displayName = await getChannelDisplayName(db, channelTwitchId);
  await db.prepare(`
    INSERT INTO channel_stats_cache
      (channel_twitch_id, channel_display_name, total_unique_viewers, avg_rank_score, avg_rank_tier,
       avg_rank_division, avg_lp, median_rank_score, median_rank_tier,
       median_rank_division, median_lp, top_viewers_json, last_updated, last_computed_stat_date, is_eligible)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(channel_twitch_id) DO UPDATE SET
      channel_display_name = excluded.channel_display_name,
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
    displayName,
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

  await computeDailySnapshot(db, channelTwitchId, statDate, avgScore, avgRank.lp, medianScore, medianRank.lp, totalViewers, streamerUsernames);

  console.log(`[StatsCron] ${channelTwitchId}: ${totalViewers} non-streamer viewers, avg=${avgRank.tier}${avgRank.division ? ' ' + avgRank.division : ''}`);
}

/**
 * Compute daily snapshot metrics for a specific day
 * @param {Object} db - D1 database wrapper with query counter
 * @param {string} channelTwitchId - Channel Twitch ID
 * @param {string} statDate - Current stat date window
 * @param {number} alltimeAvgScore - All-time average rank score
 * @param {number} alltimeAvgLp - All-time average LP
 * @param {number} alltimeMedianScore - All-time median rank score
 * @param {number} alltimeMedianLp - All-time median LP
 * @param {number} alltimeViewerCount - All-time viewer count
 * @param {Set<string>} streamerUsernames - Set of eligible leaderboard streamer usernames to exclude
 */
async function computeDailySnapshot(db, channelTwitchId, statDate, alltimeAvgScore, alltimeAvgLp, alltimeMedianScore, alltimeMedianLp, alltimeViewerCount, streamerUsernames) {
  const dailyViewersResult = await db.prepare(`
    SELECT DISTINCT riot_puuid
    FROM channel_viewers_daily
    WHERE channel_twitch_id = ? AND stat_date = ?
  `).bind(channelTwitchId, statDate).all();

  const dailyPuuids = dailyViewersResult.results || [];

  if (dailyPuuids.length === 0) {
    return;
  }

  const dailyRankedViewers = await getRankedViewers(db, dailyPuuids.map(v => v.riot_puuid));
  const dailyOwnerPuuids = await getChannelOwnerPuuids(db, channelTwitchId);
  const filteredDailyViewers = dailyRankedViewers.filter(v => {
    if (dailyOwnerPuuids.has(v.riot_puuid)) return false;
    const viewerUsernameLower = v.twitch_username?.toLowerCase();
    if (viewerUsernameLower && streamerUsernames.has(viewerUsernameLower)) return false;
    return true;
  });

  if (filteredDailyViewers.length === 0) {
    return;
  }
  const dailyScores = filteredDailyViewers.map(v =>
    calculateRankScore(v.effective_tier, v.effective_division, v.effective_lp || 0)
  );
  const dailyAvgScore = dailyScores.reduce((sum, s) => sum + s, 0) / dailyScores.length;
  const dailyRank = scoreToRank(dailyAvgScore);
  const dailyAvgLp = dailyRank.lp;

  const sortedDailyScores = [...dailyScores].sort((a, b) => a - b);
  const dailyMedianScore = sortedDailyScores.length % 2 === 0
    ? (sortedDailyScores[sortedDailyScores.length / 2 - 1] + sortedDailyScores[sortedDailyScores.length / 2]) / 2
    : sortedDailyScores[Math.floor(sortedDailyScores.length / 2)];
  const dailyMedianRank = scoreToRank(dailyMedianScore);
  const dailyMedianLp = dailyMedianRank.lp;

  await db.prepare(`
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
 * Get all eligible leaderboard channel usernames (>= 10 viewers)
 * These are streamers who shouldn't count as viewers for other channels
 */
async function getLeaderboardStreamerUsernames(db) {
  const result = await db.prepare(`
    SELECT LOWER(channel_twitch_id) as channel_twitch_id
    FROM channel_stats_cache
    WHERE is_eligible = 1
  `).all();

  return new Set((result.results || []).map(r => r.channel_twitch_id));
}

/**
 * Get the channel owner's riot_puuid(s) by exact username match
 */
async function getChannelOwnerPuuids(db, channelTwitchId) {
  const ownerPuuids = new Set();
  const exactMatch = await db.prepare(`
    SELECT riot_puuid
    FROM lol_ranks
    WHERE LOWER(twitch_username) = LOWER(?)
  `).bind(channelTwitchId).first();

  if (exactMatch) {
    ownerPuuids.add(exactMatch.riot_puuid);
  }

  return ownerPuuids;
}

/**
 * Get the channel's display name (proper capitalization) from the database
 */
async function getChannelDisplayName(db, channelTwitchId) {
  const userResult = await db.prepare(`
    SELECT display_name
    FROM users
    WHERE LOWER(channel_name) = LOWER(?)
  `).bind(channelTwitchId).first();

  if (userResult?.display_name) {
    return userResult.display_name;
  }

  const lolRanksResult = await db.prepare(`
    SELECT twitch_username
    FROM lol_ranks
    WHERE LOWER(twitch_username) = LOWER(?)
  `).bind(channelTwitchId).first();

  if (lolRanksResult?.twitch_username) {
    return lolRanksResult.twitch_username;
  }

  return channelTwitchId;
}

/**
 * Get rank data for viewers in batches (respects show_peak flag)
 */
async function getRankedViewers(db, puuids) {
  if (puuids.length === 0) {
    return [];
  }

  const BATCH_SIZE = 100;
  const allViewers = [];

  if (puuids.length > BATCH_SIZE) {
    console.log(`[getRankedViewers] batching ${puuids.length} PUUIDs in chunks of ${BATCH_SIZE}`);
  }

  for (let i = 0; i < puuids.length; i += BATCH_SIZE) {
    const batch = puuids.slice(i, i + BATCH_SIZE);
    const placeholders = batch.map(() => '?').join(',');

    const result = await db.prepare(`
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
 */
function calculateRankScore(tier, division, lp) {
  const tierBase = {
    'IRON': 0,
    'BRONZE': 400,
    'SILVER': 800,
    'GOLD': 1200,
    'PLATINUM': 1600,
    'EMERALD': 2000,
    'DIAMOND': 2400,
    'MASTER': 2800,
    'GRANDMASTER': 2800,
    'CHALLENGER': 2800
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

  if (division && divisionOffset[division.toUpperCase()] !== undefined) {
    score += divisionOffset[division.toUpperCase()];
  }

  score += (lp || 0);

  return score;
}

/**
 * Convert numeric score back to human-readable rank with LP
 */
function scoreToRank(score) {
  if (score >= 2800) {
    const lpInMaster = score - 2800;

    if (lpInMaster >= 700) {
      return { tier: 'CHALLENGER', division: null, lp: Math.round(lpInMaster) };
    } else if (lpInMaster >= 200) {
      return { tier: 'GRANDMASTER', division: null, lp: Math.round(lpInMaster) };
    } else {
      return { tier: 'MASTER', division: null, lp: Math.round(lpInMaster) };
    }
  }

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
 * Windows reset at 07:00 UTC
 */
function getCurrentWindow() {
  const now = new Date();
  const utcHour = now.getUTCHours();

  if (utcHour < 7) {
    now.setUTCDate(now.getUTCDate() - 1);
  }

  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}
