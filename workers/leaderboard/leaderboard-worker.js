/**
 * EloWard Leaderboard API Worker
 *
 * Public read-only API for channel statistics and leaderboard data
 * Serves data computed by stats-worker (eloward-stats)
 *
 * Endpoints:
 * - GET /leaderboard - Top channels ranked by average viewer rank
 * - GET /channel/:name/stats - Individual channel statistics
 * - GET /channel/:name/trend - Daily trend data for charts
 * - GET /health - Health check
 */

// Dummy data for testing (add ?dummy=true to endpoints)
// Note: Scores use new system: 100 LP = 100 points, Master+ starts at 2800
const DUMMY_LEADERBOARD = [
  { channel_twitch_id: 'doublelift', avg_rank_tier: 'CHALLENGER', avg_rank_division: null, avg_lp: 750, avg_rank_score: 3550, median_rank_tier: 'GRANDMASTER', median_rank_division: null, median_lp: 650, median_rank_score: 3450, total_unique_viewers: 2847, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'imaqtpie', avg_rank_tier: 'GRANDMASTER', avg_rank_division: null, avg_lp: 550, avg_rank_score: 3350, median_rank_tier: 'GRANDMASTER', median_rank_division: null, median_lp: 500, median_rank_score: 3300, total_unique_viewers: 1923, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'yassuo', avg_rank_tier: 'GRANDMASTER', avg_rank_division: null, avg_lp: 480, avg_rank_score: 3280, median_rank_tier: 'MASTER', median_rank_division: null, median_lp: 150, median_rank_score: 2950, total_unique_viewers: 1547, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'tyler1', avg_rank_tier: 'MASTER', avg_rank_division: null, avg_lp: 120, avg_rank_score: 2920, median_rank_tier: 'MASTER', median_rank_division: null, median_lp: 80, median_rank_score: 2880, total_unique_viewers: 3256, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'iwd', avg_rank_tier: 'DIAMOND', avg_rank_division: 'I', avg_lp: 65, avg_rank_score: 2765, median_rank_tier: 'DIAMOND', median_rank_division: 'I', median_lp: 75, median_rank_score: 2775, total_unique_viewers: 892, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'voyboy', avg_rank_tier: 'DIAMOND', avg_rank_division: 'II', avg_lp: 80, avg_rank_score: 2680, median_rank_tier: 'DIAMOND', median_rank_division: 'II', median_lp: 60, median_rank_score: 2660, total_unique_viewers: 1204, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'trick2g', avg_rank_tier: 'DIAMOND', avg_rank_division: 'III', avg_lp: 30, avg_rank_score: 2530, median_rank_tier: 'DIAMOND', median_rank_division: 'III', median_lp: 25, median_rank_score: 2525, total_unique_viewers: 756, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'nightblue3', avg_rank_tier: 'EMERALD', avg_rank_division: 'II', avg_lp: 50, avg_rank_score: 2250, median_rank_tier: 'EMERALD', median_rank_division: 'III', median_lp: 40, median_rank_score: 2140, total_unique_viewers: 1678, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'scarra', avg_rank_tier: 'EMERALD', avg_rank_division: 'IV', avg_lp: 50, avg_rank_score: 2050, median_rank_tier: 'PLATINUM', median_rank_division: 'I', median_lp: 55, median_rank_score: 1955, total_unique_viewers: 543, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'sanchovies', avg_rank_tier: 'PLATINUM', avg_rank_division: 'II', avg_lp: 2, avg_rank_score: 1802, median_rank_tier: 'PLATINUM', median_rank_division: 'II', median_lp: 5, median_rank_score: 1805, total_unique_viewers: 324, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'pokimane', avg_rank_tier: 'PLATINUM', avg_rank_division: 'IV', avg_lp: 20, avg_rank_score: 1620, median_rank_tier: 'PLATINUM', median_rank_division: 'IV', median_lp: 15, median_rank_score: 1615, total_unique_viewers: 2134, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'hasanabi', avg_rank_tier: 'GOLD', avg_rank_division: 'I', avg_lp: 80, avg_rank_score: 1580, median_rank_tier: 'GOLD', median_rank_division: 'I', median_lp: 70, median_rank_score: 1570, total_unique_viewers: 967, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'sneaky', avg_rank_tier: 'GOLD', avg_rank_division: 'III', avg_lp: 80, avg_rank_score: 1380, median_rank_tier: 'GOLD', median_rank_division: 'IV', median_lp: 30, median_rank_score: 1230, total_unique_viewers: 445, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'meteos', avg_rank_tier: 'SILVER', avg_rank_division: 'I', avg_lp: 80, avg_rank_score: 1180, median_rank_tier: 'SILVER', median_rank_division: 'I', median_lp: 75, median_rank_score: 1175, total_unique_viewers: 289, last_updated: Date.now() / 1000 },
  { channel_twitch_id: 'boxbox', avg_rank_tier: 'SILVER', avg_rank_division: 'III', avg_lp: 80, avg_rank_score: 980, median_rank_tier: 'SILVER', median_rank_division: 'III', median_lp: 90, median_rank_score: 990, total_unique_viewers: 187, last_updated: Date.now() / 1000 },
];

const DUMMY_CHANNEL_STATS = {
  doublelift: {
    channel_twitch_id: 'doublelift',
    avg_rank_tier: 'CHALLENGER',
    avg_rank_division: null,
    avg_lp: 750,
    avg_rank_score: 3550,
    median_rank_tier: 'GRANDMASTER',
    median_rank_division: null,
    median_lp: 650,
    median_rank_score: 3450,
    total_unique_viewers: 2847,
    leaderboard_rank: 1,
    top_viewers: [
      { twitch_username: 'player1', rank_tier: 'CHALLENGER', rank_division: null, lp: 900, score: 3700 },
      { twitch_username: 'player2', rank_tier: 'GRANDMASTER', rank_division: null, lp: 600, score: 3400 },
      { twitch_username: 'player3', rank_tier: 'GRANDMASTER', rank_division: null, lp: 550, score: 3350 },
      { twitch_username: 'player4', rank_tier: 'MASTER', rank_division: null, lp: 400, score: 3200 },
      { twitch_username: 'player5', rank_tier: 'MASTER', rank_division: null, lp: 350, score: 3150 },
      { twitch_username: 'player6', rank_tier: 'DIAMOND', rank_division: 'I', lp: 80, score: 2780 },
      { twitch_username: 'player7', rank_tier: 'DIAMOND', rank_division: 'II', lp: 20, score: 2620 },
      { twitch_username: 'player8', rank_tier: 'DIAMOND', rank_division: 'III', lp: 80, score: 2580 },
      { twitch_username: 'player9', rank_tier: 'EMERALD', rank_division: 'I', lp: 80, score: 2380 },
      { twitch_username: 'player10', rank_tier: 'EMERALD', rank_division: 'II', lp: 50, score: 2250 },
    ]
  }
};

const DUMMY_TREND_DATA = [
  { stat_date: '2025-11-01', daily_viewer_count: 45, daily_avg_rank_score: 3420, alltime_avg_rank_score: 3350, alltime_viewer_count: 2340 },
  { stat_date: '2025-11-03', daily_viewer_count: 52, daily_avg_rank_score: 3450, alltime_avg_rank_score: 3370, alltime_viewer_count: 2392 },
  { stat_date: '2025-11-05', daily_viewer_count: 48, daily_avg_rank_score: 3430, alltime_avg_rank_score: 3385, alltime_viewer_count: 2440 },
  { stat_date: '2025-11-07', daily_viewer_count: 61, daily_avg_rank_score: 3500, alltime_avg_rank_score: 3405, alltime_viewer_count: 2501 },
  { stat_date: '2025-11-09', daily_viewer_count: 55, daily_avg_rank_score: 3475, alltime_avg_rank_score: 3420, alltime_viewer_count: 2556 },
  { stat_date: '2025-11-11', daily_viewer_count: 58, daily_avg_rank_score: 3520, alltime_avg_rank_score: 3440, alltime_viewer_count: 2614 },
  { stat_date: '2025-11-13', daily_viewer_count: 63, daily_avg_rank_score: 3550, alltime_avg_rank_score: 3462, alltime_viewer_count: 2677 },
  { stat_date: '2025-11-15', daily_viewer_count: 59, daily_avg_rank_score: 3535, alltime_avg_rank_score: 3480, alltime_viewer_count: 2736 },
  { stat_date: '2025-11-17', daily_viewer_count: 56, daily_avg_rank_score: 3510, alltime_avg_rank_score: 3495, alltime_viewer_count: 2792 },
  { stat_date: '2025-11-19', daily_viewer_count: 60, daily_avg_rank_score: 3560, alltime_avg_rank_score: 3515, alltime_viewer_count: 2847 },
  { stat_date: '2025-11-21', daily_viewer_count: 62, daily_avg_rank_score: 3570, alltime_avg_rank_score: 3530, alltime_viewer_count: 2847 },
  { stat_date: '2025-11-23', daily_viewer_count: 58, daily_avg_rank_score: 3545, alltime_avg_rank_score: 3540, alltime_viewer_count: 2847 },
  { stat_date: '2025-11-25', daily_viewer_count: 61, daily_avg_rank_score: 3555, alltime_avg_rank_score: 3545, alltime_viewer_count: 2847 },
  { stat_date: '2025-11-27', daily_viewer_count: 64, daily_avg_rank_score: 3580, alltime_avg_rank_score: 3550, alltime_viewer_count: 2847 },
];

const leaderboardWorker = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS headers for public API
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Max-Age': '86400'
    };

    // Handle preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Only allow GET requests
    if (request.method !== 'GET') {
      return new Response(JSON.stringify({ error: 'Method not allowed' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    try {
      // Route handling
      if (url.pathname === '/health') {
        return handleHealth(env, corsHeaders);
      }

      if (url.pathname === '/leaderboard') {
        return handleGetLeaderboard(request, env, corsHeaders);
      }

      // Match /channel/:name/stats
      const statsMatch = url.pathname.match(/^\/channel\/([^\/]+)\/stats$/);
      if (statsMatch) {
        return handleGetChannelStats(request, env, statsMatch[1], corsHeaders);
      }

      // Match /channel/:name/trend
      const trendMatch = url.pathname.match(/^\/channel\/([^\/]+)\/trend$/);
      if (trendMatch) {
        return handleGetChannelTrend(request, env, trendMatch[1], corsHeaders);
      }

      // 404 for unknown routes
      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });

    } catch (error) {
      console.error('[LeaderboardAPI] Error:', error);
      return new Response(JSON.stringify({
        error: 'Internal server error',
        message: error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }
};

export default leaderboardWorker;

/**
 * Health check endpoint
 */
async function handleHealth(env, corsHeaders) {
  return new Response(JSON.stringify({
    status: 'ok',
    worker: 'eloward-leaderboard',
    timestamp: new Date().toISOString()
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

/**
 * GET /leaderboard
 * Returns top channels ranked by average viewer rank
 */
async function handleGetLeaderboard(request, env, corsHeaders) {
  const url = new URL(request.url);

  // Parse query parameters
  let limit = parseInt(url.searchParams.get('limit') || '100');
  let offset = parseInt(url.searchParams.get('offset') || '0');
  const useDummy = url.searchParams.get('dummy') === 'true';

  // Validate and cap limits
  limit = Math.min(Math.max(1, limit), 500);  // Between 1 and 500
  offset = Math.max(0, offset);

  // Return dummy data if requested
  if (useDummy) {
    return new Response(JSON.stringify({
      leaderboard: DUMMY_LEADERBOARD.slice(offset, offset + limit),
      total_eligible_channels: DUMMY_LEADERBOARD.length,
      pagination: {
        limit,
        offset,
        has_more: offset + limit < DUMMY_LEADERBOARD.length
      }
    }), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600',
        ...corsHeaders
      }
    });
  }

  // Fetch limit+1 to check if there are more results
  const results = await env.DB.prepare(`
    SELECT
      channel_twitch_id,
      avg_rank_tier,
      avg_rank_division,
      avg_lp,
      avg_rank_score,
      median_rank_tier,
      median_rank_division,
      median_lp,
      median_rank_score,
      total_unique_viewers,
      last_updated
    FROM channel_stats_cache
    WHERE is_eligible = 1
    ORDER BY avg_rank_score DESC
    LIMIT ? OFFSET ?
  `).bind(limit + 1, offset).all();

  const hasMore = results.results.length > limit;
  const leaderboard = results.results.slice(0, limit);

  // Get total count of eligible channels
  const countResult = await env.DB.prepare(`
    SELECT COUNT(*) as count
    FROM channel_stats_cache
    WHERE is_eligible = 1
  `).first();

  return new Response(JSON.stringify({
    leaderboard,
    total_eligible_channels: countResult?.count || 0,
    pagination: {
      limit,
      offset,
      has_more: hasMore
    }
  }), {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=3600',  // Cache for 1 hour
      ...corsHeaders
    }
  });
}

/**
 * GET /channel/:name/stats
 * Returns detailed statistics for a specific channel
 */
async function handleGetChannelStats(request, env, channelName, corsHeaders) {
  const url = new URL(request.url);
  const useDummy = url.searchParams.get('dummy') === 'true';

  // Validate and sanitize channel name
  const cleanChannelName = sanitizeChannelName(channelName);
  if (!cleanChannelName) {
    return new Response(JSON.stringify({
      error: 'Invalid channel name',
      message: 'Channel name must be 4-25 alphanumeric characters or underscores'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  // Return dummy data if requested
  if (useDummy && DUMMY_CHANNEL_STATS[cleanChannelName]) {
    return new Response(JSON.stringify(DUMMY_CHANNEL_STATS[cleanChannelName]), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=1800',
        ...corsHeaders
      }
    });
  }

  // Fetch channel stats
  const stats = await env.DB.prepare(`
    SELECT
      channel_twitch_id,
      avg_rank_tier,
      avg_rank_division,
      avg_lp,
      avg_rank_score,
      median_rank_tier,
      median_rank_division,
      median_lp,
      median_rank_score,
      total_unique_viewers,
      top_viewers_json,
      last_updated,
      is_eligible
    FROM channel_stats_cache
    WHERE channel_twitch_id = ?
  `).bind(cleanChannelName).first();

  if (!stats) {
    return new Response(JSON.stringify({
      error: 'Channel not found',
      message: 'This channel has no viewer data yet. They need at least 1 EloWard viewer who has watched for 5+ minutes.'
    }), {
      status: 404,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  // Get leaderboard rank (only if eligible)
  let leaderboardRank = null;
  if (stats.is_eligible) {
    const rankResult = await env.DB.prepare(`
      SELECT COUNT(*) + 1 as rank
      FROM channel_stats_cache
      WHERE avg_rank_score > ? AND is_eligible = 1
    `).bind(stats.avg_rank_score).first();

    leaderboardRank = rankResult?.rank || null;
  }

  // Parse top viewers JSON
  let topViewers = [];
  try {
    topViewers = JSON.parse(stats.top_viewers_json || '[]');
  } catch (e) {
    console.error('[LeaderboardAPI] Failed to parse top_viewers_json:', e);
    topViewers = [];
  }

  return new Response(JSON.stringify({
    channel_twitch_id: stats.channel_twitch_id,
    avg_rank_tier: stats.avg_rank_tier,
    avg_rank_division: stats.avg_rank_division,
    avg_lp: stats.avg_lp,
    avg_rank_score: stats.avg_rank_score,
    median_rank_tier: stats.median_rank_tier,
    median_rank_division: stats.median_rank_division,
    median_lp: stats.median_lp,
    median_rank_score: stats.median_rank_score,
    total_unique_viewers: stats.total_unique_viewers,
    top_viewers: topViewers,
    last_updated: stats.last_updated,
    leaderboard_rank: leaderboardRank
  }), {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=1800',  // Cache for 30 minutes
      ...corsHeaders
    }
  });
}

/**
 * GET /channel/:name/trend
 * Returns daily trend data for temporal charts
 */
async function handleGetChannelTrend(request, env, channelName, corsHeaders) {
  const url = new URL(request.url);
  const useDummy = url.searchParams.get('dummy') === 'true';

  // Validate and sanitize channel name
  const cleanChannelName = sanitizeChannelName(channelName);
  if (!cleanChannelName) {
    return new Response(JSON.stringify({
      error: 'Invalid channel name',
      message: 'Channel name must be 4-25 alphanumeric characters or underscores'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  // Return dummy data if requested
  if (useDummy) {
    return new Response(JSON.stringify({
      channel_twitch_id: cleanChannelName,
      trend_data: DUMMY_TREND_DATA
    }), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=1800',
        ...corsHeaders
      }
    });
  }

  // Parse days parameter
  let days = parseInt(url.searchParams.get('days') || '30');
  days = Math.min(Math.max(1, days), 365);  // Between 1 and 365

  // Calculate start date (days ago)
  const startDate = getDateNDaysAgo(days);

  // Fetch trend data
  const results = await env.DB.prepare(`
    SELECT
      stat_date,
      daily_viewer_count,
      daily_avg_rank_score,
      alltime_avg_rank_score,
      alltime_viewer_count
    FROM channel_daily_snapshots
    WHERE channel_twitch_id = ? AND stat_date >= ?
    ORDER BY stat_date DESC
  `).bind(cleanChannelName, startDate).all();

  return new Response(JSON.stringify({
    channel_twitch_id: cleanChannelName,
    trend_data: results.results || []
  }), {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=1800',  // Cache for 30 minutes
      ...corsHeaders
    }
  });
}

/**
 * Validate and sanitize channel name
 * Twitch usernames: 4-25 chars, alphanumeric + underscore
 */
function sanitizeChannelName(name) {
  if (!name || typeof name !== 'string') {
    return null;
  }

  const cleaned = name.toLowerCase().trim();

  // Validate format: 4-25 alphanumeric characters or underscores
  if (!/^[a-z0-9_]{4,25}$/.test(cleaned)) {
    return null;
  }

  return cleaned;
}

/**
 * Get date N days ago in YYYY-MM-DD format
 */
function getDateNDaysAgo(days) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);

  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}
