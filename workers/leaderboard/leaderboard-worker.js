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

  let limit = parseInt(url.searchParams.get('limit') || '100');
  let offset = parseInt(url.searchParams.get('offset') || '0');

  limit = Math.min(Math.max(1, limit), 500);
  offset = Math.max(0, offset);
  const results = await env.DB.prepare(`
    SELECT
      channel_twitch_id,
      channel_display_name,
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
      'Cache-Control': 'public, max-age=3600',
      ...corsHeaders
    }
  });
}

/**
 * GET /channel/:name/stats
 * Returns detailed statistics for a specific channel
 */
async function handleGetChannelStats(request, env, channelName, corsHeaders) {
  const cleanChannelName = sanitizeChannelName(channelName);
  if (!cleanChannelName) {
    return new Response(JSON.stringify({
      error: 'Invalid channel name',
      message: 'Channel name must be 3-25 alphanumeric characters or underscores'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
  const stats = await env.DB.prepare(`
    SELECT
      channel_twitch_id,
      channel_display_name,
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

  let leaderboardRank = null;
  if (stats.is_eligible) {
    const rankResult = await env.DB.prepare(`
      SELECT COUNT(*) + 1 as rank
      FROM channel_stats_cache
      WHERE avg_rank_score > ? AND is_eligible = 1
    `).bind(stats.avg_rank_score).first();

    leaderboardRank = rankResult?.rank || null;
  }

  let topViewers = [];
  try {
    topViewers = JSON.parse(stats.top_viewers_json || '[]');
  } catch (e) {
    console.error('[LeaderboardAPI] Failed to parse top_viewers_json:', e);
    topViewers = [];
  }

  return new Response(JSON.stringify({
    channel_twitch_id: stats.channel_twitch_id,
    channel_display_name: stats.channel_display_name || stats.channel_twitch_id,
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
      'Cache-Control': 'public, max-age=1800',
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
  const cleanChannelName = sanitizeChannelName(channelName);
  if (!cleanChannelName) {
    return new Response(JSON.stringify({
      error: 'Invalid channel name',
      message: 'Channel name must be 3-25 alphanumeric characters or underscores'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
  let days = parseInt(url.searchParams.get('days') || '30');
  days = Math.min(Math.max(1, days), 365);

  const startDate = getDateNDaysAgo(days);
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
      'Cache-Control': 'public, max-age=1800',
      ...corsHeaders
    }
  });
}

/**
 * Validate and sanitize channel name
 */
function sanitizeChannelName(name) {
  if (!name || typeof name !== 'string') {
    return null;
  }

  const cleaned = name.toLowerCase().trim();

  if (!/^[a-z0-9_]{3,25}$/.test(cleaned)) {
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
