/**
 * EloWard Users Worker
 * Handles user management and channel operations including channel verification,
 * user registration, dashboard data, and metrics tracking.
 */

// Allowed origins for CORS
const allowedOrigins = [
  'https://www.eloward.com', 
  'https://eloward.com',
  'https://www.twitch.tv'  // FFZ addon access
];

// Helper function to parse JSON with error handling
async function parseRequestBody(request) {
  try {
    return await request.json();
  } catch (e) {
    throw new Error('Invalid JSON');
  }
}

// Helper function to create error responses
function createErrorResponse(status, error, message = null, headers = {}) {
  const response = { error };
  if (message) response.message = message;
  
  return new Response(JSON.stringify(response), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers }
  });
}

// Helper function to validate channel name
function validateChannelName(channelName) {
  if (!channelName) {
    throw new Error('Missing channel_name parameter');
  }
  return channelName.toLowerCase();
}

const usersWorker = {
  async fetch(request, env, ctx) {
    const corsHeaders = getCorsHeaders(request);

    // Handle CORS preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders });
    }
    
    let response;
    const url = new URL(request.url);

    try {
      // Route handling
      if (url.pathname.includes('/channelstatus/verify')) {
        response = request.method === 'POST' 
          ? await handleChannelStatusVerify(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      } else if (url.pathname.includes('/metrics/db_read')) {
        response = request.method === 'POST'
          ? await handleIncrementDbReads(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      } else if (url.pathname.includes('/metrics/successful_lookup')) {
        response = request.method === 'POST'
          ? await handleIncrementSuccessfulLookups(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      } else if (url.pathname.includes('/dashboard/data_id')) {
        response = request.method === 'POST'
          ? await handleDashboardDataById(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      // Removed legacy name-based dashboard endpoint
      } else if (url.pathname.includes('/dashboard/data')) {
        response = new Response('Not Found', { status: 404 });
      } else if (url.pathname.includes('/user/register')) {
        if (request.method === 'POST') {
          // Require internal auth for write of Twitch users
          const provided = request.headers.get('X-Internal-Auth');
          const expected = env.USERS_WRITE_KEY;
          if (!expected || provided !== expected) {
            response = new Response(JSON.stringify({ error: 'Forbidden' }), { status: 403, headers: { 'Content-Type': 'application/json' } });
          } else {
            response = await handleUserRegistration(request, env, corsHeaders);
          }
        } else {
          response = new Response('Method Not Allowed', { status: 405 });
        }
      } else if (url.pathname.includes('/channel/active/update_id')) {
        response = request.method === 'POST'
          ? await handleChannelActiveUpdateById(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      // Removed legacy name-based channel active update endpoint
      } else if (url.pathname.includes('/channel/active/update')) {
        response = new Response('Not Found', { status: 404 });
      } else if (url.pathname.includes('/user/lookup')) {
        response = request.method === 'POST'
          ? await handleUserLookup(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      } else if (url.pathname.includes('/user/riot-fallback')) {
        response = request.method === 'POST'
          ? await handleRiotDataFallback(request, env, corsHeaders)
          : new Response('Method Not Allowed', { status: 405 });
      } else if (url.pathname.includes('/health')) {
        response = new Response(JSON.stringify({ status: 'ok' }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } else {
        response = new Response('Not Found', { status: 404 });
      }
    } catch (error) {
      console.error("Worker error:", error);
      response = new Response(JSON.stringify({ error: 'Internal Server Error' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    
    // Ensure all responses have CORS headers
    const finalHeaders = new Headers(response.headers);
    for (const [key, value] of Object.entries(corsHeaders)) {
      finalHeaders.set(key, value);
    }
    
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: finalHeaders,
    });
  }
};

export default usersWorker;

/**
 * Generates CORS headers based on the request origin
 */
function getCorsHeaders(request) {
  const origin = request.headers.get('Origin');
  const allowedOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Internal-Auth',
    'Access-Control-Max-Age': '86400',
  };
}

/**
 * Handles channel status verification requests
 */
async function handleChannelStatusVerify(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const channelName = validateChannelName(body.channel_name);
    
    const query = `
      SELECT channel_active FROM \`users\` 
      WHERE channel_name = ?
      LIMIT 1
    `;
    
    const result = await env.DB.prepare(query).bind(channelName).first();
    
    return new Response(JSON.stringify({ 
      active: result ? !!result.channel_active : true
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error verifying channel status:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to verify channel status',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Increments the db_reads counter for a channel
 */
async function handleIncrementDbReads(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const channelName = validateChannelName(body.channel_name);
    
    const query = `UPDATE \`users\` SET db_reads = db_reads + 1 WHERE channel_name = ?`;
    const result = await env.DB.prepare(query).bind(channelName).run();
    
    return new Response(JSON.stringify({ 
      success: true,
      changes: result.meta?.changes || 0,
      channel_name: body.channel_name 
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error incrementing db_reads:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to increment db_reads',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Increments the successful_lookups counter for a channel
 */
async function handleIncrementSuccessfulLookups(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const channelName = validateChannelName(body.channel_name);
    
    const query = `UPDATE \`users\` SET successful_lookups = successful_lookups + 1 WHERE channel_name = ?`;
    const result = await env.DB.prepare(query).bind(channelName).run();
    
    return new Response(JSON.stringify({ 
      success: true,
      changes: result.meta?.changes || 0,
      channel_name: body.channel_name 
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error incrementing successful_lookups:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to increment successful_lookups',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Handles user registration when connecting Twitch account
 */
async function handleUserRegistration(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const { 
      twitch_id, 
      twitch_username, 
      display_name,
      type,
      broadcaster_type,
      description,
      profile_image_url,
      offline_image_url,
      view_count,
      email,
      twitch_created_at
    } = body;
    
    // Validate required fields
    if (!twitch_id || !twitch_username) {
      return createErrorResponse(400, 'Missing required fields: twitch_id and twitch_username are required');
    }
    
    // Basic email validation if provided
    if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return createErrorResponse(400, 'Invalid email format');
    }
    
    // Insert or update user with complete Twitch data
    const query = `
      INSERT INTO \`users\` 
        (twitch_id, channel_name, display_name, type, broadcaster_type, description, 
         profile_image_url, offline_image_url, view_count, email, twitch_created_at,
         channel_active, db_reads, successful_lookups)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0)
      ON CONFLICT (twitch_id) DO UPDATE SET
        channel_name = excluded.channel_name,
        display_name = excluded.display_name,
        type = excluded.type,
        broadcaster_type = excluded.broadcaster_type,
        description = excluded.description,
        profile_image_url = excluded.profile_image_url,
        offline_image_url = excluded.offline_image_url,
        view_count = excluded.view_count,
        email = excluded.email,
        twitch_created_at = excluded.twitch_created_at,
        updated_at = CURRENT_TIMESTAMP
    `;
    
    await env.DB.prepare(query).bind(
      twitch_id, 
      twitch_username.toLowerCase(), 
      display_name || null,
      type || null,
      broadcaster_type || null,
      description || null,
      profile_image_url || null,
      offline_image_url || null,
      view_count || 0,
      email || null,
      twitch_created_at || null
    ).run();
    
    return new Response(JSON.stringify({ 
      success: true,
      message: 'User registered successfully',
      user_data: {
        twitch_id,
        channel_name: twitch_username,
        display_name,
        type,
        broadcaster_type,
        description,
        profile_image_url,
        offline_image_url,
        view_count,
        email,
        twitch_created_at
      }
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error registering user:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to register user',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Handles dashboard data retrieval for a channel
 */
// Removed legacy handleDashboardData

/**
 * Handles dashboard data retrieval by twitch_id
 */
async function handleDashboardDataById(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const { twitch_id } = body || {};
    if (!twitch_id) {
      return createErrorResponse(400, 'Missing twitch_id parameter', null, corsHeaders);
    }

    const query = `
      SELECT db_reads, successful_lookups, channel_active, display_name
      FROM \`users\` WHERE twitch_id = ? LIMIT 1
    `;

    const result = await env.DB.prepare(query).bind(twitch_id).first();

    if (!result) {
      return createErrorResponse(404, 'Channel not found or not active', null, corsHeaders);
    }

    let badgeDisplayRate = '-';
    if (result.db_reads > 0) {
      badgeDisplayRate = Math.round((result.successful_lookups / result.db_reads) * 100);
    }

    return new Response(JSON.stringify({
      db_reads: result.db_reads,
      successful_lookups: result.successful_lookups,
      badge_display_rate: badgeDisplayRate,
      channel_active: result.channel_active,
      display_name: result.display_name
    }), {
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error fetching dashboard data by id:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to fetch dashboard data',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Handles updating the channel_active status for a channel
 */
// Removed legacy handleChannelActiveUpdate

/**
 * Handles updating the channel_active status for a channel by twitch_id
 */
async function handleChannelActiveUpdateById(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const { twitch_id, channel_active } = body;

    if (!twitch_id) {
      return createErrorResponse(400, 'Missing twitch_id parameter');
    }

    if (typeof channel_active !== 'boolean' && channel_active !== 0 && channel_active !== 1) {
      return createErrorResponse(400, 'channel_active must be a boolean or 0/1');
    }

    // Authorization: internal service-only using secret header
    const providedInternal = request.headers.get('X-Internal-Auth');
    const expectedInternal = env.USERS_WRITE_KEY;
    if (!expectedInternal || providedInternal !== expectedInternal) {
      return createErrorResponse(401, 'Unauthorized: missing or invalid internal auth', null, corsHeaders);
    }

    const activeValue = channel_active === true || channel_active === 1 ? 1 : 0;
    const query = `
      UPDATE \`users\` SET channel_active = ?, updated_at = CURRENT_TIMESTAMP
      WHERE twitch_id = ?
    `;

    const result = await env.DB.prepare(query).bind(activeValue, twitch_id).run();

    if (result.meta?.changes === 0) {
      return createErrorResponse(404, 'Channel not found or no changes made', null, corsHeaders);
    }

    return new Response(JSON.stringify({
      success: true,
      message: 'Channel active status updated successfully',
      twitch_id,
      channel_active: activeValue,
      changes: result.meta?.changes || 0
    }), {
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error updating channel active status by id:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to update channel active status',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Handles user lookup by twitch_id for riot auth
 */
async function handleUserLookup(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const { twitch_id } = body;
    
    if (!twitch_id) {
      return createErrorResponse(400, 'Missing twitch_id parameter', null, corsHeaders);
    }
    // Require internal auth for this lookup to avoid exposing mappings publicly
    const providedInternal = request.headers.get('X-Internal-Auth');
    const expectedInternal = env.USERS_WRITE_KEY;
    if (!expectedInternal || providedInternal !== expectedInternal) {
      return createErrorResponse(401, 'Unauthorized: missing or invalid internal auth', null, corsHeaders);
    }
    
    const query = `
      SELECT channel_name FROM \`users\` 
      WHERE twitch_id = ?
      LIMIT 1
    `;
    
    const result = await env.DB.prepare(query).bind(twitch_id).first();
    
    if (!result) {
      return createErrorResponse(404, 'User not found', null, corsHeaders);
    }
    
    return new Response(JSON.stringify({ 
      channel_name: result.channel_name
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error looking up user:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to lookup user',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}

/**
 * Handles riot data fallback lookup by twitch_id
 * Allows users to retrieve their own riot data if it exists in the database
 */
async function handleRiotDataFallback(request, env, corsHeaders) {
  try {
    const body = await parseRequestBody(request);
    const { twitch_id } = body;
    
    if (!twitch_id) {
      return createErrorResponse(400, 'Missing twitch_id parameter', null, corsHeaders);
    }
    
    // First, get the twitch username from the users table
    const userQuery = `
      SELECT channel_name FROM \`users\` 
      WHERE twitch_id = ?
      LIMIT 1
    `;
    
    const userResult = await env.DB.prepare(userQuery).bind(twitch_id).first();
    
    if (!userResult) {
      return createErrorResponse(404, 'User not found', null, corsHeaders);
    }
    
    // Now look up riot data using the twitch username
    const riotQuery = `
      SELECT riot_puuid, riot_id, rank_tier, rank_division, lp, region
      FROM lol_ranks 
      WHERE twitch_username = ?
      LIMIT 1
    `;
    
    const riotResult = await env.DB.prepare(riotQuery).bind(userResult.channel_name).first();
    
    if (!riotResult) {
      return createErrorResponse(404, 'No riot account found for this user', null, corsHeaders);
    }
    
    // Return the riot data needed by the extension
    return new Response(JSON.stringify({ 
      success: true,
      riot_data: {
        puuid: riotResult.riot_puuid,
        riotId: riotResult.riot_id,
        rankInfo: {
          tier: riotResult.rank_tier,
          rank: riotResult.rank_division,
          leaguePoints: riotResult.lp
        },
        region: riotResult.region
      }
    }), { 
      status: 200,
      headers: corsHeaders
    });
  } catch (error) {
    console.error('Error in riot data fallback lookup:', error);
    return createErrorResponse(
      error.message === 'Invalid JSON' ? 400 : 500,
      error.message === 'Invalid JSON' ? 'Invalid JSON' : 'Failed to lookup riot data',
      error.message === 'Invalid JSON' ? null : error.message,
      corsHeaders
    );
  }
}