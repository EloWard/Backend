/**
 * EloWard API - Riot RSO Authentication Proxy
 * 
 * This Cloudflare Worker serves as a secure proxy for Riot RSO authentication,
 * protecting client credentials while facilitating the OAuth2 flow with Riot Games API.
 */

import { Router } from 'itty-router';

// Cloudflare Workers types
interface Fetcher {
  fetch: (request: Request) => Promise<Response>;
}

// Define environment variable interface
interface Env {
  RIOT_CLIENT_ID: string;
  RIOT_API_KEY: string;
  RIOT_CLIENT_SECRET: string;
  RANK_WORKER: Fetcher; // Service binding to rank-worker
  TWITCH_AUTH_WORKER: Fetcher; // Service binding to twitchauth-worker
  USERS_WORKER: Fetcher; // Service binding to users-worker
  // Secret for internal service-to-service calls to rank-worker (set in wrangler)
  RANK_WRITE_KEY?: string;
  INTERNAL_WRITE_KEY?: string;
  USERS_WRITE_KEY?: string;
}

// Define interface for token request
interface TokenRequest {
  code: string;
}

// Define interface for league entry
interface LeagueEntry {
  leagueId?: string;
  puuid: string;
  queueType: string;
  tier?: string;
  rank?: string;
  leaguePoints?: number;
  wins?: number;
  losses?: number;
  hotStreak?: boolean;
  veteran?: boolean;
  freshBlood?: boolean;
  inactive?: boolean;
}

// Define interface for account data
interface RiotAccountData {
  puuid: string;
  gameName: string;
  tagLine: string;
}

// Define interface for Riot token response
interface RiotTokenResponse {
  access_token: string;
  refresh_token: string;
  id_token: string;
  expires_in: number;
  scope: string;
  token_type: string;
  error?: string;
  error_description?: string;
}


// Define interface for store rank request  
interface StoreRankRequest {
  twitch_token: string;
  riot_token: string;
  region: string;
  twitch_username: string;
}

// Define interface for complete auth request
interface CompleteAuthRequest {
  code: string;
  twitch_id: string;
  region: string;
}


// Create a new router
const router = Router();

// Add CORS headers to all responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

// Define the standard redirect URI throughout the app
const STANDARD_REDIRECT_URI = "https://www.eloward.com/riot/auth/redirect";

// Add a helper function for consistent CORS responses
function corsResponse(status: number, data: any): Response {
  return new Response(
    JSON.stringify(data),
    {
      status,
      headers: {
        ...corsHeaders,
        'Content-Type': 'application/json',
      }
    }
  );
}

// Handle OPTIONS requests for CORS preflight
router.options('*', () => {
  return new Response(null, {
    status: 204,
    headers: corsHeaders,
  });
});


// Authentication initialization endpoint
router.get('/auth/init', async (request, env) => {
  try {
    // Extract query parameters
    const url = new URL(request.url);
    const state = url.searchParams.get('state');
    
    // Validate required parameters
    if (!state) {
      return new Response(JSON.stringify({ 
        error: 'missing_parameters',
        error_description: 'Required parameter: state' 
      }), {
        status: 400,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Using only the minimum required scopes for Riot RSO
    const minimumScopes = 'openid offline_access lol cpid';
    
    // Build authorization URL
    const params = new URLSearchParams({
      client_id: env.RIOT_CLIENT_ID,
      redirect_uri: STANDARD_REDIRECT_URI,
      response_type: 'code',
      scope: minimumScopes,
      state: state
    });
    
    const authUrl = `https://auth.riotgames.com/authorize?${params.toString()}`;
    
    // Return the authorization URL with the expected field name
    return new Response(JSON.stringify({ 
      authorizationUrl: authUrl
    }), {
      status: 200,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  } catch (error) {
    return new Response(JSON.stringify({ 
      error: 'server_error',
      error_description: error instanceof Error ? error.message : String(error)
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  }
});

// Handle redirect from Riot RSO
router.get('/auth/redirect', async (request, env) => {
  try {
    // Extract query parameters
    const url = new URL(request.url);
    const code = url.searchParams.get('code');
    const state = url.searchParams.get('state');
    const error = url.searchParams.get('error');
    const errorDescription = url.searchParams.get('error_description');
    
    // Always redirect to the website redirect page for cross-browser reliability (Chrome/Firefox)
    if (code && state) {
      return Response.redirect(
        `https://www.eloward.com/riot/auth/redirect?code=${encodeURIComponent(code)}&state=${encodeURIComponent(state)}`,
        302
      );
    }

    // Handle error case
    if (error) {
      return new Response(`
        <html>
          <head>
            <title>Authentication Failed</title>
            <style>
              body {
                font-family: 'Inter', sans-serif;
                background-color: #0A0A0A;
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                margin: 0;
                padding: 20px;
                text-align: center;
              }
              h1 {
                color: #D9A336;
                margin-bottom: 10px;
              }
              p {
                margin: 10px 0;
                font-size: 16px;
                line-height: 1.5;
              }
              .error {
                color: #F44336;
                font-weight: bold;
              }
              .container {
                max-width: 600px;
                background-color: rgba(34, 49, 63, 0.9);
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
                border: 2px solid #F44336;
              }
            </style>
          </head>
          <body>
            <div class="container">
              <h1>Authentication Failed</h1>
              <p class="error">${error}</p>
              <p>${errorDescription || 'No description provided'}</p>
              <p>Please close this window and try again.</p>
            </div>
          </body>
        </html>
      `, {
        status: 400,
        headers: {
          'Content-Type': 'text/html',
          ...corsHeaders
        }
      });
    }
    
    // Validate required parameters
    if (!code || !state) {
      return new Response(`
        <html>
          <head>
            <title>Authentication Failed</title>
            <style>
              body {
                font-family: 'Inter', sans-serif;
                background-color: #0A0A0A;
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                margin: 0;
                padding: 20px;
                text-align: center;
              }
              h1 {
                color: #D9A336;
                margin-bottom: 10px;
              }
              p {
                margin: 10px 0;
                font-size: 16px;
                line-height: 1.5;
              }
              .error {
                color: #F44336;
                font-weight: bold;
              }
              .container {
                max-width: 600px;
                background-color: rgba(34, 49, 63, 0.9);
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
                border: 2px solid #F44336;
              }
            </style>
          </head>
          <body>
            <div class="container">
              <h1>Authentication Failed</h1>
              <p class="error">Missing required parameters</p>
              <p>The authentication response is missing required parameters: code and state</p>
              <p>Please close this window and try again.</p>
            </div>
          </body>
        </html>
      `, {
        status: 400,
        headers: {
          'Content-Type': 'text/html',
          ...corsHeaders
        }
      });
    }
    
    // Should not reach here for success cases due to redirect above
  } catch (error) {
    return new Response(`
      <html>
        <head><title>Authentication Failed</title></head>
        <body>
          <h1>Authentication Failed</h1>
          <p>Server error: ${error instanceof Error ? error.message : String(error)}</p>
          <p>Please close this window and try again.</p>
        </body>
      </html>
    `, {
      status: 500,
      headers: {
        'Content-Type': 'text/html',
        ...corsHeaders
      }
    });
  }
});

// Optimized single-call auth endpoint
router.post('/auth/complete', async (request: Request, env: Env) => {
  try {
    const body = await request.json() as CompleteAuthRequest;
    const { code, twitch_id, region } = body;
    
    console.log('[RiotAuth] /auth/complete request:', { 
      hasCode: !!code, 
      codeLength: code?.length, 
      twitch_id, 
      region 
    });
    
    if (!code || !twitch_id || !region) {
      return corsResponse(400, {
        error: 'missing_parameters',
        error_description: 'Required parameters: code, twitch_id, region'
      });
    }

    // Step 1: Exchange code for Riot tokens
    const tokenUrl = 'https://auth.riotgames.com/token';
    const params = new URLSearchParams();
    params.append('grant_type', 'authorization_code');
    params.append('code', code);
    params.append('redirect_uri', STANDARD_REDIRECT_URI);
    
    console.log('[RiotAuth] Token exchange params:', {
      redirect_uri: STANDARD_REDIRECT_URI,
      code_prefix: code.substring(0, 20) + '...'
    });
    
    const basicAuth = btoa(`${env.RIOT_CLIENT_ID}:${env.RIOT_CLIENT_SECRET}`);
    
    const tokenResponse = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${basicAuth}`
      },
      body: params.toString(),
    });
    
    if (!tokenResponse.ok) {
      const errorText = await tokenResponse.text();
      console.log('[RiotAuth] Token exchange failed:', {
        status: tokenResponse.status,
        error: errorText
      });
      return corsResponse(tokenResponse.status, {
        error: 'token_exchange_error',
        error_description: errorText
      });
    }
    
    const tokenData = await tokenResponse.json() as RiotTokenResponse;
    if (tokenData.error) {
      return corsResponse(400, {
        error: 'token_exchange_error',
        error_description: tokenData.error_description || tokenData.error
      });
    }

    // Step 2: Get Riot account info using token
    let accountData: RiotAccountData;
    try {
      accountData = await validateRiotTokenAndGetAccount(tokenData.access_token);
    } catch (error) {
      return corsResponse(401, {
        error: 'invalid_riot_token',
        error_description: error instanceof Error ? error.message : String(error)
      });
    }

    // Step 3: Look up channel_name from users table using twitch_id
    const getUserRequest = new Request('https://users-worker/user/lookup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ twitch_id })
    });

    const getUserResponse = await env.USERS_WORKER.fetch(getUserRequest);
    if (!getUserResponse.ok) {
      return corsResponse(getUserResponse.status, {
        error: 'twitch_user_not_found',
        error_description: 'Twitch user not found in database. Please connect Twitch first.'
      });
    }

    const userData = await getUserResponse.json();
    const channelName = userData.channel_name;

    // Step 4: Fetch current rank from Riot API
    const leagueUrl = `https://${region}.api.riotgames.com/lol/league/v4/entries/by-puuid/${accountData.puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: { 'X-Riot-Token': env.RIOT_API_KEY }
    });

    if (!leagueResponse.ok) {
      return corsResponse(leagueResponse.status, {
        error: 'riot_api_error',
        error_description: 'Failed to fetch rank data from Riot API'
      });
    }

    const leagueData = await leagueResponse.json() as LeagueEntry[];
    const currentRank = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');

    // Step 5: Store rank data in database (twitch_username field contains channel_name)
    const riotId = `${accountData.gameName}#${accountData.tagLine}`;
    const rankData = {
      riot_puuid: accountData.puuid,
      twitch_username: channelName, // This is the channel_name from users table
      riot_id: riotId,
      rank_tier: currentRank ? currentRank.tier : 'UNRANKED',
      rank_division: currentRank ? currentRank.rank : null,
      lp: currentRank ? currentRank.leaguePoints : 0,
      region: region
    };

    const storeRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-Auth': (env.RANK_WRITE_KEY || env.INTERNAL_WRITE_KEY || '')
      },
      body: JSON.stringify(rankData)
    });

    const storeResponse = await env.RANK_WORKER.fetch(storeRequest);
    if (!storeResponse.ok) {
      const errorText = await storeResponse.text();
      return corsResponse(storeResponse.status, {
        error: 'database_error',
        error_description: 'Failed to store rank data'
      });
    }

    // Step 6: Return complete user data for frontend
    return corsResponse(200, {
      status: 'success',
      message: 'Authentication completed successfully',
      data: {
        riotId: riotId,
        puuid: accountData.puuid,
        region: region,
        soloQueueRank: currentRank ? {
          tier: currentRank.tier,
          rank: currentRank.rank,
          leaguePoints: currentRank.leaguePoints,
          wins: currentRank.wins,
          losses: currentRank.losses
        } : null
      }
    });
  } catch (error) {
    return corsResponse(500, {
      error: 'server_error',
      error_description: error instanceof Error ? error.message : String(error)
    });
  }
});

// Add compatibility endpoint for POST /auth/token
router.post('/auth/token', async (request, env) => {
  try {
    // Parse request body
    const body = await request.json() as TokenRequest;
    const { code } = body;
    
    // Validate required parameters
    if (!code) {
      return new Response(JSON.stringify({ 
        error: 'missing_parameters',
        error_description: 'Required parameter: code' 
      }), {
        status: 400,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Token exchange with Riot
    const tokenUrl = 'https://auth.riotgames.com/token';
    const params = new URLSearchParams();
    params.append('grant_type', 'authorization_code');
    params.append('code', code);
    params.append('redirect_uri', STANDARD_REDIRECT_URI);
    
    // Create Basic auth header using client_id and client_secret
    const basicAuth = btoa(`${env.RIOT_CLIENT_ID}:${env.RIOT_CLIENT_SECRET}`);
    
    const tokenResponse = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${basicAuth}`
      },
      body: params.toString(),
    });
    
    if (!tokenResponse.ok) {
      const errorText = await tokenResponse.text();
      return new Response(JSON.stringify({ 
        error: 'token_exchange_error',
        error_description: errorText,
        status: tokenResponse.status
      }), {
        status: tokenResponse.status,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Return token response to client
    const tokenData = await tokenResponse.json() as RiotTokenResponse;
    
    // Return the complete token data exactly as received from Riot
    return new Response(JSON.stringify(tokenData), {
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  } catch (error) {
    return new Response(JSON.stringify({ 
      error: 'server_error',
      error_description: error instanceof Error ? error.message : 'Unknown error'
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  }
});


// Get account info from Riot Account v1 API - uses americas endpoint for all regions
router.get('/riot/account/:region', async (request, env) => {
  try {
    // Get access token from Authorization header
    const authorization = request.headers.get('Authorization');
    if (!authorization || !authorization.startsWith('Bearer ')) {
      return new Response(JSON.stringify({ 
        error: 'missing_token',
        error_description: 'Authorization header with Bearer token is required' 
      }), {
        status: 401,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    const token = authorization.replace('Bearer ', '');
    
    // Call Riot Account v1 API - always use americas endpoint
    const accountUrl = `https://americas.api.riotgames.com/riot/account/v1/accounts/me`;
    const accountResponse = await fetch(accountUrl, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    
    if (!accountResponse.ok) {
      const errorText = await accountResponse.text();
      return new Response(JSON.stringify({ 
        error: 'riot_api_error',
        error_description: errorText,
        status: accountResponse.status
      }), {
        status: accountResponse.status,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Return account data to client
    const accountData = await accountResponse.json();
    return new Response(JSON.stringify(accountData), {
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  } catch (error) {
    return new Response(JSON.stringify({ 
      error: 'server_error',
      error_description: error instanceof Error ? error.message : 'Unknown error'
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  }
});

// League entries endpoint with PUUID support
router.get('/riot/league/entries', async (request, env) => {
  try {
    // Get parameters from query
    const url = new URL(request.url);
    const region = url.searchParams.get('region') || 'na1';
    const puuid = url.searchParams.get('puuid');
    
    // Validate parameters
    if (!puuid) {
      return new Response(JSON.stringify({ 
        error: 'missing_parameters',
        error_description: 'Required parameters: puuid' 
      }), {
        status: 400,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Call League v4 API with PUUID
    const leagueUrl = `https://${region}.api.riotgames.com/lol/league/v4/entries/by-puuid/${puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: {
        'X-Riot-Token': env.RIOT_API_KEY
      }
    });
    
    if (!leagueResponse.ok) {
      const errorText = await leagueResponse.text();
      return new Response(JSON.stringify({ 
        error: 'riot_api_error',
        error_description: errorText,
        status: leagueResponse.status
      }), {
        status: leagueResponse.status,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Return league data to client
    const leagueData = await leagueResponse.json() as LeagueEntry[];
    
    // Filter to get just Solo/Duo queue data if available
    const rankedSoloData = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');
    
    if (rankedSoloData) {
      return new Response(JSON.stringify(rankedSoloData), {
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    } else {
      // If no ranked solo data, return all data
      return new Response(JSON.stringify(leagueData), {
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
  } catch (error) {
    return new Response(JSON.stringify({ 
      error: 'server_error',
      error_description: error instanceof Error ? error.message : 'Unknown error'
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  }
});

// Get league entries for a player by PUUID
router.get('/riot/league/:platform/:puuid', async (request, env) => {
  try {
    // Get parameters
    const { platform, puuid } = request.params;
    
    // Validate parameters
    if (!platform || !puuid) {
      return new Response(JSON.stringify({ 
        error: 'missing_parameters',
        error_description: 'Required parameters: platform, puuid' 
      }), {
        status: 400,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Call League v4 API with PUUID
    const leagueUrl = `https://${platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/${puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: {
        'X-Riot-Token': env.RIOT_API_KEY
      }
    });
    
    if (!leagueResponse.ok) {
      const errorText = await leagueResponse.text();
      return new Response(JSON.stringify({ 
        error: 'riot_api_error',
        error_description: errorText,
        status: leagueResponse.status
      }), {
        status: leagueResponse.status,
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
    
    // Return league data to client
    const leagueData = await leagueResponse.json() as LeagueEntry[];
    
    // Filter to get just Solo/Duo queue data if available
    const rankedSoloData = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');
    
    if (rankedSoloData) {
      return new Response(JSON.stringify(rankedSoloData), {
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    } else {
      // If no ranked solo data, return all data
      return new Response(JSON.stringify(leagueData), {
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      });
    }
  } catch (error) {
    return new Response(JSON.stringify({ 
      error: 'server_error',
      error_description: error instanceof Error ? error.message : 'Unknown error'
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders
      }
    });
  }
});

// Simplified rank refresh endpoint that uses PUUID only
router.post('/riot/refreshrank', async (request: Request, env: Env) => {
  try {
    const { puuid } = await request.json();

    if (!puuid) {
      return corsResponse(400, {
        status: 'error',
        message: 'Missing required parameter: puuid',
      });
    }

    // Look up user in lol_ranks database to get region
    const getUserRequest = new Request('https://rank-worker/api/ranks/lol/by-puuid', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ puuid })
    });

    const getUserResponse = await env.RANK_WORKER.fetch(getUserRequest);
    
    if (!getUserResponse.ok) {
      // User not found in database - clear frontend persistent data
      return corsResponse(404, {
        status: 'error',
        message: 'User not found. Please reconnect your account.',
        action: 'clear_persistent_data'
      });
    }

    const userData = await getUserResponse.json();
    const { region, twitch_username } = userData;

    // Fetch current rank from Riot API using PUUID
    const leagueUrl = `https://${region}.api.riotgames.com/lol/league/v4/entries/by-puuid/${puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: {
        'X-Riot-Token': env.RIOT_API_KEY
      }
    });

    if (!leagueResponse.ok) {
      // Riot API error - clear frontend persistent data
      return corsResponse(leagueResponse.status, {
        status: 'error',
        message: 'Failed to fetch rank from Riot API. Please reconnect your account.',
        action: 'clear_persistent_data',
        riot_error: await leagueResponse.text()
      });
    }

    const leagueData = await leagueResponse.json() as LeagueEntry[];
    const currentRank = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');

    // Update database with new rank data
    const rankData = {
      riot_puuid: puuid,
      twitch_username: twitch_username,
      riot_id: userData.riot_id,
      rank_tier: currentRank ? currentRank.tier : 'UNRANKED',
      rank_division: currentRank ? currentRank.rank : null,
      lp: currentRank ? currentRank.leaguePoints : 0,
      region: region
    };

    const updateRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': (env.RANK_WRITE_KEY || env.INTERNAL_WRITE_KEY || '')
      },
      body: JSON.stringify(rankData)
    });

    const updateResponse = await env.RANK_WORKER.fetch(updateRequest);

    if (!updateResponse.ok) {
      const errorText = await updateResponse.text();
      return corsResponse(updateResponse.status, {
        status: 'error',
        message: 'Failed to update rank data',
        error: errorText
      });
    }

    // Return the updated rank data to frontend
    return corsResponse(200, {
      status: 'success',
      message: 'Rank refreshed successfully',
      data: {
        tier: rankData.rank_tier,
        rank: rankData.rank_division,
        lp: rankData.lp,
        region: rankData.region
      }
    });
  } catch (error) {
    return corsResponse(500, {
      status: 'error',
      message: 'Internal server error during rank refresh',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// Secure endpoint for fetching and storing rank data
router.post('/store-rank', async (request: Request, env: Env) => {
  try {
    const body = await request.json() as StoreRankRequest;
    const { twitch_token, riot_token, region, twitch_username } = body;

    if (!twitch_token || !riot_token || !region || !twitch_username) {
      return corsResponse(400, {
        status: 'error',
        message: 'Missing required parameters for storing rank',
      });
    }

    // Verify Twitch token via twitchauth-worker service binding
    const twitchValidateRequest = new Request('https://twitchauth-worker/auth/twitch/validate', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        access_token: twitch_token
      })
    });

    const twitchVerifyResponse = await env.TWITCH_AUTH_WORKER.fetch(twitchValidateRequest);

    if (!twitchVerifyResponse.ok) {
      const errorText = await twitchVerifyResponse.text();
      return corsResponse(twitchVerifyResponse.status, {
        status: 'error',
        message: 'Invalid Twitch token',
        error: errorText
      });
    }

    const twitchUserInfo = await twitchVerifyResponse.json();
    const twitchUserId = twitchUserInfo.user_id;

    if (!twitchUserId) {
      return corsResponse(500, {
        status: 'error',
        message: 'Could not verify Twitch user ID',
      });
    }

    // Get account info using reusable validation function
    let accountData: RiotAccountData;
    try {
      accountData = await validateRiotTokenAndGetAccount(riot_token);
    } catch (error) {
      return corsResponse(401, {
        status: 'error',
        message: 'Invalid or expired riot token',
        error: error instanceof Error ? error.message : String(error)
      });
    }

    const { puuid, gameName, tagLine } = accountData;

    // Fetch current rank data from Riot API using PUUID
    const leagueUrl = `https://${region}.api.riotgames.com/lol/league/v4/entries/by-puuid/${puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: {
        'X-Riot-Token': env.RIOT_API_KEY
      }
    });

    if (!leagueResponse.ok) {
      const errorText = await leagueResponse.text();
      return corsResponse(leagueResponse.status, {
        status: 'error',
        message: 'Error fetching Riot league data',
        error: errorText
      });
    }

    const leagueData = await leagueResponse.json() as LeagueEntry[];
    const currentRank = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');

    // Prepare rank data for storage
    const riotId = `${gameName}#${tagLine}`;
    const rankData = {
      riot_puuid: puuid,
      twitch_username: twitch_username,
      riot_id: riotId,
      rank_tier: currentRank ? currentRank.tier : 'UNRANKED',
      rank_division: currentRank ? currentRank.rank : null,
      lp: currentRank ? currentRank.leaguePoints : 0,
      region: region
    };

    // Call the rank-worker via service binding to store the data
    const rankWorkerRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(rankData)
    });
    
    const rankWorkerResponse = await env.RANK_WORKER.fetch(rankWorkerRequest);

    if (!rankWorkerResponse.ok) {
      const errorText = await rankWorkerResponse.text();
      return corsResponse(rankWorkerResponse.status, {
        status: 'error',
        message: 'Error storing rank data',
        error: errorText
      });
    }

    const rankWorkerResult = await rankWorkerResponse.json();

    return corsResponse(200, {
      status: 'success',
      message: 'Rank data successfully stored/updated',
      data: {
        ...rankData,
        storage_result: rankWorkerResult
      }
    });
  } catch (error) {
    return corsResponse(500, {
      status: 'error',
      message: 'Internal server error storing rank data',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// Disconnect endpoint - uses PUUID only to delete rank data
router.delete('/disconnect', async (request: Request, env: Env) => {
  try {
    const { puuid } = await request.json();
    
    if (!puuid) {
      return corsResponse(400, {
        status: 'error',
        message: 'Missing required parameter: puuid',
      });
    }

    // Delete via rank-worker service binding using PUUID only
    const deleteRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'DELETE',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-Auth': (env.RANK_WRITE_KEY || env.INTERNAL_WRITE_KEY || '')
      },
      body: JSON.stringify({ puuid })
    });

    const deleteResponse = await env.RANK_WORKER.fetch(deleteRequest);
    
    if (!deleteResponse.ok) {
      const errorText = await deleteResponse.text();
      return corsResponse(deleteResponse.status, {
        status: 'error',
        message: 'Failed to delete rank data',
        error: errorText
      });
    }

    const result = await deleteResponse.json();
    
    return corsResponse(200, {
      status: 'success',
      message: 'Riot account disconnected successfully',
      data: result
    });
  } catch (error) {
    return corsResponse(500, {
      status: 'error',
      message: 'Internal server error during disconnect',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// Catch-all route for 404s
router.all('*', () => {
  return new Response(JSON.stringify({ 
    error: 'not_found',
    error_description: 'Endpoint not found' 
  }), {
    status: 404,
    headers: { 
      'Content-Type': 'application/json',
      ...corsHeaders
    }
  });
});


// Reusable function to validate Riot token and get account data - always uses americas endpoint
async function validateRiotTokenAndGetAccount(riot_token: string): Promise<RiotAccountData> {
  const accountUrl = `https://americas.api.riotgames.com/riot/account/v1/accounts/me`;
  const accountResponse = await fetch(accountUrl, {
    headers: {
      'Authorization': `Bearer ${riot_token}`
    }
  });

  if (!accountResponse.ok) {
    const errorText = await accountResponse.text();
    throw new Error(`Error fetching Riot account data: ${errorText}`);
  }

  const accountData = await accountResponse.json() as RiotAccountData;
  const { puuid, gameName, tagLine } = accountData;

  if (!puuid) {
    throw new Error('Could not extract PUUID from account data');
  }

  return { puuid, gameName, tagLine };
}

// Export fetch handler
const workerExport = {
  async fetch(request: Request, env: Env, ctx: any): Promise<Response> {
    try {
      return await router.handle(request, env, ctx);
    } catch (error) {
      return corsResponse(500, {
        status: 'error',
        message: 'An unexpected error occurred',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
};

export default workerExport;