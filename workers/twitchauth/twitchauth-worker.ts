/**
 * EloWard API - Twitch OAuth Authentication Proxy
 * 
 * This Cloudflare Worker serves as a secure proxy for Twitch OAuth authentication,
 * protecting client credentials while facilitating the OAuth2 flow with Twitch API.
 */

import { Router } from 'itty-router';

// Cloudflare Workers types
interface Fetcher {
  fetch: (request: Request) => Promise<Response>;
}

// Define environment variable interface
interface Env {
  TWITCH_CLIENT_ID: string;
  TWITCH_CLIENT_SECRET: string;
  USERS_WORKER: Fetcher; // Service binding to users-worker
  USERS_WRITE_KEY: string;
}

// Define interface for auth init request
interface AuthInitRequest {
  state: string;
  scopes?: string;
  redirect_uri?: string;
}

// Define interface for token request
interface TokenRequest {
  code: string;
}

// Define interface for refresh token request
interface RefreshTokenRequest {
  refresh_token: string;
}

// Define interface for validate token request
interface ValidateTokenRequest {
  access_token: string;
}

// Define interface for store user request
interface StoreUserRequest {
  twitch_token: string;
}

// Define interface for Twitch token response
interface TwitchTokenResponse {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  scope: string[];
  token_type: string;
  error?: string;
  error_description?: string;
}

// Define interface for Twitch user info
interface TwitchUserInfo {
  id: string;
  login: string;
  display_name: string;
  type: string;
  broadcaster_type: string;
  description: string;
  profile_image_url: string;
  offline_image_url: string;
  view_count: number;
  email: string;
  created_at: string;
}

// Define the standard redirect URI throughout the app
const STANDARD_REDIRECT_URI = "https://www.eloward.com/twitch/auth/redirect";

// Create a router
const router = Router();

// Helper function for CORS responses
function corsResponse(status: number, data: any): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}

// Health check endpoint
router.get('/health', () => {
  return corsResponse(200, { status: 'ok', service: 'eloward-twitchauth' });
});

// Handle OPTIONS requests for CORS
router.options('*', () => {
  return corsResponse(200, {});
});

// Redirect handler (parity with Riot): forward Twitch code/state to website
router.get('/auth/twitch/redirect', async (request: Request) => {
  try {
    const url = new URL(request.url);
    const code = url.searchParams.get('code');
    const state = url.searchParams.get('state');
    const error = url.searchParams.get('error');
    const errorDescription = url.searchParams.get('error_description');

    if (code && state) {
      return Response.redirect(
        `https://www.eloward.com/twitch/auth/redirect?code=${encodeURIComponent(code)}&state=${encodeURIComponent(state)}`,
        302
      );
    }

    if (error) {
      return new Response(JSON.stringify({ error, error_description: errorDescription || 'No description' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response(JSON.stringify({ error: 'missing_parameters' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: 'server_error', error_description: e?.message || 'Unknown' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});

// Consolidated tokenless complete flow for Twitch (no tokens returned to clients)
// POST /twitch/auth { code, redirect_uri? }
router.post('/twitch/auth', async (request: Request, env: Env) => {
  try {
    const body = await request.json().catch(() => ({}));
    const code = body?.code as string | undefined;
    const redirect_uri = (body?.redirect_uri as string | undefined) || STANDARD_REDIRECT_URI;

    if (!code) {
      return corsResponse(400, { error: 'missing_parameters', error_description: 'Required parameter: code' });
    }

    // 1) Exchange code for tokens (server-side only)
    const tokenUrl = 'https://id.twitch.tv/oauth2/token';
    const tokenParams = new URLSearchParams();
    tokenParams.append('client_id', env.TWITCH_CLIENT_ID);
    tokenParams.append('client_secret', env.TWITCH_CLIENT_SECRET);
    tokenParams.append('code', code);
    tokenParams.append('grant_type', 'authorization_code');
    tokenParams.append('redirect_uri', redirect_uri);

    const tokenResponse = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: tokenParams,
    });

    const tokenData = (await tokenResponse.json()) as TwitchTokenResponse;
    if (!tokenResponse.ok || (tokenData as any)?.error) {
      return corsResponse(tokenResponse.status || 400, {
        error: (tokenData as any)?.error || 'token_exchange_error',
        error_description: (tokenData as any)?.error_description || 'Failed to exchange code for token',
      });
    }

    // 2) Fetch user info from Twitch
    const userResponse = await fetch('https://api.twitch.tv/helix/users', {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${tokenData.access_token}`,
        'Client-Id': env.TWITCH_CLIENT_ID,
      },
    });

    if (!userResponse.ok) {
      const errText = await userResponse.text();
      return corsResponse(userResponse.status, { error: 'twitch_user_error', error_description: errText });
    }

    const userJson = await userResponse.json();
    const twitchUser = (userJson?.data && userJson.data[0]) as TwitchUserInfo | undefined;
    if (!twitchUser?.id || !twitchUser?.login) {
      return corsResponse(500, { error: 'invalid_user_data', error_description: 'Missing id/login from Twitch' });
    }

    // 3) Register/update user in Users worker
    const userDataForStorage = {
      twitch_id: twitchUser.id,
      twitch_username: twitchUser.login,
      display_name: twitchUser.display_name,
      type: twitchUser.type,
      broadcaster_type: twitchUser.broadcaster_type,
      description: twitchUser.description,
      profile_image_url: twitchUser.profile_image_url,
      offline_image_url: twitchUser.offline_image_url,
      view_count: twitchUser.view_count,
      email: (twitchUser as any)?.email || null,
      twitch_created_at: twitchUser.created_at,
    };

    const usersWorkerRequest = new Request('https://users-worker/user/register', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.USERS_WRITE_KEY
      },
      body: JSON.stringify(userDataForStorage),
    });

    const usersWorkerResponse = await env.USERS_WORKER.fetch(usersWorkerRequest);
    if (!usersWorkerResponse.ok) {
      const tx = await usersWorkerResponse.text();
      return corsResponse(usersWorkerResponse.status, { error: 'user_register_failed', error_description: tx });
    }

    // 4) Return minimal user info to client (no tokens, no email)
    return corsResponse(200, {
      success: true,
      user_data: {
        id: twitchUser.id,
        login: twitchUser.login,
        display_name: twitchUser.display_name,
        profile_image_url: twitchUser.profile_image_url
      },
    });
  } catch (error: any) {
    return corsResponse(500, { error: 'server_error', error_description: error?.message || 'Unknown error' });
  }
});

// Initialize Twitch OAuth
// Removed legacy init endpoint: clients now build URL directly

// Exchange authorization code for access token
// Removed legacy token exchange endpoint

// Refresh access token
// Removed legacy token refresh endpoint

// Validate access token
// Removed legacy validate endpoint

// Get user information
// Removed legacy user info endpoint

// Secure endpoint for fetching and storing user data
// Removed legacy store-user endpoint

// Catch-all handler for 404s
router.all('*', () => {
  return corsResponse(404, { error: 'Not found' });
});

// Main fetch handler
export default {
  async fetch(request: Request, env: Env, ctx: any): Promise<Response> {
    // Handle CORS preflight requests
    if (request.method === 'OPTIONS') {
      return corsResponse(200, {});
    }
    
    // Handle all other requests via the router
    return router.handle(request, env, ctx).catch((error: any) => {
      return corsResponse(500, { error: 'Internal Server Error' });
    });
  },
}; 