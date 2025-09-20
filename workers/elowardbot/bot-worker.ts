/**
 * EloWard Twitch Bot Worker - Hybrid Architecture
 * 
 * Business logic and token management for the EloWard Twitch bot.
 * Works in conjunction with IRC bot running on AWS Lightsail.
 * - Token management with automatic refresh
 * - Message processing and rank checking
 * - Timeout execution via Twitch Helix API
 * - Channel configuration management
 */

import { Router } from 'itty-router';

type KVNamespace = {
  get: (key: string, type?: 'text' | 'json' | 'arrayBuffer' | 'stream') => Promise<any>;
  put: (key: string, value: string, options?: Record<string, any>) => Promise<void>;
};
type ExecutionContext = { waitUntil(promise: Promise<any>): void; passThroughOnException(): void };
type ScheduledEvent = { cron: string; scheduledTime: number; };

interface Fetcher { fetch: (request: Request) => Promise<Response> }
type D1Database = { 
  prepare: (query: string) => { 
    bind: (...values: any[]) => any; 
    first?: (column?: string) => Promise<any>; 
    all: () => Promise<{ results?: any[] }>; 
    run: () => Promise<any> 
  } 
}

interface Env {
  TWITCH_CLIENT_ID: string;
  TWITCH_CLIENT_SECRET: string;
  WORKER_PUBLIC_URL: string;
  BOT_KV: KVNamespace;
  RANK_WORKER: Fetcher;
  DB: D1Database;
  SITE_BASE_URL?: string;
  BOT_WRITE_KEY?: string;
}

const router = Router();

// Utility functions
function json(status: number, obj: any): Response {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

// Production-optimized logging - only log important events
function log(level: 'error' | 'warn' | 'info' | 'debug', msg: string, obj?: any): void {
  // Only log errors and warnings in production to reduce noise and improve performance
  if (level === 'error' || level === 'warn' || level === 'info') {
    const timestamp = new Date().toISOString();
    if (obj) {
      console.log(`[${level.toUpperCase()}] [${timestamp}] ${msg}`, obj);
    } else {
      console.log(`[${level.toUpperCase()}] [${timestamp}] ${msg}`);
    }
  }
}

// Helper function to get count of enabled channels
async function getEnabledChannelCount(env: Env): Promise<number> {
  try {
    const stmt = env.DB.prepare('SELECT COUNT(*) as count FROM twitch_bot_users WHERE bot_enabled = 1');
    if (typeof stmt.first === 'function') {
      const result = await stmt.first();
      return result ? (result as any).count || 0 : 0;
    }
    // Fallback for different D1 API versions
    const allResult = await stmt.all();
    return (allResult.results?.[0] as any)?.count || 0;
  } catch (e) {
    log('warn', 'Failed to get enabled channel count', { error: String(e) });
    return 0;
  }
}

// Helper function to timeout user via Twitch Helix API
async function timeoutUser(env: Env, channelLogin: string, userLogin: string, duration: number, reason: string) {
  try {
    // Get bot credentials
    const bot = await getBotUserAndToken(env);
    if (!bot?.access || !bot?.user?.id) {
      log('error', 'Bot credentials not available for timeout', { channel: channelLogin, user: userLogin });
      return;
    }
    
    // Get channel ID from database
    const channelData = await env.DB.prepare(`
      SELECT twitch_id FROM twitch_bot_users WHERE channel_name = ?
    `).bind(channelLogin).first();
    
    if (!channelData?.twitch_id) {
      log('error', 'Channel ID not found for timeout', { channel: channelLogin, user: userLogin });
      return;
    }
    
    // Get user ID from Twitch API
    const userResponse = await fetch(`https://api.twitch.tv/helix/users?login=${userLogin}`, {
      headers: {
        'Authorization': `Bearer ${bot.access}`,
        'Client-Id': env.TWITCH_CLIENT_ID,
      },
    });
    
    if (!userResponse.ok) {
      log('error', 'Failed to get user ID for timeout', { user: userLogin, status: userResponse.status });
      return;
    }
    
    const userData = await userResponse.json();
    const userId = userData.data?.[0]?.id;
    if (!userId) {
      log('error', 'User not found for timeout', { user: userLogin });
      return;
    }
    
    // Execute timeout via Helix API
    const timeoutResponse = await fetch(
      `https://api.twitch.tv/helix/moderation/bans?broadcaster_id=${channelData.twitch_id}&moderator_id=${bot.user.id}`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${bot.access}`,
          'Client-Id': env.TWITCH_CLIENT_ID,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          data: {
            user_id: userId,
            duration,
            reason,
          },
        }),
      }
    );
    
    if (timeoutResponse.ok) {
      log('info', 'User timeout successful', { channel: channelLogin, user: userLogin, duration });
    } else {
      const errorText = await timeoutResponse.text();
      log('error', 'User timeout failed', { 
        channel: channelLogin, 
        user: userLogin, 
        status: timeoutResponse.status,
        error: errorText 
      });
    }
  } catch (e) {
    log('error', 'Timeout process failed', { 
      channel: channelLogin, 
      user: userLogin, 
      error: String(e) 
    });
  }
}

// Token management
async function getBotUserAndToken(env: Env) {
  try {
    const stored = await env.BOT_KV.get('bot_tokens', 'json');
    if (!stored) {
      log('warn', 'No stored bot tokens found');
      return null;
    }

    // Handle both storage formats
    let access_token, refresh_token, expires_at, user;
    
    if (stored.tokens) {
      access_token = stored.tokens.access_token;
      refresh_token = stored.tokens.refresh_token;
      expires_at = stored.tokens.expires_at;
      user = stored.user;
    } else {
      access_token = stored.access_token;
      refresh_token = stored.refresh_token;
      expires_at = stored.expires_at;
      user = stored.user;
    }

    const now = Date.now();
    const needsRefresh = expires_at && now >= expires_at - 300000; // 5min buffer

    if (needsRefresh && refresh_token) {
      log('info', 'Token refresh needed - refreshing automatically');
      const refreshed = await refreshBotToken(env, refresh_token);
      if (refreshed) {
        return refreshed;
      }
    }

    // Normalize user object to ensure consistent structure (id instead of user_id)
    let normalizedUser = user;
    if (user && user.user_id && !user.id) {
      normalizedUser = {
        ...user,
        id: user.user_id,
        display_name: user.display_name || user.login
      };
      
      // Update stored token with normalized format to prevent future issues
      const updateData = stored.tokens ? {
        user: normalizedUser,
        tokens: { access_token, refresh_token, expires_at }
      } : {
        access_token, refresh_token, expires_at, user: normalizedUser
      };
      
      env.BOT_KV.put('bot_tokens', JSON.stringify(updateData)).catch(e => 
        log('warn', 'Failed to update normalized token format', { error: String(e) })
      );
    }

    return { access: access_token, refresh: refresh_token, user: normalizedUser };
  } catch (e) {
    log('error', 'Failed to get bot token', { error: String(e) });
    return null;
  }
}

async function refreshBotToken(env: Env, refreshToken: string) {
  try {
    const response = await fetch('https://id.twitch.tv/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_id: env.TWITCH_CLIENT_ID,
        client_secret: env.TWITCH_CLIENT_SECRET,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      log('error', 'Token refresh failed', { 
        status: response.status, 
        error: errorText
      });
      return null;
    }

    const data = await response.json();
    const validationResponse = await validateToken(env, data.access_token);
    if (!validationResponse) {
      log('error', 'Token validation failed after refresh');
      return null;
    }

    // Normalize user object to have consistent structure (id instead of user_id)
    const user = {
      id: validationResponse.user_id,
      login: validationResponse.login,
      display_name: validationResponse.login,
      client_id: validationResponse.client_id,
      scopes: validationResponse.scopes,
      expires_in: validationResponse.expires_in
    };

    // Store in consistent format
    const tokenData = {
      user,
      tokens: {
        access_token: data.access_token,
        refresh_token: data.refresh_token || refreshToken,
        expires_at: Date.now() + data.expires_in * 1000,
      }
    };

    await env.BOT_KV.put('bot_tokens', JSON.stringify(tokenData));
    log('info', 'Token refresh successful', { userLogin: user.login });
    
    return { access: data.access_token, refresh: data.refresh_token || refreshToken, user };
  } catch (e) {
    log('error', 'Token refresh failed', { error: String(e) });
    return null;
  }
}

async function validateToken(env: Env, token: string) {
  try {
    const response = await fetch('https://id.twitch.tv/oauth2/validate', {
      headers: { Authorization: `OAuth ${token}` },
    });
    return response.ok ? await response.json() : null;
  } catch {
    return null;
  }
}

// Dashboard routes
router.post('/bot/config_id', async (req: Request, env: Env) => {
  try {
    const body = await req.json().catch(() => ({} as any));
    const twitch_id = String(body?.twitch_id || '');
    if (!twitch_id) return json(400, { error: 'twitch_id required' });
    
    const cfg = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template, 
             enforcement_mode, min_rank_tier, min_rank_division
      FROM twitch_bot_users WHERE twitch_id = ?
    `).bind(twitch_id).first();
    
    if (!cfg) return json(404, { error: 'not found' });
    
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/dashboard/init', async (req: Request, env: Env) => {
  try {
    const body = await req.json().catch(() => ({} as any));
    const twitch_id = String(body?.twitch_id || '');
    if (!twitch_id) return json(400, { error: 'twitch_id required' });
    
    const cfg = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template,
             enforcement_mode, min_rank_tier, min_rank_division
      FROM twitch_bot_users WHERE twitch_id = ?
    `).bind(twitch_id).first();
    
    const bot_config = cfg ? {
      channel_login: cfg.channel_login,
      bot_enabled: !!cfg.bot_enabled,
      timeout_seconds: cfg.timeout_seconds,
      reason_template: cfg.reason_template,
      enforcement_mode: cfg.enforcement_mode || 'has_rank',
      min_rank_tier: cfg.min_rank_tier || null,
      min_rank_division: cfg.min_rank_division ?? null
    } : null;
    
    return json(200, { 
      bot_active: !!(cfg && cfg.bot_enabled), 
      bot_config 
    });
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

// IRC bot status endpoints (for hybrid architecture compatibility)
router.post('/irc/connect', async (_req: Request, env: Env) => {
  // In hybrid architecture, IRC bot manages its own connection
  log('info', 'IRC connect requested - hybrid architecture uses external IRC bot');
  return json(200, { 
    message: 'IRC bot handles its own connections in hybrid architecture',
    architecture: 'hybrid',
    status: 'delegated'
  });
});

router.post('/irc/reload', async (_req: Request, env: Env) => {
  // In hybrid architecture, IRC bot reloads channels via /channels endpoint
  log('info', 'IRC reload requested - hybrid architecture uses external IRC bot');
  return json(200, { 
    message: 'IRC bot reloads channels automatically in hybrid architecture',
    architecture: 'hybrid', 
    status: 'delegated'
  });
});

router.get('/irc/health', async (_req: Request, env: Env) => {
  // Simple health check for CF Worker
  const channelCount = await getEnabledChannelCount(env);
  return json(200, {
    worker_status: 'healthy',
    architecture: 'hybrid',
    enabled_channels: channelCount,
    timestamp: new Date().toISOString()
  });
});

// Channel management (database-only for hybrid architecture)
router.post('/irc/channel/add', async (req: Request, env: Env) => {
  let channel_login: string | undefined;
  try {
    const body = await req.json();
    ({ channel_login } = body);
    const { twitch_id } = body;
    
    if (!channel_login) {
      return json(400, { error: 'channel_login required' });
    }
    
    // Enable channel in database - IRC bot will pick it up automatically
    await env.DB.prepare(`
      UPDATE twitch_bot_users 
      SET bot_enabled = 1, updated_at = CURRENT_TIMESTAMP
      WHERE channel_name = ? OR twitch_id = ?
    `).bind(channel_login.toLowerCase(), twitch_id).run();
    
    log('info', 'Channel enabled in database', { channel_login, twitch_id });
    
    return json(200, { 
      message: 'Channel enabled - IRC bot will join automatically',
      channel_login,
      twitch_id 
    });
  } catch (e: any) {
    log('error', 'Failed to enable channel', { error: String(e), channel_login: channel_login || 'unknown' });
    return json(500, { error: 'Failed to enable channel' });
  }
});

router.post('/irc/channel/remove', async (req: Request, env: Env) => {
  let channel_login: string | undefined;
  try {
    const body = await req.json();
    ({ channel_login } = body);
    
    if (!channel_login) {
      return json(400, { error: 'channel_login required' });
    }
    
    // Disable channel in database - IRC bot will leave automatically
    await env.DB.prepare(`
      UPDATE twitch_bot_users 
      SET bot_enabled = 0, updated_at = CURRENT_TIMESTAMP
      WHERE channel_name = ?
    `).bind(channel_login.toLowerCase()).run();
    
    log('info', 'Channel disabled in database', { channel_login });
    
    return json(200, { 
      message: 'Channel disabled - IRC bot will leave automatically',
      channel_login
    });
  } catch (e: any) {
    log('error', 'Failed to disable channel', { error: String(e), channel_login: channel_login || 'unknown' });
    return json(500, { error: 'Failed to disable channel' });
  }
});

// IRC Bot Integration - Core message processing endpoint
router.post('/check-message', async (req: Request, env: Env) => {
  try {
    const { channel, user, message } = await req.json();
    
    if (!channel || !user) {
      return json(400, { error: 'channel and user required' });
    }
    
    // Get channel configuration
    const config = await env.DB.prepare(`
      SELECT * FROM twitch_bot_users 
      WHERE channel_name = ? AND bot_enabled = 1
    `).bind(channel.toLowerCase()).first();
    
    if (!config) {
      return json(200, { action: 'skip', reason: 'channel not configured or disabled' });
    }
    
    // Check user rank via rank worker
    try {
      const rankResponse = await env.RANK_WORKER.fetch(
        new Request(`https://internal/api/ranks/lol/${user}`)
      );
      
      if (rankResponse.ok) {
        // User has valid rank - allow message
        return json(200, { action: 'allow', reason: 'user has valid rank' });
      } else {
        // User lacks required rank - timeout
        const duration = config.timeout_seconds || 20;
        const reason = (config.reason_template || "{seconds}s timeout: not enough elo to speak. Link your EloWard at {site}")
          .replace('{seconds}', String(duration))
          .replace('{site}', env.SITE_BASE_URL || 'https://www.eloward.com')
          .replace('{user}', user);
        
        // Execute timeout via Helix API
        await timeoutUser(env, channel, user, duration, reason);
        
        return json(200, { 
          action: 'timeout', 
          reason: 'insufficient rank', 
          duration 
        });
      }
    } catch (e) {
      log('error', 'Rank check failed', { user, error: String(e) });
      // On rank check failure, assume no rank and timeout
      const duration = config.timeout_seconds || 20;
      const reason = (config.reason_template || "{seconds}s timeout: not enough elo to speak. Link your EloWard at {site}")
        .replace('{seconds}', String(duration))
        .replace('{site}', env.SITE_BASE_URL || 'https://www.eloward.com')
        .replace('{user}', user);
      
      await timeoutUser(env, channel, user, duration, reason);
      return json(200, { action: 'timeout', reason: 'rank check failed', duration });
    }
  } catch (e: any) {
    log('error', 'Message check failed', { error: String(e) });
    return json(500, { error: 'message processing failed' });
  }
});

// PRODUCTION TOKEN ENDPOINT - IRC Bot Token Sync
router.get('/token', async (req: Request, env: Env) => {
  try {
    // Get current valid token (with automatic refresh if needed)
    const tokenData = await getBotUserAndToken(env);
    
    if (!tokenData?.access || !tokenData?.user) {
      log('warn', 'No valid bot token available for IRC bot');
      return json(500, { 
        error: 'No valid bot token available',
        shouldReauth: true 
      });
    }
    
    // Calculate expiration info for IRC bot
    const storedTokens = await env.BOT_KV.get('bot_tokens', 'json');
    const expiresAt = storedTokens?.tokens?.expires_at || 0;
    const expiresInMinutes = Math.floor((expiresAt - Date.now()) / 60000);
    const needsRefreshSoon = expiresInMinutes < 30; // Warn if expires in 30min
    
    return json(200, {
      token: tokenData.access.startsWith('oauth:') ? tokenData.access : `oauth:${tokenData.access}`,
      user: {
        login: tokenData.user.login,
        id: tokenData.user.id,
        display_name: tokenData.user.display_name
      },
      expires_at: expiresAt,
      expires_in_minutes: expiresInMinutes,
      needs_refresh_soon: needsRefreshSoon,
      timestamp: Date.now()
    });
  } catch (e: any) {
    log('error', 'Token sync failed', { error: String(e) });
    return json(500, { error: 'Token sync failed', detail: e?.message });
  }
});

// PRODUCTION TOKEN REFRESH ENDPOINT - Force refresh for maintenance  
router.post('/token/refresh', async (req: Request, env: Env) => {
  try {
    log('info', 'Manual token refresh requested');
    
    const currentTokens = await env.BOT_KV.get('bot_tokens', 'json');
    if (!currentTokens?.tokens?.refresh_token) {
      return json(400, { error: 'No refresh token available' });
    }
    
    const refreshed = await refreshBotToken(env, currentTokens.tokens.refresh_token);
    if (!refreshed) {
      return json(500, { error: 'Token refresh failed' });
    }
    
    // Get expiration from storage
    const updatedTokens = await env.BOT_KV.get('bot_tokens', 'json');
    const expiresAt = updatedTokens?.tokens?.expires_at || 0;
    
    return json(200, {
      success: true,
      user: refreshed.user.login,
      expires_in_minutes: Math.floor((expiresAt - Date.now()) / 60000),
      timestamp: Date.now()
    });
  } catch (e: any) {
    log('error', 'Manual token refresh failed', { error: String(e) });
    return json(500, { error: 'Refresh failed', detail: e?.message });
  }
});

router.get('/channels', async (_req: Request, env: Env) => {
  try {
    // Get enabled channels from database for IRC bot
    const result = await env.DB.prepare(`
      SELECT channel_name FROM twitch_bot_users 
      WHERE bot_enabled = 1 
      ORDER BY channel_name
    `).all();
    
    const channels = (result.results || []).map((row: any) => row.channel_name);
    
    return json(200, { 
      channels,
      count: channels.length,
      timestamp: new Date().toISOString()
    });
  } catch (e: any) {
    log('error', 'Failed to get channels list', { error: String(e) });
    return json(500, { error: 'Failed to get channels' });
  }
});

// CORS handling
const allowedOrigins = [
  'https://www.eloward.com',
  'https://eloward.com',
  'https://www.twitch.tv',   // FFZ add-ons run in Twitch context
  'https://twitch.tv',       // FFZ add-ons run in Twitch context
  'http://localhost:3000'
];

function getCorsHeaders(request: Request) {
  const origin = request.headers.get('Origin');
  const allowedOrigin = origin && allowedOrigins.includes(origin) ? origin : allowedOrigins[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}

const worker = {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const corsHeaders = getCorsHeaders(request);
    
    const addCors = (res: Response) => {
      const headers = new Headers(res.headers);
      for (const [key, value] of Object.entries(corsHeaders)) {
        headers.set(key, value);
      }
      return new Response(res.body, {
        status: res.status,
        statusText: res.statusText,
        headers,
      });
    };

    if (request.method === 'OPTIONS') return addCors(json(200, {}));
    const res = await router.handle(request, env, ctx).catch((e: any) => json(500, { error: 'internal', detail: e?.message }));
    return addCors(res);
  },

  // PRODUCTION MAINTENANCE - Scheduled token refresh during low-traffic hours
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    try {
      log('info', 'Scheduled maintenance started', { 
        cron: event.cron,
        scheduledTime: new Date(event.scheduledTime).toISOString()
      });

      // Check current token status
      const currentTokens = await env.BOT_KV.get('bot_tokens', 'json');
      if (!currentTokens?.tokens?.refresh_token) {
        log('warn', 'No refresh token available for maintenance');
        return;
      }

      // Calculate token expiration
      const expiresAt = currentTokens.tokens.expires_at || 0;
      const expiresInHours = Math.floor((expiresAt - Date.now()) / 3600000);
      const shouldRefresh = expiresInHours < 12; // Refresh if expires in next 12 hours

      if (shouldRefresh) {
        log('info', 'Performing proactive token refresh', { expiresInHours });
        
        const refreshed = await refreshBotToken(env, currentTokens.tokens.refresh_token);
        if (refreshed) {
          const updatedTokens = await env.BOT_KV.get('bot_tokens', 'json');
          const newExpiresAt = updatedTokens?.tokens?.expires_at || 0;
          
          log('info', 'Token refresh successful during maintenance', {
            userLogin: refreshed.user?.login,
            newExpiresInHours: Math.floor((newExpiresAt - Date.now()) / 3600000)
          });
        } else {
          log('error', 'Token refresh failed during maintenance');
        }
      }

      // Check enabled channels count
      const channelCount = await getEnabledChannelCount(env);
      log('info', 'Maintenance completed', { enabledChannels: channelCount });
    } catch (e) {
      log('error', 'Scheduled maintenance failed', { error: String(e) });
    }
  }
};

export default worker;