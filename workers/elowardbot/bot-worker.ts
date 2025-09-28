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
  // Production Redis for instant config updates
  UPSTASH_REDIS_REST_URL?: string; // Redis REST API URL for Workers
  UPSTASH_REDIS_REST_TOKEN?: string; // Redis REST API token
  // HMAC security for bot communication
  HMAC_SECRET?: string; // Shared secret for HMAC-signed requests from bot
}

const router = Router();

// OAuth Configuration - Required Helix Scopes per README
const BOT_SCOPES = [
  'chat:read',
  'chat:edit',
  'moderator:manage:banned_users', // Required for timeout/ban actions via /moderation/bans
  'channel:moderate', // Required for mod/broadcaster context  
  // 'moderator:manage:chat_messages' // Optional: for message deletion if needed
];
const BROADCASTER_SCOPES = ['channel:bot'];

// OAuth URLs
const AUTH_URL = 'https://id.twitch.tv/oauth2/authorize';
const TOKEN_URL = 'https://id.twitch.tv/oauth2/token';

// Helix API endpoints
const HELIX_USERS = 'https://api.twitch.tv/helix/users';

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

// OAuth helper functions for token exchange
async function exchangeCodeForToken(env: Env, code: string, redirectUri: string) {
  const body = new URLSearchParams();
  body.set('client_id', env.TWITCH_CLIENT_ID);
  body.set('client_secret', env.TWITCH_CLIENT_SECRET);
  body.set('code', code);
  body.set('grant_type', 'authorization_code');
  body.set('redirect_uri', redirectUri);
  
  const res = await fetch(TOKEN_URL, { method: 'POST', body });
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error_description || 'token exchange failed');
  }
  
  return data as {
    access_token: string;
    refresh_token?: string;
    expires_in: number;
    scope: string[];
  };
}

async function getUserFromToken(env: Env, userAccessToken: string) {
  const res = await fetch(HELIX_USERS, {
    headers: { 
      Authorization: `Bearer ${userAccessToken}`, 
      'Client-Id': env.TWITCH_CLIENT_ID 
    },
  });
  const data = await res.json();
  if (!res.ok) throw new Error('Failed to fetch user info');
  const u = data.data?.[0];
  if (!u) throw new Error('User not found');
  return u as { id: string; login: string; display_name: string };
}

// HMAC request validation for secure bot communication
async function validateHmacRequest(env: Env, request: Request, body: string): Promise<boolean> {
  if (!env.HMAC_SECRET) {
    log('warn', 'HMAC_SECRET not configured - allowing request');
    return true; // Allow if not configured (dev mode)
  }

  const signature = request.headers.get('X-HMAC-Signature');
  const timestamp = request.headers.get('X-Timestamp');
  
  if (!signature || !timestamp) {
    return false;
  }

  // Check timestamp window (±60s)
  const now = Math.floor(Date.now() / 1000);
  const requestTime = parseInt(timestamp);
  if (Math.abs(now - requestTime) > 60) {
    log('warn', 'HMAC request outside time window', { 
      now, 
      requestTime, 
      diff: Math.abs(now - requestTime) 
    });
    return false;
  }

  // Verify HMAC signature using Web Crypto API
  const method = request.method;
  const path = new URL(request.url).pathname;
  const payload = timestamp + method + path + body;
  
  const encoder = new TextEncoder();
  const keyData = encoder.encode(env.HMAC_SECRET);
  const messageData = encoder.encode(payload);
  
  const key = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  
  const signatureBuffer = await crypto.subtle.sign('HMAC', key, messageData);
  const expectedSignature = Array.from(new Uint8Array(signatureBuffer))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');

  return signature === expectedSignature;
}

// Redis pub/sub for instant config propagation (1-3s target)
async function publishConfigUpdate(env: Env, channelLogin: string, fields: any) {
  if (!env.UPSTASH_REDIS_REST_URL || !env.UPSTASH_REDIS_REST_TOKEN) {
    log('debug', 'Redis not configured - no instant updates');
    return;
  }

  const message = {
    type: 'config_update',
    channel_login: channelLogin,
    fields,
    version: Date.now(),
    updated_at: new Date().toISOString()
  };

  try {
    const response = await fetch(`${env.UPSTASH_REDIS_REST_URL}/publish/eloward:config:updates`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(message)
    });

    if (!response.ok) {
      throw new Error(`Redis publish failed: ${response.status}`);
    }

    log('info', 'Config update published to Redis', { 
      channel: channelLogin, 
      fields: Object.keys(fields) 
    });
  } catch (e) {
    log('error', 'Redis publish failed', { 
      channel: channelLogin, 
      error: String(e) 
    });
  }
}

// Simple channel enable function - matches existing codebase patterns
async function enableChannelForUser(env: Env, channel_login: string, twitch_id: string) {
  const login = channel_login.toLowerCase();
  
  // Try UPDATE first (for existing channels)
  const result = await env.DB.prepare(`
    UPDATE twitch_bot_users 
    SET bot_enabled = 1, updated_at = CURRENT_TIMESTAMP
    WHERE twitch_id = ?
  `).bind(twitch_id).run();
  
  // If no rows updated, INSERT new channel record with defaults
  if ((result as any)?.meta?.changes === 0) {
    await env.DB.prepare(`
      INSERT INTO twitch_bot_users (twitch_id, channel_name, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode)
      VALUES (?, ?, 1, 30, "not enough elo to speak. type !eloward", "broadcaster,moderator,vip", "has_rank")
    `).bind(twitch_id, login).run();
  }
  
  log('info', 'Channel enabled via OAuth', { login, twitch_id });
  
  // Publish config update to Redis for instant bot notification (1-3s)
  await publishConfigUpdate(env, login, { bot_enabled: true });
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

// HMAC-secured endpoints for IRC bot communication
router.post('/bot/config:get', async (req: Request, env: Env) => {
  try {
    const body = await req.text();
    
    // Validate HMAC signature
    if (!(await validateHmacRequest(env, req, body))) {
      return json(401, { error: 'Invalid HMAC signature' });
    }
    
    const { channel_login } = JSON.parse(body || '{}');
    if (!channel_login) return json(400, { error: 'channel_login required' });
    
    const config = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template, 
             enforcement_mode, min_rank_tier, min_rank_division, ignore_roles
      FROM twitch_bot_users WHERE channel_name = ?
    `).bind(channel_login.toLowerCase()).first();
    
    if (!config) return json(404, { error: 'Channel not configured' });
    
    return json(200, {
      channel_login: config.channel_login,
      bot_enabled: !!config.bot_enabled,
      timeout_seconds: config.timeout_seconds || 30,
      reason_template: config.reason_template || 'not enough elo to speak. type !eloward',
      enforcement_mode: config.enforcement_mode || 'has_rank',
      min_rank_tier: config.min_rank_tier,
      min_rank_division: config.min_rank_division,
      ignore_roles: config.ignore_roles || 'broadcaster,moderator'
    });
  } catch (e: any) {
    log('error', 'Bot config:get failed', { error: String(e) });
    return json(500, { error: 'Config fetch failed' });
  }
});

router.post('/bot/config:update', async (req: Request, env: Env) => {
  try {
    const body = await req.text();
    
    // Validate HMAC signature
    if (!(await validateHmacRequest(env, req, body))) {
      return json(401, { error: 'Invalid HMAC signature' });
    }
    
    const { channel_login, fields } = JSON.parse(body || '{}');
    if (!channel_login || !fields) {
      return json(400, { error: 'channel_login and fields required' });
    }
    
    // Build dynamic update query
    const updates: string[] = [];
    const values: any[] = [];
    
    if (fields.bot_enabled !== undefined) {
      updates.push('bot_enabled = ?');
      values.push(fields.bot_enabled ? 1 : 0);
    }
    if (fields.timeout_seconds !== undefined) {
      updates.push('timeout_seconds = ?');
      values.push(Number(fields.timeout_seconds) || 30);
    }
    if (fields.reason_template !== undefined) {
      updates.push('reason_template = ?');
      values.push(String(fields.reason_template).slice(0, 500));
    }
    if (fields.enforcement_mode !== undefined) {
      updates.push('enforcement_mode = ?');
      values.push(String(fields.enforcement_mode));
    }
    if (fields.min_rank_tier !== undefined) {
      updates.push('min_rank_tier = ?');
      values.push(fields.min_rank_tier);
    }
    if (fields.min_rank_division !== undefined) {
      updates.push('min_rank_division = ?');
      values.push(fields.min_rank_division);
    }
    
    if (updates.length === 0) {
      return json(400, { error: 'No valid updates provided' });
    }
    
    updates.push('updated_at = CURRENT_TIMESTAMP');
    
    const updateQuery = `UPDATE twitch_bot_users SET ${updates.join(', ')} WHERE channel_name = ?`;
    const result = await env.DB.prepare(updateQuery).bind(...values, channel_login.toLowerCase()).run();
    
    // Verify update succeeded
    if (!result.success || (result.meta?.changes === 0)) {
      log('warn', 'Config update matched no rows', { 
        channel_login, 
        query: updateQuery.substring(0, 100),
        changes: result.meta?.changes 
      });
    }
    
    // Publish to Redis for instant bot notification
    await publishConfigUpdate(env, channel_login.toLowerCase(), fields);
    
    log('info', 'Bot config updated via HMAC', { channel_login, fields: Object.keys(fields) });
    return json(200, { success: true, updated: Object.keys(fields) });
  } catch (e: any) {
    log('error', 'Bot config:update failed', { error: String(e) });
    return json(500, { error: 'Config update failed' });
  }
});

router.post('/rank:get', async (req: Request, env: Env) => {
  try {
    const body = await req.text();
    
    // Validate HMAC signature
    if (!(await validateHmacRequest(env, req, body))) {
      return json(401, { error: 'Invalid HMAC signature' });
    }
    
    const { user_login } = JSON.parse(body || '{}');
    if (!user_login) return json(400, { error: 'user_login required' });
    
    // Check rank via rank worker
    const rankResponse = await env.RANK_WORKER.fetch(
      new Request(`https://internal/api/ranks/lol/${user_login}`)
    );
    
    if (rankResponse.ok) {
      const rankData = await rankResponse.json();
      return json(200, { 
        has_rank: true, 
        rank_data: rankData,
        user_login 
      });
    } else {
      return json(404, { 
        has_rank: false, 
        user_login 
      });
    }
  } catch (e: any) {
    log('error', 'Bot rank:get failed', { error: String(e) });
    return json(500, { error: 'Rank check failed' });
  }
});

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

// Internal authentication for trusted endpoints
function internalAuthOk(env: Env, req: Request): boolean {
  const provided = req.headers.get('X-Internal-Auth') || '';
  const expected = env.BOT_WRITE_KEY || '';
  return Boolean(expected) && provided === expected;
}

// Helper function to normalize roman numerals to numbers
function romanOrNumberToDivision(div: any): number {
  if (typeof div === 'string') {
    const upper = div.toUpperCase();
    if (upper === 'I') return 1;
    if (upper === 'II') return 2;
    if (upper === 'III') return 3;
    if (upper === 'IV') return 4;
  }
  const n = Number(div);
  if (!isFinite(n) || n < 1) return 4; // default worst
  return Math.max(1, Math.min(4, n));
}

// Internal endpoints for dashboard (authenticated with BOT_WRITE_KEY)
router.post('/bot/enable_internal', async (req: Request, env: Env) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    
    if (!twitch_id && !channel_login) {
      return json(400, { error: 'twitch_id or channel_login required' });
    }
    
    // Enable bot for the channel
    let result;
    if (twitch_id) {
      result = await env.DB.prepare(`
        UPDATE twitch_bot_users 
        SET bot_enabled = 1, updated_at = CURRENT_TIMESTAMP
        WHERE twitch_id = ?
      `).bind(twitch_id).run();
    } else {
      result = await env.DB.prepare(`
        UPDATE twitch_bot_users 
        SET bot_enabled = 1, updated_at = CURRENT_TIMESTAMP
        WHERE channel_name = ?
      `).bind(channel_login!.toLowerCase()).run();
    }
    
    // Get the updated config
    const cfg = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template,
             enforcement_mode, min_rank_tier, min_rank_division
      FROM twitch_bot_users 
      WHERE twitch_id = ? OR channel_name = ?
    `).bind(twitch_id || '', channel_login?.toLowerCase() || '').first();
    
    log('info', 'Bot enabled internally', { twitch_id, channel_login });
    
    // Publish config update to Redis for instant bot notification
    const channelName = cfg?.channel_login || channel_login;
    if (channelName) {
      await publishConfigUpdate(env, channelName, { bot_enabled: true });
    }
    
    return json(200, cfg);
  } catch (e: any) {
    log('error', 'Internal enable failed', { error: String(e) });
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/disable_internal', async (req: Request, env: Env) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    
    if (!twitch_id && !channel_login) {
      return json(400, { error: 'twitch_id or channel_login required' });
    }
    
    // Disable bot for the channel
    let result;
    if (twitch_id) {
      result = await env.DB.prepare(`
        UPDATE twitch_bot_users 
        SET bot_enabled = 0, updated_at = CURRENT_TIMESTAMP
        WHERE twitch_id = ?
      `).bind(twitch_id).run();
    } else {
      result = await env.DB.prepare(`
        UPDATE twitch_bot_users 
        SET bot_enabled = 0, updated_at = CURRENT_TIMESTAMP
        WHERE channel_name = ?
      `).bind(channel_login!.toLowerCase()).run();
    }
    
    // Get the updated config
    const cfg = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template,
             enforcement_mode, min_rank_tier, min_rank_division
      FROM twitch_bot_users 
      WHERE twitch_id = ? OR channel_name = ?
    `).bind(twitch_id || '', channel_login?.toLowerCase() || '').first();
    
    log('info', 'Bot disabled internally', { twitch_id, channel_login });
    
    // Publish config update to Redis for instant bot notification
    const channelName = cfg?.channel_login || channel_login;
    if (channelName) {
      await publishConfigUpdate(env, channelName, { bot_enabled: false });
    }
    
    return json(200, cfg);
  } catch (e: any) {
    log('error', 'Internal disable failed', { error: String(e) });
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/config_internal', async (req: Request, env: Env) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    
    if (!twitch_id && !channel_login) {
      return json(400, { error: 'twitch_id or channel_login required' });
    }
    
    // Build update patch
    const patch: any = {};
    if (typeof body.timeout_seconds === 'number') patch.timeout_seconds = body.timeout_seconds;
    if (typeof body.reason_template === 'string') patch.reason_template = body.reason_template;
    if (typeof body.bot_enabled === 'number' || typeof body.bot_enabled === 'boolean') {
      patch.bot_enabled = body.bot_enabled ? 1 : 0;
    }
    if (typeof body.enforcement_mode === 'string') patch.enforcement_mode = String(body.enforcement_mode);
    
    if (body.min_rank_tier !== undefined) {
      patch.min_rank_tier = body.min_rank_tier == null ? null : String(body.min_rank_tier);
    }
    if (body.min_rank_division !== undefined) {
      const d = body.min_rank_division;
      patch.min_rank_division = d == null ? null : (typeof d === 'string' ? romanOrNumberToDivision(d) : Number(d));
    }
    
    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];
    
    if (patch.timeout_seconds !== undefined) {
      updates.push('timeout_seconds = ?');
      values.push(patch.timeout_seconds);
    }
    if (patch.reason_template !== undefined) {
      updates.push('reason_template = ?');
      values.push(patch.reason_template);
    }
    if (patch.bot_enabled !== undefined) {
      updates.push('bot_enabled = ?');
      values.push(patch.bot_enabled);
    }
    if (patch.enforcement_mode !== undefined) {
      updates.push('enforcement_mode = ?');
      values.push(patch.enforcement_mode);
    }
    if (patch.min_rank_tier !== undefined) {
      updates.push('min_rank_tier = ?');
      values.push(patch.min_rank_tier);
    }
    if (patch.min_rank_division !== undefined) {
      updates.push('min_rank_division = ?');
      values.push(patch.min_rank_division);
    }
    
    if (updates.length === 0) {
      return json(400, { error: 'No valid updates provided' });
    }
    
    updates.push('updated_at = CURRENT_TIMESTAMP');
    
    // Update the record
    let updateQuery;
    if (twitch_id) {
      updateQuery = `UPDATE twitch_bot_users SET ${updates.join(', ')} WHERE twitch_id = ?`;
      values.push(twitch_id);
    } else {
      updateQuery = `UPDATE twitch_bot_users SET ${updates.join(', ')} WHERE channel_name = ?`;
      values.push(channel_login!.toLowerCase());
    }
    
    await env.DB.prepare(updateQuery).bind(...values).run();
    
    // Get the updated config
    const cfg = await env.DB.prepare(`
      SELECT channel_name AS channel_login, bot_enabled, timeout_seconds, reason_template,
             enforcement_mode, min_rank_tier, min_rank_division
      FROM twitch_bot_users 
      WHERE twitch_id = ? OR channel_name = ?
    `).bind(twitch_id || '', channel_login?.toLowerCase() || '').first();
    
    log('info', 'Bot config updated internally', { twitch_id, channel_login, updates: Object.keys(patch) });
    
    // Publish config update to Redis for instant bot notification
    const channelName = cfg?.channel_login || channel_login;
    if (channelName) {
      await publishConfigUpdate(env, channelName, patch);
    }
    
    return json(200, cfg);
  } catch (e: any) {
    log('error', 'Internal config update failed', { error: String(e) });
    return json(500, { error: e?.message || 'error' });
  }
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
    
    // Publish config update to Redis for instant bot notification
    await publishConfigUpdate(env, channel_login.toLowerCase(), { bot_enabled: true });
      
      return json(200, { 
      message: 'Channel enabled - IRC bot notified to join immediately',
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
    
    // Publish config update to Redis for instant bot notification
    await publishConfigUpdate(env, channel_login.toLowerCase(), { bot_enabled: false });
      
      return json(200, { 
      message: 'Channel disabled - IRC bot notified to leave immediately',
      channel_login
    });
  } catch (e: any) {
    log('error', 'Failed to disable channel', { error: String(e), channel_login: channel_login || 'unknown' });
    return json(500, { error: 'Failed to disable channel' });
  }
});

// Legacy endpoint - removed in favor of bot-side processing with caching

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
    // Get ALL channels from database for IRC bot (always-on presence model)
    // Bot joins all channels and operates in Standby/Enforcing mode per channel config
    const result = await env.DB.prepare(`
      SELECT channel_name FROM twitch_bot_users 
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

// Legacy endpoint - kept for backward compatibility but webhooks are now primary method
router.post('/channels/reload', async (_req: Request, env: Env) => {
  try {
    // Always-on presence model: return ALL channels, bot manages Standby/Enforcing per channel
    const result = await env.DB.prepare(`
      SELECT channel_name FROM twitch_bot_users 
      ORDER BY channel_name
    `).all();
    
    const channels = (result.results || []).map((row: any) => row.channel_name);
    
    return json(200, { 
      message: 'Channel reload handled via webhooks - this endpoint shows current state',
      channels,
      count: channels.length,
      timestamp: new Date().toISOString()
    });
  } catch (e: any) {
    log('error', 'Channel reload endpoint failed', { error: String(e) });
    return json(500, { error: 'Failed to get channel state' });
  }
});

// Basic health check endpoint
router.get('/health', () => json(200, { status: 'ok', service: 'eloward-bot' }));

// OAuth Authorization Flow
// Start OAuth: actor=bot | broadcaster
router.get('/oauth/start', (req: Request, env: Env) => {
  const url = new URL(req.url);
  const actor = (url.searchParams.get('actor') || 'bot').toLowerCase();
  const state = url.searchParams.get('state') || Math.random().toString(36).substring(2);
  const redirectUri = `${env.WORKER_PUBLIC_URL}/oauth/callback`;
  const scopes = actor === 'broadcaster' ? BROADCASTER_SCOPES : BOT_SCOPES;
  
  const auth = new URL(AUTH_URL);
  auth.searchParams.set('client_id', env.TWITCH_CLIENT_ID);
  auth.searchParams.set('redirect_uri', redirectUri);
  auth.searchParams.set('response_type', 'code');
  auth.searchParams.set('scope', scopes.join(' '));
  auth.searchParams.set('state', `${actor}:${state}`);
  
  // Perform the redirect
  return new Response(null, {
    status: 302,
    headers: { 'Location': auth.toString() }
  });
});

// OAuth callback for both flows
router.get('/oauth/callback', async (req: Request, env: Env) => {
  const url = new URL(req.url);
  const code = url.searchParams.get('code');
  const stateRaw = url.searchParams.get('state') || '';
  const [actor] = stateRaw.split(':');
  
  if (!code || !actor) {
    return json(400, { error: 'missing code/state' });
  }
  
  const redirectUri = `${env.WORKER_PUBLIC_URL}/oauth/callback`;
  
  try {
    const token = await exchangeCodeForToken(env, code, redirectUri);
    const user = await getUserFromToken(env, token.access_token);

    if (actor === 'bot') {
      // Store bot tokens
      const record = {
        user,
        tokens: {
          access_token: token.access_token,
          refresh_token: token.refresh_token || null,
          expires_at: Date.now() + token.expires_in * 1000,
        },
      };
      await env.BOT_KV.put('bot_tokens', JSON.stringify(record));
      
      // Redirect to success page
      return new Response(null, { 
        status: 302, 
        headers: { 
          Location: `${env.WORKER_PUBLIC_URL}/oauth/done?actor=bot&login=${user.login}` 
        } 
      });
      } else {
      // Broadcaster OAuth: enable bot for this channel using production pattern
      try {
        await enableChannelForUser(env, user.login, user.id);
        log('info', 'Broadcaster OAuth completed successfully', { login: user.login });
    } catch (e) {
        log('warn', 'Failed to enable bot for broadcaster', { 
          login: user.login, 
          error: String(e) 
        });
      }

      // Redirect back to dashboard
      const redirect = env.SITE_BASE_URL 
        ? `${env.SITE_BASE_URL}/dashboard?bot=enabled` 
        : `${env.WORKER_PUBLIC_URL}/oauth/done?actor=broadcaster&login=${user.login}`;
      
      return new Response(null, { 
        status: 302, 
        headers: { Location: redirect } 
      });
    }
  } catch (e: any) {
    log('error', 'OAuth callback failed', { error: e?.message || 'unknown' });
    return json(500, { error: e?.message || 'oauth failed' });
  }
});

// Simple landing page after OAuth
router.get('/oauth/done', (req: Request) => {
  const url = new URL(req.url);
  const actor = url.searchParams.get('actor');
  const login = url.searchParams.get('login');
  
  const body = `<!doctype html>
<meta charset="utf-8">
<title>EloWard Bot</title>
<body style="font-family:system-ui;padding:24px;text-align:center;max-width:600px;margin:0 auto">
  <h2>✅ Connected ${actor}</h2>
  <p><strong>${login}</strong> authorized successfully.</p>
  <p>You can close this window and return to the EloWard dashboard.</p>
  <style>body{background:#f8f9fa}h2{color:#28a745}p{color:#6c757d}</style>
</body>`;
  
  return new Response(body, { 
    headers: { 'Content-Type': 'text/html' } 
  });
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
    
    // Handle OAuth redirects without CORS processing
    if (request.url.includes('/oauth/')) {
      return await router.handle(request, env, ctx).catch((e: any) => json(500, { error: 'internal', detail: e?.message }));
    }

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