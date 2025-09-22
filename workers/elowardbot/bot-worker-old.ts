/**
 * EloWard Bot Worker
 * - Handles OAuth for the EloWardBot account (moderation + chat scopes)
 * - Handles broadcaster grant of channel:bot to this app
 * - IRC-only ingestion: Connects to Twitch IRC and enforces via /timeout in chat
 * - Processes chat messages, checks EloWard rank DB, and timeouts if missing
 */

import { Router } from 'itty-router';

// Minimal Cloudflare Workers types to satisfy TypeScript without external deps
type KVNamespace = {
  get: (key: string, type?: 'text' | 'json' | 'arrayBuffer' | 'stream') => Promise<any>;
  put: (key: string, value: string, options?: Record<string, any>) => Promise<void>;
};
type ExecutionContext = { waitUntil(promise: Promise<any>): void; passThroughOnException(): void };

interface Fetcher { fetch: (request: Request) => Promise<Response> }

interface DurableObjectState { storage: { get: (key: string) => Promise<any>; put: (key: string, value: any) => Promise<void>; delete: (key: string) => Promise<void> }; alarm?: number; setAlarm?(when: number): void }
interface DurableObjectNamespace { idFromName(name: string): any; get(id: any): { fetch: (input: RequestInfo, init?: RequestInit) => Promise<Response> } }
type D1Database = { prepare: (query: string) => { bind: (...values: any[]) => any; first?: (column?: string) => Promise<any>; all: () => Promise<{ results?: any[] }>; run: () => Promise<any> } }

interface Env {
  TWITCH_CLIENT_ID: string;
  TWITCH_CLIENT_SECRET: string;
  // Public base URL of this Worker (no trailing slash), e.g. https://eloward-bot.your-account.workers.dev
  WORKER_PUBLIC_URL: string;
  // (Optional) Legacy EventSub secret — no longer used in IRC-only mode
  EVENTSUB_SECRET: string;
  // Optional: KV for tokens and config
  BOT_KV: KVNamespace;
  // Service binding to ranks worker
  RANK_WORKER: Fetcher;
  // Durable Objects
  BOT_MANAGER: DurableObjectNamespace;
  // Removed cooldown shard; no per-user cooldown
  // IRC client shards (Durable Object namespace for IRC connections)
  IRC_CLIENT?: DurableObjectNamespace;
  // D1 database
  DB: D1Database;
  // Optional site base url for reasons
  SITE_BASE_URL?: string;
  // Bot write key for trusted calls from website backend
  BOT_WRITE_KEY: string;
}

const router = Router();

// Scopes for IRC: chat read/write + moderation (IRC commands deprecated Feb 2023)
const BOT_SCOPES = [
  'chat:read',
  'chat:edit',
  'moderator:manage:banned_users', // Required for timeout/ban via Helix API (replaces deprecated IRC commands)
];
const BROADCASTER_SCOPES = ['channel:bot'];

// OAuth URLs
const AUTH_URL = 'https://id.twitch.tv/oauth2/authorize';
const TOKEN_URL = 'https://id.twitch.tv/oauth2/token';

// Helix endpoints (users only; moderation/EventSub removed in IRC-only mode)
const HELIX_USERS = 'https://api.twitch.tv/helix/users';

// Helpers
function json(status: number, data: unknown): Response {
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

function timestamp(): string {
  return new Date().toISOString();
}

function log(message: string, data?: any) {
  if (data) {
    console.log(`[${timestamp()}] ${message}`, data);
  } else {
    console.log(`[${timestamp()}] ${message}`);
  }
}

function toQuery(params: Record<string, string | number | boolean>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) sp.set(k, String(v));
  return sp.toString();
}

async function exchangeCodeForToken(env: Env, code: string, redirectUri: string) {
  const body = new URLSearchParams();
  body.set('client_id', env.TWITCH_CLIENT_ID);
  body.set('client_secret', env.TWITCH_CLIENT_SECRET);
  body.set('code', code);
  body.set('grant_type', 'authorization_code');
  body.set('redirect_uri', redirectUri);
  const res = await fetch(TOKEN_URL, { method: 'POST', body });
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(data.error_description || 'token exchange failed');
  return data as {
    access_token: string;
    refresh_token?: string;
    expires_in: number;
    scope?: string[];
    token_type: string;
  };
}

async function refreshUserToken(env: Env, refreshToken: string) {
  const body = new URLSearchParams();
  body.set('client_id', env.TWITCH_CLIENT_ID);
  body.set('client_secret', env.TWITCH_CLIENT_SECRET);
  body.set('grant_type', 'refresh_token');
  body.set('refresh_token', refreshToken);
  const res = await fetch(TOKEN_URL, { method: 'POST', body });
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(data.error_description || 'token refresh failed');
  return data as { access_token: string; refresh_token?: string; expires_in: number };
}

async function getAppAccessToken(env: Env) {
  const cacheKey = 'app_access_token';
  try {
    const cached = await env.BOT_KV.get(cacheKey, 'json');
    if (cached && cached.access_token && cached.expires_at && Date.now() < cached.expires_at - 60_000) {
      return cached.access_token as string;
    }
  } catch {}

  const body = new URLSearchParams();
  body.set('client_id', env.TWITCH_CLIENT_ID);
  body.set('client_secret', env.TWITCH_CLIENT_SECRET);
  body.set('grant_type', 'client_credentials');
  const res = await fetch(TOKEN_URL, { method: 'POST', body });
  const data = await res.json();
  if (!res.ok || data.error) throw new Error('app access token failed');
  const access_token = data.access_token as string;
  const expires_in = data.expires_in as number;
  const expires_at = Date.now() + expires_in * 1000;
  try { await env.BOT_KV.put(cacheKey, JSON.stringify({ access_token, expires_at }), { expirationTtl: expires_in }); } catch {}
  return access_token;
}

async function getUserByLogin(env: Env, accessToken: string, login: string) {
  const res = await fetch(`${HELIX_USERS}?${toQuery({ login })}`, {
    headers: { Authorization: `Bearer ${accessToken}`, 'Client-Id': env.TWITCH_CLIENT_ID },
  });
  const data = await res.json();
  if (!res.ok) throw new Error('get user failed');
  const u = data.data?.[0];
  if (!u) throw new Error('user not found');
  return u as { id: string; login: string; display_name: string };
}

async function getUserFromToken(env: Env, userAccessToken: string) {
  const res = await fetch(HELIX_USERS, {
    headers: { Authorization: `Bearer ${userAccessToken}`, 'Client-Id': env.TWITCH_CLIENT_ID },
  });
  const data = await res.json();
  if (!res.ok) throw new Error('users failed');
  const u = data.data?.[0];
  if (!u) throw new Error('no user');
  return u as { id: string; login: string; display_name: string };
}

// Validates any Twitch user token without requiring matching Client-Id.
// Returns minimal identity: user_id and login.
// EventSub signature verification removed (IRC-only mode)

// CORS preflight
router.options('*', () => json(200, {}));

router.get('/health', () => json(200, { status: 'ok', service: 'eloward-bot' }));

// Start / reload IRC via Durable Objects
router.post('/irc/start', async (_req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    console.log('[Bot] /irc/start called');
    const id = env.BOT_MANAGER.idFromName('manager');
    ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/start', { method: 'POST' }));
    return json(202, { accepted: true });
  } catch (e: any) {
    return json(500, { error: e?.message || 'start failed' });
  }
});

router.post('/irc/reload', async (_req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    log('[Bot] /irc/reload called');
    const id = env.BOT_MANAGER.idFromName('manager');
    ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' }));
    return json(202, { accepted: true });
  } catch (e: any) {
    return json(500, { error: e?.message || 'reload failed' });
  }
});

// WebSocket endpoint for hibernatable IRC connections
router.get('/irc/connect', async (req: Request, env: Env) => {
  log('[Bot] /irc/connect WebSocket upgrade requested');
  
  if (!env.IRC_CLIENT) {
    return json(500, { error: 'IRC_CLIENT not configured' });
  }

  // Check if this is a WebSocket upgrade request
  const upgradeHeader = req.headers.get('Upgrade');
  if (upgradeHeader !== 'websocket') {
    return json(400, { error: 'Expected WebSocket upgrade' });
  }

  // Create WebSocket pair for hibernatable connection
  const [client, server] = new (globalThis as any).WebSocketPair();

  // Get the IRC client DO
  const shardId = new URL(req.url).searchParams.get('shard') || '0';
  const id = env.IRC_CLIENT.idFromName(`irc:${shardId}`);
  const stub = env.IRC_CLIENT.get(id);

  // Pass the server WebSocket to the DO for hibernatable handling
  const response = await stub.fetch('https://do/websocket', {
    method: 'POST',
    headers: {
      'Upgrade': 'websocket',
      'Connection': 'Upgrade',
    },
    // @ts-ignore - WebSocket as body is valid for hibernatable WebSockets
    body: server,
  });

  if (!response.ok) {
    return json(500, { error: 'Failed to establish WebSocket with DO' });
  }

  // Return the client WebSocket to establish hibernatable connection
  return new Response(null, {
    status: 101,
    // @ts-ignore - WebSocket response is valid for hibernatable connections
    webSocket: client,
  });
});

// Quick shard state probe: /irc/state?shard=0
router.get('/irc/state', async (req: Request, env: Env) => {
  try {
    const u = new URL(req.url);
    const shard = Number(u.searchParams.get('shard') || '0') || 0;
    if (!env.IRC_CLIENT) return json(200, { ok: false, note: 'IRC_CLIENT binding not configured' });
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/state');
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'state failed' });
  }
});

router.get('/irc/metrics', async (req: Request, env: Env) => {
  try {
    const u = new URL(req.url);
    const shard = Number(u.searchParams.get('shard') || '0') || 0;
    if (!env.IRC_CLIENT) return json(200, { ok: false, note: 'IRC_CLIENT binding not configured' });
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/metrics');
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'metrics failed' });
  }
});

router.get('/irc/frames', async (req: Request, env: Env) => {
  try {
    const u = new URL(req.url);
    const shard = Number(u.searchParams.get('shard') || '0') || 0;
    const limit = Number(u.searchParams.get('limit') || '100') || 100;
    if (!env.IRC_CLIENT) return json(200, { ok: false, note: 'IRC_CLIENT binding not configured' });
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch(`https://do/frames?limit=${encodeURIComponent(String(limit))}`);
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'frames failed' });
  }
});

// Debug helper: ask shard to send a test message into a channel
router.post('/irc/debug/say', async (req: Request, env: Env) => {
  try {
    if (!env.IRC_CLIENT) return json(400, { error: 'IRC_CLIENT not configured' });
    const body = await req.json().catch(() => ({} as any));
    const channel_login = String(body?.channel_login || 'yomata1').toLowerCase();
    const message = String(body?.message || 'eloward debug: hello');
    const shard = Number(body?.shard || 0) || 0;
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/debug/say', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_login, message })
    });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'debug failed' });
  }
});

// Debug helper: request NAMES list from IRC for a channel
router.post('/irc/debug/names', async (req: Request, env: Env) => {
  try {
    if (!env.IRC_CLIENT) return json(400, { error: 'IRC_CLIENT not configured' });
    const body = await req.json().catch(() => ({} as any));
    const channel_login = String(body?.channel_login || 'yomata1').toLowerCase();
    const shard = Number(body?.shard || 0) || 0;
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/debug/names', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_login })
    });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'debug names failed' });
  }
});

// Debug helper: send a PING via live socket (for connection testing)
router.post('/irc/debug/ping', async (_req: Request, env: Env) => {
  try {
    if (!env.IRC_CLIENT) return json(400, { error: 'IRC_CLIENT not configured' });
    const id = env.IRC_CLIENT.idFromName(`irc:0`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/debug/ping', { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'debug ping failed' });
  }
});

// Debug helper: inject a synthetic PRIVMSG into the parser (without Twitch delivery)
router.post('/irc/debug/privmsg', async (req: Request, env: Env) => {
  try {
    if (!env.IRC_CLIENT) return json(400, { error: 'IRC_CLIENT not configured' });
    const body = await req.json().catch(() => ({} as any));
    const shard = Number(body?.shard || 0) || 0;
    const id = env.IRC_CLIENT.idFromName(`irc:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/debug/privmsg', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_login: body?.channel_login, user: body?.user, text: body?.text })
    });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'debug privmsg failed' });
  }
});

// -------- Dashboard Config APIs (HMAC protected) --------

async function hmacValid(env: Env, bodyText: string, signatureHeader: string | null): Promise<boolean> {
  const secret = (env as any).DASHBOARD_HMAC_SECRET as string | undefined;
  if (!secret) return true; // allow if not configured (dev)
  if (!signatureHeader) return false;
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sigBuf = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(bodyText));
  const hex = [...new Uint8Array(sigBuf)].map(b => b.toString(16).padStart(2, '0')).join('');
  return signatureHeader === `sha256=${hex}`;
}

async function getChannelConfig(env: Env, channel_login: string) {
  const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode, min_rank_tier, min_rank_division
             FROM twitch_bot_users WHERE channel_name = ?`;
  try {
    const stmt: any = env.DB.prepare(q).bind(channel_login.toLowerCase());
    if (typeof stmt.first === 'function') {
      const row = await stmt.first();
      return row || null;
    } else {
      const res = await stmt.all();
      return (res?.results && res.results[0]) || null;
    }
  } catch {
    return null;
  }
}

async function upsertChannelConfig(env: Env, channel_login: string, patch: any) {
  const login = channel_login.toLowerCase();
  const v = (x: any) => (x === undefined ? null : x);
  const to01 = (x: any) => (x === undefined || x === null ? null : (x === true || x === 1 ? 1 : x === false || x === 0 ? 0 : Number(x)));
  // Ensure twitch_id present
  let twitchId = v(patch.twitch_id) as string | null;
  if (!twitchId) {
    try {
      const app = await getAppAccessToken(env);
      const u = await getUserByLogin(env, app, login);
      twitchId = u.id;
    } catch {}
  }
  if (!twitchId) throw new Error('unable to resolve twitch_id');

  // Upsert by primary key twitch_id
  const upd = `UPDATE twitch_bot_users SET
    channel_name = COALESCE(?, channel_name),
    bot_enabled = COALESCE(?, bot_enabled),
    timeout_seconds = COALESCE(?, timeout_seconds),
    reason_template = COALESCE(?, reason_template),
    ignore_roles = COALESCE(?, ignore_roles),
    enforcement_mode = COALESCE(?, enforcement_mode),
    min_rank_tier = COALESCE(?, min_rank_tier),
    min_rank_division = COALESCE(?, min_rank_division),
    updated_at = CURRENT_TIMESTAMP
    WHERE twitch_id = ?`;
  const result = await env.DB.prepare(upd).bind(
    login,
    to01(patch.bot_enabled),
    v(patch.timeout_seconds),
    v(patch.reason_template),
    v(patch.ignore_roles),
    v(patch.enforcement_mode),
    v(patch.min_rank_tier),
    v(patch.min_rank_division),
    twitchId
  ).run();
  if ((result as any)?.meta?.changes === 0) {
    const ins = `INSERT INTO twitch_bot_users (twitch_id, channel_name, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode, min_rank_tier, min_rank_division)
                 VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 30), COALESCE(?, "{seconds}s timeout: not enough elo to speak. Link your EloWard at {site}"), COALESCE(?, "broadcaster,moderator,vip"), COALESCE(?, 'has_rank'), COALESCE(?, NULL), COALESCE(?, NULL))`;
    await env.DB.prepare(ins).bind(
      twitchId,
      login,
      to01(patch.bot_enabled),
      v(patch.timeout_seconds),
      v(patch.reason_template),
      v(patch.ignore_roles),
      v(patch.enforcement_mode),
      v(patch.min_rank_tier),
      v(patch.min_rank_division)
    ).run();
  }
  return await getChannelConfig(env, login);
}

router.post('/bot/config_id', async (req: Request, env: Env) => {
  try {
    const body = await req.json().catch(() => ({} as any));
    const twitch_id = String(body?.twitch_id || '');
    if (!twitch_id) return json(400, { error: 'twitch_id required' });
    const cfg = await getChannelConfigByTwitchId(env, twitch_id);
    if (!cfg) return json(404, { error: 'not found' });
    // Omit twitch_id from response to avoid exposing it publicly
    const { twitch_id: _omit, ...safe } = cfg as any;
    return json(200, safe);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

// Dashboard initialization: combine channel_active (users worker can provide separately) with bot config
// For now, this endpoint returns only bot config and a derived `bot_active` boolean;
// callers can merge with Users service data for channel_active.
router.post('/dashboard/init', async (req: Request, env: Env) => {
  try {
    const body = await req.json().catch(() => ({} as any));
    const twitch_id = String(body?.twitch_id || '');
    if (!twitch_id) return json(400, { error: 'twitch_id required' });
    const cfg = await getChannelConfigByTwitchId(env, twitch_id);
    const bot = cfg ? ({
      channel_login: (cfg as any).channel_login,
      bot_enabled: !!(cfg as any).bot_enabled,
      timeout_seconds: (cfg as any).timeout_seconds,
      reason_template: (cfg as any).reason_template,
      ignore_roles: (cfg as any).ignore_roles,
      enforcement_mode: (cfg as any).enforcement_mode || 'has_rank',
      min_rank_tier: (cfg as any).min_rank_tier || null,
      min_rank_division: (cfg as any).min_rank_division ?? null
    }) : null;
    return json(200, { bot_active: !!(cfg && (cfg as any).bot_enabled), bot_config: bot });
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

// Removed legacy HMAC write endpoints. All DB writes now require X-Internal-Auth via *_internal routes or occur during OAuth callback.

// Public endpoints authenticated by Twitch user token (frontend-friendly)
// Removed legacy twitch_token endpoints in favor of internal write-key paths and OAuth auto-enable.

// -------- Internal enable/disable (trusted path via server-side secret) --------

function internalAuthOk(env: Env, req: Request): boolean {
  const provided = req.headers.get('X-Internal-Auth') || '';
  const expected = (env as any).BOT_WRITE_KEY || '';
  return Boolean(expected) && provided === expected;
}

async function resolveLoginFromInput(env: Env, twitch_id?: string, channel_login?: string): Promise<{ login: string; id?: string } | null> {
  if (channel_login) return { login: channel_login.toLowerCase() };
  if (twitch_id) {
    try {
      const app = await getAppAccessToken(env);
      const u = await getUserById(env, app, twitch_id);
      return { login: u.login.toLowerCase(), id: u.id };
    } catch {
      return null;
    }
  }
  return null;
}

router.post('/bot/enable_internal', async (req: Request, env: Env, ctx: ExecutionContext) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    const resolved = await resolveLoginFromInput(env, twitch_id, channel_login);
    if (!resolved?.login) return json(400, { error: 'missing twitch_id or channel_login' });
    const cfg = await upsertChannelConfig(env, resolved.login, { bot_enabled: 1, twitch_id });
    try {
      const id = env.BOT_MANAGER.idFromName('manager');
      ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' }));
    } catch {}
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/disable_internal', async (req: Request, env: Env, ctx: ExecutionContext) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    const resolved = await resolveLoginFromInput(env, twitch_id, channel_login);
    if (!resolved?.login) return json(400, { error: 'missing twitch_id or channel_login' });
    const cfg = await upsertChannelConfig(env, resolved.login, { bot_enabled: 0, twitch_id });
    try {
      const id = env.BOT_MANAGER.idFromName('manager');
      ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' }));
    } catch {}
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/config_internal', async (req: Request, env: Env, ctx: ExecutionContext) => {
  if (!internalAuthOk(env, req)) return json(401, { error: 'unauthorized' });
  try {
    const body = await req.json().catch(() => ({}));
    const twitch_id = body?.twitch_id as string | undefined;
    const channel_login = body?.channel_login as string | undefined;
    const resolved = await resolveLoginFromInput(env, twitch_id, channel_login);
    if (!resolved?.login) return json(400, { error: 'missing twitch_id or channel_login' });
    const patch: any = {};
    if (typeof body.timeout_seconds === 'number') patch.timeout_seconds = body.timeout_seconds;
    if (typeof body.reason_template === 'string') patch.reason_template = body.reason_template;
    // cooldown removed
    if (typeof body.bot_enabled === 'number' || typeof body.bot_enabled === 'boolean') patch.bot_enabled = body.bot_enabled;
    if (typeof body.enforcement_mode === 'string') patch.enforcement_mode = String(body.enforcement_mode);
    if (body.min_rank_tier !== undefined) patch.min_rank_tier = body.min_rank_tier == null ? null : String(body.min_rank_tier);
    if (body.min_rank_division !== undefined) {
      const d = body.min_rank_division;
      patch.min_rank_division = d == null ? null : (typeof d === 'string' ? romanOrNumberToDivision(d) : Number(d));
    }
    if (twitch_id) patch.twitch_id = twitch_id;
    const cfg = await upsertChannelConfig(env, resolved.login, patch);
    try {
      const id = env.BOT_MANAGER.idFromName('manager');
      ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' }));
    } catch {}
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

// Start OAuth: actor=bot | broadcaster
router.get('/oauth/start', (req: Request, env: Env) => {
  const url = new URL(req.url);
  const actor = (url.searchParams.get('actor') || 'bot').toLowerCase();
  const state = url.searchParams.get('state') || crypto.randomUUID();
  const redirectUri = `${env.WORKER_PUBLIC_URL}/oauth/callback`; // single callback handler
  const scopes = actor === 'broadcaster' ? BROADCASTER_SCOPES : BOT_SCOPES;
  const auth = new URL(AUTH_URL);
  auth.searchParams.set('client_id', env.TWITCH_CLIENT_ID);
  auth.searchParams.set('redirect_uri', redirectUri);
  auth.searchParams.set('response_type', 'code');
  auth.searchParams.set('scope', scopes.join(' '));
  auth.searchParams.set('state', `${actor}:${state}`);
  return Response.redirect(auth.toString(), 302);
});

// OAuth callback for both flows
router.get('/oauth/callback', async (req: Request, env: Env) => {
  const url = new URL(req.url);
  const code = url.searchParams.get('code');
  const stateRaw = url.searchParams.get('state') || '';
  const [actor] = stateRaw.split(':');
  if (!code || !actor) return json(400, { error: 'missing code/state' });
  const redirectUri = `${env.WORKER_PUBLIC_URL}/oauth/callback`;
  try {
    const token = await exchangeCodeForToken(env, code, redirectUri);
    const user = await getUserFromToken(env, token.access_token);

    if (actor === 'bot') {
      const record = {
        user,
        tokens: {
          access_token: token.access_token,
          refresh_token: token.refresh_token || null,
          expires_at: Date.now() + token.expires_in * 1000,
        },
      };
      await env.BOT_KV.put('bot_tokens', JSON.stringify(record));
      // Optional: redirect to your dashboard success page
      return new Response(null, { status: 302, headers: { Location: `${env.WORKER_PUBLIC_URL}/oauth/done?actor=bot&login=${user.login}` } });
    } else {
      // Broadcaster OAuth: auto-enable bot and warm shards (IRC-only)
      try {
        await upsertChannelConfig(env, String(user.login).toLowerCase(), { bot_enabled: 1, twitch_id: user.id });
      } catch {}
      try {
        const id = env.BOT_MANAGER.idFromName('manager');
        await env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' });
      } catch {}

      // Redirect back to dashboard if available, else show done page
      const redirect = env.SITE_BASE_URL ? `${env.SITE_BASE_URL}/dashboard?bot=enabled` : `${env.WORKER_PUBLIC_URL}/oauth/done?actor=broadcaster&login=${user.login}`;
      return new Response(null, { status: 302, headers: { Location: redirect } });
    }
  } catch (e: any) {
    return json(500, { error: e?.message || 'oauth failed' });
  }
});

// Simple landing after OAuth
router.get('/oauth/done', (req: Request) => {
  const u = new URL(req.url);
  const actor = u.searchParams.get('actor');
  const login = u.searchParams.get('login');
  const body = `<!doctype html><meta charset="utf-8"><title>EloWard Bot</title><body style="font-family:system-ui;padding:24px"><h2>Connected ${actor}</h2><p>${login} authorized successfully.</p><p>You can close this window.</p></body>`;
  return new Response(body, { headers: { 'Content-Type': 'text/html' } });
});

// EventSub endpoints removed (IRC-only mode)

const worker = {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const addCors = (res: Response) => {
      try {
        const headers = new Headers(res.headers);
        headers.set('Access-Control-Allow-Origin', '*');
        headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        headers.set('Access-Control-Allow-Headers', 'Content-Type, X-Signature');
        return new Response(res.body, { status: res.status, headers });
      } catch {
        return res;
      }
    };

    if (request.method === 'OPTIONS') return addCors(json(200, {}));
    const res = await router.handle(request, env, ctx).catch((e: any) => json(500, { error: 'internal', detail: e?.message }));
    return addCors(res);
  },
};

export default worker;

// ---------------- Durable Objects ----------------

type ChannelConfig = {
  channel_login: string;
  twitch_id?: string;
  bot_enabled: number | boolean;
  timeout_seconds: number;
  reason_template: string;
  ignore_roles: string;
  cooldown_seconds: number;
  enforcement_mode?: 'has_rank' | 'min_rank';
  min_rank_tier?: string | null;
  min_rank_division?: number | null;
};

async function listEnabledChannels(env: Env): Promise<ChannelConfig[]> {
  const startTime = Date.now();
  log('[listEnabledChannels] START');
  
  try {
    // PRODUCTION: Load all enabled channels (removed test filter)
    const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode, min_rank_tier, min_rank_division
               FROM twitch_bot_users
               WHERE bot_enabled = 1`;
    
    const dbStartTime = Date.now();
    const res = await env.DB.prepare(q).all();
    const dbDuration = Date.now() - dbStartTime;
    
    const rows = res?.results || [];
    log('[listEnabledChannels] END - Success', { 
      duration: Date.now() - startTime,
      dbDuration,
      channelCount: rows.length,
      channels: rows.map((r: any) => r.channel_login).slice(0, 5) // First 5 channels for debugging
    });
    
    return rows as ChannelConfig[];
  } catch (e) {
    log('[listEnabledChannels] END - Error', { duration: Date.now() - startTime, error: String(e) });
    return [];
  }
}

async function getUserById(env: Env, accessToken: string, id: string) {
  const res = await fetch(`${HELIX_USERS}?${toQuery({ id })}`, {
    headers: { Authorization: `Bearer ${accessToken}`, 'Client-Id': env.TWITCH_CLIENT_ID },
  });
  const data = await res.json();
  if (!res.ok) throw new Error('get user by id failed');
  const u = data.data?.[0];
  if (!u) throw new Error('user not found');
  return u as { id: string; login: string; display_name: string };
}

async function getChannelConfigByTwitchId(env: Env, twitchId: string) {
  try {
    const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode, min_rank_tier, min_rank_division
               FROM twitch_bot_users WHERE twitch_id = ?`;
    const res = await env.DB.prepare(q).bind(twitchId).all();
    const row = res?.results && res.results[0];
    if (row) return row;
    // Fallback by resolving login then querying by login
    const app = await getAppAccessToken(env);
    const user = await getUserById(env, app, twitchId);
    if (!user?.login) return null;
    return await getChannelConfig(env, String(user.login).toLowerCase());
  } catch {
    return null;
  }
}

function resolveReasonTemplate(env: Env, channelConfig: any, chatterLogin: string): string {
  const site = env.SITE_BASE_URL || 'https://www.eloward.com';
  const seconds = channelConfig?.timeout_seconds || 30;
  const tpl = channelConfig?.reason_template || '{seconds}s timeout: not enough elo to speak. Link your EloWard at {site}';
  return String(tpl)
    .replace('{seconds}', String(seconds))
    .replace('{site}', site)
    .replace('{user}', chatterLogin);
}

// Rank comparison helpers
const RANK_ORDER = [
  'IRON','BRONZE','SILVER','GOLD','PLATINUM','EMERALD','DIAMOND','MASTER','GRANDMASTER','CHALLENGER'
] as const;
function normalizeTier(t: any): string {
  return String(t || '').toUpperCase();
}
function divisionToNumber(div: any): number {
  const n = Number(div);
  if (!isFinite(n) || n < 1) return 4; // default worst
  return Math.max(1, Math.min(4, n));
}
function meetsMinRank(userRank: any, threshold: { tier: string; division: number }): boolean {
  if (!userRank || !userRank.rank_tier) return false;
  const userTier = normalizeTier(userRank.rank_tier);
  const userDiv = divisionToNumber(userRank.rank_division);
  const thrTier = normalizeTier(threshold.tier);
  const thrDiv = (thrTier === 'MASTER' || thrTier === 'GRANDMASTER' || thrTier === 'CHALLENGER')
    ? 1
    : romanOrNumberToDivision(threshold.division);
  const userIdx = RANK_ORDER.indexOf(userTier as any);
  const thrIdx = RANK_ORDER.indexOf(thrTier as any);
  if (userIdx < 0 || thrIdx < 0) return false;
  if (userIdx > thrIdx) return true; // strictly higher tier
  if (userIdx < thrIdx) return false; // strictly lower tier
  // same tier: lower division number is higher (I=1 > IV=4)
  return userDiv <= thrDiv;
}

function romanOrNumberToDivision(div: any): number {
  if (typeof div === 'string') {
    const upper = div.toUpperCase();
    if (upper === 'I') return 1;
    if (upper === 'II') return 2;
    if (upper === 'III') return 3;
    if (upper === 'IV') return 4;
  }
  return divisionToNumber(div);
}

async function getBotUserAndToken(env: Env): Promise<{ access: string; refresh?: string; expires_at?: number; user?: { id: string; login: string } } | null> {
  const startTime = Date.now();
  log('[getBotUserAndToken] START');
  
  try {
    const kvStartTime = Date.now();
    const raw = (await env.BOT_KV.get('bot_tokens', 'json')) as any;
    log('[getBotUserAndToken] KV fetch completed', { duration: Date.now() - kvStartTime });
    
    if (!raw?.tokens?.access_token) {
      log('[getBotUserAndToken] END - No access token found', { duration: Date.now() - startTime });
      return null;
    }
    let access = raw.tokens.access_token as string;
    let refresh = raw.tokens.refresh_token as string | undefined;
    let exp = raw.tokens.expires_at as number | undefined;
    
    log('[getBotUserAndToken] Token info', { 
      hasAccess: !!access, 
      hasRefresh: !!refresh, 
      expiresAt: exp, 
      expiresInMinutes: exp ? Math.round((exp - Date.now()) / 60000) : null,
      needsRefresh: refresh && exp && Date.now() > exp - 60_000
    });
    
    // Only refresh if token expires soon and we have refresh token
    if (refresh && exp && Date.now() > exp - 60_000) {
      log('[getBotUserAndToken] Token needs refresh - starting refresh process');
      try {
        // Add timeout to token refresh to prevent hanging
        const refreshPromise = refreshUserToken(env, refresh);
        const timeoutPromise = new Promise<any>((_, reject) => 
          setTimeout(() => reject(new Error('Token refresh timeout')), 5000)
        );
        
        const nt = await Promise.race([refreshPromise, timeoutPromise]);
        access = nt.access_token;
        refresh = nt.refresh_token || refresh;
        exp = Date.now() + nt.expires_in * 1000;
        
        // Non-blocking token storage update
        env.BOT_KV.put('bot_tokens', JSON.stringify({ 
          user: raw.user, 
          tokens: { access_token: access, refresh_token: refresh, expires_at: exp } 
        })).catch(e => console.error('[Token] KV put failed:', e));
        
        log('[getBotUserAndToken] Token refresh successful', { newExpiresInMinutes: Math.round((exp - Date.now()) / 60000) });
      } catch (e) {
        log('[getBotUserAndToken] Token refresh failed - using existing token', { error: String(e) });
        // Use existing token even if refresh fails
      }
    }
    
    const result = { access, refresh, expires_at: exp, user: raw.user };
    log('[getBotUserAndToken] END - Success', { 
      duration: Date.now() - startTime,
      hasUser: !!result.user,
      userLogin: result.user?.login,
      tokenLength: access?.length
    });
    return result;
  } catch (e) {
    log('[getBotUserAndToken] END - Error', { duration: Date.now() - startTime, error: String(e) });
    return null;
  }
}

// IRC client is implemented via Durable Object IrcClientShard; no EventSub ingestion.

class BotManager {
  state: DurableObjectState;
  env: Env;
  constructor(state: DurableObjectState, env: Env) {
    log('[BotManager] constructor start');
    this.state = state;
    this.env = env;
    // keep warm more aggressively
    if (typeof this.state.setAlarm === 'function') {
      const next = Date.now() + 60_000 * 2; // every 2 minutes for better performance  
      try { this.state.setAlarm!(next); } catch {}
    }
    log('[BotManager] constructor end');
  }

  async fetch(req: Request): Promise<Response> {
    const startTime = Date.now();
    console.log('[BotManager] fetch start:', startTime);
    const url = new URL(req.url);
    if (req.method === 'POST' && (url.pathname === '/start' || url.pathname === '/reload')) {
      const dbStartTime = Date.now();
      console.log('[BotManager] fetching enabled channels...', dbStartTime);
      
      // Use Promise.race with timeout to prevent hanging on slow DB queries
      const channelsPromise = listEnabledChannels(this.env);
      const timeoutPromise = new Promise<ChannelConfig[]>((_, reject) => 
        setTimeout(() => reject(new Error('DB query timeout')), 5000)
      );
      
      let channels: ChannelConfig[];
      try {
        channels = await Promise.race([channelsPromise, timeoutPromise]);
        const dbEndTime = Date.now();
        console.log('[BotManager] loaded channels', { count: channels.length, dbTime: dbEndTime - dbStartTime });
        
        // Database already filtered to yomata1 only for testing
      } catch (e) {
        console.error('[BotManager] channel loading failed/timeout:', e);
        channels = []; // fail gracefully, allow restart to try again
      }
      
      // Start IRC client shards (IRC-only mode)
      if (!this.env.IRC_CLIENT) {
        console.warn('[BotManager] IRC_CLIENT binding not configured. Channels:', channels.length);
        return json(200, { ok: true, channels: channels.length, note: 'IRC_CLIENT binding not configured.' });
      }

      const shardSize = 50; // target channels per shard
      const total = channels.length;
      const shardCount = Math.max(1, Math.min(10, Math.ceil(total / shardSize)));

      // Build assignments in parallel with logging
      console.log('[BotManager] building assignments...', { total, shardCount });
      const assignments: Array<{ shard: number; channels: any[] }> = Array.from({ length: shardCount }, (_, i) => ({ shard: i, channels: [] }));
      channels.forEach((ch, idx) => {
        const shard = idx % shardCount;
        assignments[shard].channels.push(ch);
      });

      // Dispatch assignments to shards with timeout protection
      const dispatchStartTime = Date.now();
      console.log('[BotManager] dispatching to shards...', dispatchStartTime);
      const dispatchPromises = assignments.map(async (a) => {
        const shardStartTime = Date.now();
        const id = this.env.IRC_CLIENT!.idFromName(`irc:${a.shard}`);
        console.log('[BotManager] dispatch /assign', { shard: a.shard, count: a.channels.length, time: shardStartTime });
        
        try {
          // First assign channels to the shard
          const assignPromise = this.env.IRC_CLIENT!.get(id).fetch('https://do/assign', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ channels: a.channels }),
          });
          
          const timeoutPromise = new Promise<Response>((_, reject) => 
            setTimeout(() => reject(new Error(`Shard ${a.shard} assign timeout`)), 10000)
          );
          
          await Promise.race([assignPromise, timeoutPromise]);
          
          // Then establish hibernatable WebSocket connection
          await this.establishHibernatableConnection(a.shard);
          
          const shardEndTime = Date.now();
          console.log(`[BotManager] shard ${a.shard} assignment and connection completed`, { time: shardEndTime - shardStartTime });
        } catch (e) {
          console.error(`[BotManager] shard ${a.shard} setup failed:`, e);
        }
      });

      await Promise.allSettled(dispatchPromises);
      const dispatchEndTime = Date.now();
      console.log('[BotManager] Assigned channels to shards', { total, shardCount, dispatchTime: dispatchEndTime - dispatchStartTime, totalTime: dispatchEndTime - startTime });
      return json(200, { ok: true, channels: channels.length, shards: shardCount });
    }
    if (req.method === 'POST' && url.pathname === '/alarm') {
      // noop warm
      return json(200, { ok: true });
    }
    return json(404, { error: 'not found' });
  }

  // Establish hibernatable WebSocket connection for a shard
  async establishHibernatableConnection(shardId: number) {
    try {
      console.log(`[BotManager] Establishing hibernatable connection for shard ${shardId}`);
      
      // Create WebSocket connection to Twitch IRC
      const ws = new WebSocket('wss://irc-ws.chat.twitch.tv:443');
      
      // Create WebSocket pair for hibernation
      const [client, server] = new (globalThis as any).WebSocketPair();
      
      // Get the shard DO
      const id = this.env.IRC_CLIENT!.idFromName(`irc:${shardId}`);
      const stub = this.env.IRC_CLIENT!.get(id);
      
      // Pass the server WebSocket to the DO
      const response = await stub.fetch('https://do/websocket', {
        method: 'POST',
        headers: {
          'Upgrade': 'websocket',
          'Connection': 'Upgrade',
        },
        // @ts-ignore - WebSocket as body is valid for hibernatable connections
        body: server,
      });
      
      if (!response.ok) {
        throw new Error(`Failed to establish hibernatable connection: ${response.status}`);
      }
      
      // Connect the client WebSocket to Twitch IRC
      ws.addEventListener('open', () => {
        console.log(`[BotManager] Twitch IRC connection established for shard ${shardId}`);
      });
      
      ws.addEventListener('message', (event) => {
        // Forward messages from Twitch to the hibernatable WebSocket
        try {
          client.send(event.data);
        } catch (e) {
          console.error(`[BotManager] Failed to forward message to shard ${shardId}:`, e);
        }
      });
      
      ws.addEventListener('close', () => {
        console.log(`[BotManager] Twitch IRC connection closed for shard ${shardId}`);
        try {
          client.close();
        } catch {}
      });
      
      // Forward messages from hibernatable WebSocket to Twitch
      client.addEventListener('message', (event) => {
        try {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(event.data);
          }
        } catch (e) {
          console.error(`[BotManager] Failed to forward message to Twitch for shard ${shardId}:`, e);
        }
      });
      
      console.log(`[BotManager] Hibernatable connection established for shard ${shardId}`);
    } catch (e) {
      console.error(`[BotManager] Failed to establish hibernatable connection for shard ${shardId}:`, e);
    }
  }
}

export { BotManager };

// Implement alarm to periodically (re)assign channels to IRC shards
// and keep the manager object warm.
// Cloudflare Durable Objects will call this method if setAlarm() was used.
// This helps recover after instance restarts without manual /irc/reload.
//
// Note: keep it lightweight; we reuse the same logic as in /reload.
//
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
BotManager.prototype.alarm = async function alarm(this: any) {
  try {
    const startTime = Date.now();
    const channels = await listEnabledChannels(this.env);
    console.log('[BotManager.alarm] loaded channels', { count: channels.length });
    if (!this.env.IRC_CLIENT) {
      console.warn('[BotManager.alarm] IRC_CLIENT not configured');
    } else {
      const shardSize = 50;
      const total = channels.length;
      const shardCount = Math.max(1, Math.min(10, Math.ceil(total / shardSize)));
      const assignments: Array<{ shard: number; channels: any[] }> = Array.from({ length: shardCount }, (_, i) => ({ shard: i, channels: [] }));
      channels.forEach((ch, idx) => assignments[idx % shardCount].channels.push(ch));
      const dispatchPromises = assignments.map(async (a) => {
        const id = this.env.IRC_CLIENT!.idFromName(`irc:${a.shard}`);
        try {
          await this.env.IRC_CLIENT!.get(id).fetch('https://do/assign', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ channels: a.channels }),
          });
        } catch (e) {
          console.error('[BotManager.alarm] assign failed', { shard: a.shard, error: String(e) });
        }
      });
      await Promise.allSettled(dispatchPromises);
    }
    console.log('[BotManager.alarm] done', { ms: Date.now() - startTime });
  } catch (e) {
    console.error('[BotManager.alarm] failed', e);
  } finally {
    if (typeof this.state.setAlarm === 'function') {
      try { this.state.setAlarm(Date.now() + 60_000); } catch {}
    }
  }
};

// IRC Client Durable Object: maintains WebSocket to Twitch IRC and enforces via /timeout
class IrcClientShard {
  state: DurableObjectState;
  env: Env;
  ws: any | null = null;
  connecting = false;
  ready = false;
  didInitialJoin = false;
  botLogin: string | null = null;
  assignedChannels: Array<{ channel_login: string; twitch_id?: string | null }> = [];
  channelSet: Set<string> = new Set();
  channelIdByLogin: Map<string, string> = new Map();
  lastJoinAt = 0;
  joinIntervalMs = 400; // faster join pacing to reduce startup delay
  rateLimitSleepMs = 700; // basic pacing for commands
  keepaliveInterval: any = null; // WebSocket keepalive
  modChannels: Set<string> = new Set(); // Channels where bot has mod permissions
  connectionStartTime = 0; // Track connection duration
  // Diagnostics
  totalRawFrames = 0;
  totalPrivmsg = 0;
  totalPing = 0;
  lastRawAt = 0;
  lastPrivmsgAt = 0;
  lastPingAt = 0;
  framesRing: string[] = [];
  framesMax = 200;
  lastJoinReassertAt = 0;
  joinReassertIntervalMs = 60_000; // every 60s
  statusHeartbeatInterval: any = null; // Testing: frequent status logs

  // Track pending moderation commands for fallback handling
  pendingTimeouts: Map<string, { variant: 'raw' | 'dot' | 'slash'; chan: string; user: string; duration: number; reason: string; sentAt: number } > = new Map();
  // Trace mode: dump parsed messages
  traceAll = true;
  // Reconnection attempt counter for exponential backoff
  reconnectAttempts = 0;
  // Let hibernation work naturally with hibernatable WebSockets

  constructor(state: DurableObjectState, env: Env) {
    log('[IrcClient] constructor start');
    this.state = state;
    this.env = env;
    
    // CRITICAL: Always reload state from storage on constructor wake-up
    this.reloadStateFromStorage().then(() => {
      log('[IrcClient] state reloaded from storage on constructor');
      // After state reload, ensure connection
      this.ensureConnectedAndJoined().catch(e => {
        console.error('[IrcClient] constructor connection failed:', e);
      });
    }).catch(e => {
      console.error('[IrcClient] constructor state reload failed:', e);
    });
    
    // With hibernatable WebSockets, we can use longer alarm intervals
    if (typeof this.state.setAlarm === 'function') {
      try { this.state.setAlarm!(Date.now() + 3600_000); } catch {} // 1 hour intervals
    }
    // Remove status heartbeat spam
    // this.startStatusHeartbeat();
    log('[IrcClient] constructor end');
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === 'POST' && url.pathname === '/assign') {
      const body = await req.json().catch(() => ({}));
      const list = Array.isArray(body.channels) ? body.channels : [];
      this.assignedChannels = list.map((c: any) => ({ channel_login: String(c.channel_login).toLowerCase(), twitch_id: (c as any).twitch_id || null }));
      this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
      this.channelIdByLogin.clear();
      for (const c of this.assignedChannels) if (c.twitch_id) this.channelIdByLogin.set(c.channel_login, String(c.twitch_id));
      await this.state.storage.put('assigned', this.assignedChannels);
      // ensure alarm is armed
      if (typeof this.state.setAlarm === 'function') {
        try { this.state.setAlarm(Date.now() + 15_000); } catch {}
      }
      // Connect or refresh joins (non-blocking)
      const assignTime = Date.now();
      console.log('[IRC] /assign received', { assigned: this.assignedChannels.length, time: assignTime });
      this.ensureConnectedAndJoined().catch(e => console.error('[IRC] connection failed during assign:', e));
      console.log('[IRC] /assign response sent', { time: Date.now() - assignTime });
      return json(200, { ok: true, assigned: this.assignedChannels.length });
    }
    if (req.method === 'POST' && url.pathname === '/reload') {
      const stored = (await this.state.storage.get('assigned')) as any[] | undefined;
      if (stored) {
        this.assignedChannels = stored as any[];
        this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
        this.channelIdByLogin.clear();
        for (const c of this.assignedChannels) if ((c as any).twitch_id) this.channelIdByLogin.set((c as any).channel_login, String((c as any).twitch_id));
      }
      const reloadTime = Date.now();
      console.log('[IRC] /reload received', { assigned: this.assignedChannels.length, time: reloadTime });
      this.ensureConnectedAndJoined().catch(e => console.error('[IRC] connection failed during reload:', e));
      console.log('[IRC] /reload response sent', { time: Date.now() - reloadTime });
      return json(200, { ok: true, assigned: this.assignedChannels.length });
    }
    if (url.pathname === '/state') {
      const state = {
        assigned: this.assignedChannels.length,
        channels: Array.from(this.channelSet).slice(0, 10),
        modChannels: Array.from(this.modChannels).slice(0, 10),
        connecting: this.connecting,
        ready: this.ready,
        didInitialJoin: this.didInitialJoin,
        hasSocket: !!this.ws && (this.ws as any).readyState,
      };
      return json(200, state);
    }
    if (req.method === 'POST' && url.pathname === '/alarm') {
      // noop warm
      return json(200, { ok: true });
    }
    
    if (req.method === 'POST' && url.pathname === '/websocket') {
      // Handle hibernatable WebSocket from main worker
      const upgradeHeader = req.headers.get('Upgrade');
      if (upgradeHeader !== 'websocket') {
        return json(400, { error: 'Expected WebSocket upgrade' });
      }

      // Accept the hibernatable WebSocket
      // @ts-ignore - WebSocket body is valid for hibernatable connections
      const webSocket = req.body;
      if (!webSocket) {
        return json(400, { error: 'No WebSocket provided' });
      }

      try {
        // Accept the hibernatable WebSocket
        // @ts-ignore - accept() method exists on hibernatable WebSockets
        webSocket.accept();
        log('[IRC] Hibernatable WebSocket accepted from main worker');
        
        // Set this as our main WebSocket connection
        await this.setupHibernatableWebSocket(webSocket);
        
        return json(200, { ok: true, hibernatable: true });
      } catch (e) {
        log('[IRC] Failed to accept hibernatable WebSocket', { error: String(e) });
        return json(500, { error: 'Failed to accept WebSocket' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/metrics') {
      return json(200, {
        assigned: this.assignedChannels.length,
        channels: Array.from(this.channelSet),
        modChannels: Array.from(this.modChannels),
        connecting: this.connecting,
        ready: this.ready,
        didInitialJoin: this.didInitialJoin,
        hasSocket: !!this.ws && (this.ws as any).readyState,
        counts: {
          totalRawFrames: this.totalRawFrames,
          totalPrivmsg: this.totalPrivmsg,
          totalPing: this.totalPing,
        },
        lastSeenMs: {
          raw: this.lastRawAt ? Date.now() - this.lastRawAt : null,
          privmsg: this.lastPrivmsgAt ? Date.now() - this.lastPrivmsgAt : null,
          ping: this.lastPingAt ? Date.now() - this.lastPingAt : null,
        }
      });
    }
    if (req.method === 'GET' && url.pathname === '/frames') {
      const u = new URL(req.url);
      const limit = Math.max(1, Math.min(500, Number(u.searchParams.get('limit') || '100') || 100));
      const frames = this.framesRing.slice(-limit);
      return json(200, { framesCount: frames.length, frames });
    }
    // Test endpoints: trigger PING and inject synthetic PRIVMSG for parsing checks
    if (req.method === 'POST' && url.pathname === '/debug/ping') {
      this.sendRaw('PING :manual-test');
      return json(200, { ok: true, sent: 'PING :manual-test' });
    }
    if (req.method === 'POST' && url.pathname === '/debug/privmsg') {
      try {
        const body = await req.json().catch(() => ({} as any));
        const channel_login = String(body?.channel_login || 'yomata1').toLowerCase();
        const user = String(body?.user || 'synthetic_user');
        const text = String(body?.text || 'synthetic message');
        const raw = `:${user}!${user}@${user}.tmi.twitch.tv PRIVMSG #${channel_login} :${text}`;
        // Push directly into handler as if from server
        log('[IRC] DEBUG INJECT PRIVMSG', { raw });
        this.handleIrcMessage(raw + '\r\n');
        return json(200, { ok: true });
      } catch (e: any) {
        return json(500, { error: e?.message || 'failed' });
      }
    }
    if (req.method === 'POST' && url.pathname === '/debug/say') {
      try {
        const body = await req.json().catch(() => ({} as any));
        const channel_login = String(body?.channel_login || '').toLowerCase();
        const message = String(body?.message || 'eloward debug: hello');
        if (!channel_login) return json(400, { error: 'channel_login required' });
        const chan = `#${channel_login}`;
        const text = message.length > 400 ? message.slice(0, 400) : message;
        log('[IRC] DEBUG SAY', { chan, length: text.length });
        this.sendRaw(`PRIVMSG ${chan} :${text}`);
        return json(200, { ok: true, chan });
      } catch (e: any) {
        return json(500, { error: e?.message || 'failed' });
      }
    }
    if (req.method === 'POST' && url.pathname === '/debug/names') {
      try {
        const body = await req.json().catch(() => ({} as any));
        const channel_login = String(body?.channel_login || '').toLowerCase();
        if (!channel_login) return json(400, { error: 'channel_login required' });
        const chan = `#${channel_login}`;
        log('[IRC] DEBUG NAMES', { chan });
        this.sendRaw(`NAMES ${chan}`);
        return json(200, { ok: true, chan });
      } catch (e: any) {
        return json(500, { error: e?.message || 'failed' });
      }
    }
    return json(404, { error: 'not found' });
  }

  // Durable Object alarm: fires periodically to keep shard alive and rejoin if needed
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  async alarm() {
    const now = Date.now();
    // Log heartbeat with timestamps
    log('[IRC] ALARM FIRED', {
      heartbeatIso: new Date(now).toISOString(),
      ready: this.ready,
      wsReadyState: this.ws?.readyState,
      assigned: this.assignedChannels.length,
      channels: Array.from(this.channelSet),
      lastRawMs: this.lastRawAt ? now - this.lastRawAt : null,
      lastPrivmsgMs: this.lastPrivmsgAt ? now - this.lastPrivmsgAt : null,
      lastPingMs: this.lastPingAt ? now - this.lastPingAt : null
    });

    // ALWAYS reload state from storage (remove conditional check)
    await this.reloadStateFromStorage();

    // Aggressive anti-idle: if no activity for >30s, send harmless command
    if (this.ready && this.ws?.readyState === 1 && this.lastRawAt && (now - this.lastRawAt) > 30_000) {
      log('[IRC] ANTI-IDLE: sending harmless PING due to >30s inactivity');
      this.sendRaw('PING :anti-idle-keepalive');
    }

    // Ensure connection and joins ALWAYS
    try {
      await this.ensureConnectedAndJoined();
    } catch (e) {
      console.error('[IRC] alarm ensureConnectedAndJoined failed', e);
    }

    // Re-arm alarm with 1-hour interval for hibernatable WebSockets
    if (typeof this.state.setAlarm === 'function') {
      try { this.state.setAlarm(Date.now() + 3600_000); } catch {}
    }
  }

  // Testing: frequent status heartbeat logs
  startStatusHeartbeat() {
    if (this.statusHeartbeatInterval) return;
    this.statusHeartbeatInterval = setInterval(() => {
      const now = Date.now();
      log('[IRC] STATUS', {
        ready: this.ready,
        wsReadyState: this.ws?.readyState,
        assigned: this.assignedChannels.length,
        channels: Array.from(this.channelSet),
        modChannels: Array.from(this.modChannels),
        lastRawMs: this.lastRawAt ? now - this.lastRawAt : null,
        lastPrivmsgMs: this.lastPrivmsgAt ? now - this.lastPrivmsgAt : null,
        lastPingMs: this.lastPingAt ? now - this.lastPingAt : null,
        connectionSec: this.connectionStartTime ? Math.floor((now - this.connectionStartTime)/1000) : null,
        heartbeatIso: new Date(now).toISOString()
      });
    }, 5_000);
  }

  stopStatusHeartbeat() {
    if (this.statusHeartbeatInterval) {
      try { clearInterval(this.statusHeartbeatInterval); } catch {}
      this.statusHeartbeatInterval = null;
    }
  }

  // Setup hibernatable WebSocket connection to Twitch IRC
  async setupHibernatableWebSocket(webSocket: any) {
    log('[IRC] Setting up hibernatable WebSocket for Twitch IRC');
    
    // Close any existing connection
    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
    }
    
    this.ws = webSocket;
    this.connecting = true;
    this.ready = false;
    this.connectionStartTime = Date.now();
    
    // Get bot credentials for IRC authentication
    const bot = await getBotUserAndToken(this.env);
    if (!bot?.user?.login || !bot?.access) {
      log('[IRC] No bot credentials available for hibernatable connection');
      return;
    }
    
    const login = String(bot.user.login).toLowerCase();
    const token = bot.access;
    
    // Set up WebSocket event handlers
    webSocket.addEventListener('open', () => {
      log('[IRC] Hibernatable WebSocket connected to Twitch IRC', { 
        login, 
        connectionTime: Date.now() - this.connectionStartTime 
      });
      
      // Authenticate with Twitch IRC
      const authToken = token.startsWith('oauth:') ? token : `oauth:${token}`;
      this.sendRaw(`PASS ${authToken}`);
      this.sendRaw(`NICK ${login}`);
      this.sendRaw('CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership');
      log('[IRC] Hibernatable connection authenticated');
      
      this.startKeepalive();
    });
    
    webSocket.addEventListener('message', (event: any) => {
      const msgData = String(event.data || '');
      this.handleIrcMessage(msgData);
    });
    
    webSocket.addEventListener('close', (event: any) => {
      log('[IRC] Hibernatable WebSocket closed', { 
        code: event.code, 
        reason: event.reason,
        wasClean: event.wasClean 
      });
      this.handleDisconnection();
    });
    
    webSocket.addEventListener('error', (event: any) => {
      log('[IRC] Hibernatable WebSocket error', { error: String(event.error || event) });
      this.handleDisconnection();
    });
    
    // For hibernatable WebSockets, the connection should be established immediately
    // since it's coming from the main worker
    log('[IRC] Hibernatable WebSocket setup complete');
  }

  // Handle WebSocket disconnection
  handleDisconnection() {
    log('[IRC] Handling WebSocket disconnection');
    this.ready = false;
    this.connecting = false;
    
    // Stop keepalive
    if (this.keepaliveInterval) {
      clearInterval(this.keepaliveInterval);
      this.keepaliveInterval = null;
    }
    
    // Clear mod channels (will be restored on reconnect)
    this.modChannels.clear();
  }

  // Timeout user via Helix API (more reliable than IRC commands)
  async timeoutViaHelix(channelLogin: string, userLogin: string, duration: number, reason: string) {
    const startTime = Date.now();
    log('[timeoutViaHelix] START', { channelLogin, userLogin, duration, reason });
    
    const tokenStartTime = Date.now();
    const bot = await getBotUserAndToken(this.env);
    log('[timeoutViaHelix] Token fetch completed', { duration: Date.now() - tokenStartTime, hasBot: !!bot });
    
    if (!bot?.access || !bot?.user?.id) {
      const error = 'No bot token available';
      log('[timeoutViaHelix] END - Error (no token)', { duration: Date.now() - startTime, error });
      throw new Error(error);
    }

    // Get channel ID from our stored mapping or fetch it
    const channelId = this.channelIdByLogin.get(channelLogin);
    log('[timeoutViaHelix] Channel ID lookup', { channelLogin, channelId, hasMapping: !!channelId });
    
    if (!channelId) {
      const error = `Channel ID not found for ${channelLogin}`;
      log('[timeoutViaHelix] END - Error (no channel ID)', { duration: Date.now() - startTime, error });
      throw new Error(error);
    }

    // Get user ID by username
    const userLookupStartTime = Date.now();
    log('[timeoutViaHelix] Looking up user ID', { userLogin });
    
    const userResp = await fetch(`https://api.twitch.tv/helix/users?login=${userLogin}`, {
      headers: {
        'Authorization': `Bearer ${bot.access}`,
        'Client-Id': this.env.TWITCH_CLIENT_ID,
      },
    });

    const userLookupDuration = Date.now() - userLookupStartTime;
    log('[timeoutViaHelix] User lookup response', { 
      duration: userLookupDuration, 
      status: userResp.status, 
      ok: userResp.ok 
    });

    if (!userResp.ok) {
      const error = `Failed to get user ID for ${userLogin}: ${userResp.status}`;
      log('[timeoutViaHelix] END - Error (user lookup failed)', { duration: Date.now() - startTime, error });
      throw new Error(error);
    }

    const userData = await userResp.json();
    const userId = userData.data?.[0]?.id;
    log('[timeoutViaHelix] User data parsed', { userId, hasUser: !!userId });
    
    if (!userId) {
      const error = `User ${userLogin} not found`;
      log('[timeoutViaHelix] END - Error (user not found)', { duration: Date.now() - startTime, error });
      throw new Error(error);
    }

    // Send timeout via Helix API
    const timeoutStartTime = Date.now();
    log('[timeoutViaHelix] Sending timeout request', { 
      channelId, 
      moderatorId: bot.user.id, 
      userId, 
      duration, 
      reason 
    });
    
    const timeoutResp = await fetch(`https://api.twitch.tv/helix/moderation/bans?broadcaster_id=${channelId}&moderator_id=${bot.user.id}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${bot.access}`,
        'Client-Id': this.env.TWITCH_CLIENT_ID,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        data: {
          user_id: userId,
          duration: duration,
          reason: reason,
        },
      }),
    });

    const timeoutDuration = Date.now() - timeoutStartTime;
    log('[timeoutViaHelix] Timeout request response', { 
      duration: timeoutDuration, 
      status: timeoutResp.status, 
      ok: timeoutResp.ok 
    });

    if (!timeoutResp.ok) {
      const errorText = await timeoutResp.text();
      const error = `Helix timeout failed: ${timeoutResp.status} ${errorText}`;
      log('[timeoutViaHelix] END - Error (timeout failed)', { 
        duration: Date.now() - startTime, 
        error, 
        status: timeoutResp.status,
        errorText 
      });
      throw new Error(error);
    }

    const result = await timeoutResp.json();
    log('[timeoutViaHelix] END - Success', { 
      duration: Date.now() - startTime,
      totalSteps: 'token->channelId->userId->timeout',
      result: result
    });
    
    return result;
  }

  // CRITICAL FIX: Always reload state from storage to handle hibernation
  async reloadStateFromStorage() {
    try {
      const stored = (await this.state.storage.get('assigned')) as any[] | undefined;
      if (stored && stored.length > 0) {
        this.assignedChannels = stored.map((c: any) => ({ 
          channel_login: String(c.channel_login).toLowerCase(), 
          twitch_id: (c as any).twitch_id || null 
        }));
        this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
        this.channelIdByLogin.clear();
        for (const c of this.assignedChannels) {
          if ((c as any).twitch_id) {
            this.channelIdByLogin.set((c as any).channel_login, String((c as any).twitch_id));
          }
        }
        log('[IRC] STATE RELOADED from storage', { 
          channels: this.assignedChannels.length,
          channelList: this.assignedChannels.map(c => c.channel_login)
        });
      } else {
        log('[IRC] No stored state found or empty channels');
      }
      
      // Also reload mod channels state
      const storedModChannels = (await this.state.storage.get('modChannels')) as string[] | undefined;
      if (storedModChannels && storedModChannels.length > 0) {
        this.modChannels = new Set(storedModChannels);
        log('[IRC] MOD CHANNELS RELOADED from storage', { modChannels: Array.from(this.modChannels) });
      }
    } catch (e) {
      console.error('[IRC] Failed to reload state from storage:', e);
    }
  }

  // Save current state to storage to survive hibernation
  async saveStateToStorage() {
    try {
      // Save mod channels state
      await this.state.storage.put('modChannels', Array.from(this.modChannels));
      log('[IRC] STATE SAVED to storage', { modChannels: Array.from(this.modChannels) });
    } catch (e) {
      console.error('[IRC] saveStateToStorage failed:', e);
    }
  }

  async ensureConnectedAndJoined() {
    console.log('[IRC] ensureConnectedAndJoined called', { 
      hasWs: !!this.ws, 
      readyState: this.ws?.readyState, 
      ready: this.ready,
      channels: this.assignedChannels.length
    });
    if (!this.ws || this.ws.readyState !== 1) {
      console.log('[IRC] need to connect - no websocket or not open');
      await this.connectIrc();
    } else {
      console.log('[IRC] websocket already connected');
    }
    await this.joinAssigned();
  }

  async connectIrc() {
    if (this.connecting) return;
    this.connecting = true;
    const connectStartTime = Date.now();
    console.log('[IRC] connecting...', connectStartTime);
    
    try {
      const tokenStartTime = Date.now();
      const bot = await getBotUserAndToken(this.env);
      const tokenEndTime = Date.now();
      console.log('[IRC] token fetch time:', tokenEndTime - tokenStartTime);
      
      if (!bot?.access || !bot?.user?.login) {
        console.error('[IRC] no valid bot token available', { hasBot: !!bot, hasAccess: !!bot?.access, hasUser: !!bot?.user?.login });
        this.connecting = false;
        return;
      }
      
      const login = String(bot.user.login).toLowerCase();
      const token = bot.access;
      console.log('[IRC] creating WebSocket connection', { login, tokenLength: token.length, tokenExpiry: bot.expires_at });
      
      // Validate token format
      if (!token.startsWith('oauth:') && token.length < 20) {
        console.error('[IRC] invalid token format', { tokenStart: token.substring(0, 10) });
        this.connecting = false;
        return;
      }
      
      // @ts-ignore - WebSocket is available in Workers runtime
      const socket: any = new WebSocket('wss://irc-ws.chat.twitch.tv:443');
      
      // Accept hibernatable WebSocket to prevent premature hibernation
      if (typeof socket.accept === 'function') {
        try {
          socket.accept();
          log('[IRC] Hibernatable WebSocket enabled - proper hibernation handling');
        } catch (e) {
          log('[IRC] Hibernatable WebSocket not available, using regular WebSocket', { error: String(e) });
        }
      }
      
      this.ws = socket;
      
      // Add connection timeout
      const connectTimeout = setTimeout(() => {
        if (socket.readyState === 0 || socket.readyState === 2) { // CONNECTING or CLOSING
          console.error('[IRC] connection timeout');
          try { socket.close(); } catch {}
          this.scheduleReconnect();
        }
      }, 10000);
      
      socket.onopen = () => {
        const openTime = Date.now();
        this.connectionStartTime = openTime; // Track connection start
        clearTimeout(connectTimeout);
        this.ready = false;
        this.didInitialJoin = false;
        this.botLogin = login;
        log('[IRC] socket open, authenticating', { login, openTime: openTime - connectStartTime });
        
        // Send auth commands immediately with proper format
        const authToken = token.startsWith('oauth:') ? token : `oauth:${token}`;
        log('[IRC] sending auth', { tokenFormat: authToken.substring(0, 15) + '...' });
        this.sendRaw(`PASS ${authToken}`);
        this.sendRaw(`NICK ${login}`);
        this.sendRaw('CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership');
        log('[IRC] auth commands sent');
        
        // Start keepalive to prevent connection timeout  
        this.startKeepalive();
      };
      
      socket.onmessage = (evt: any) => {
        const msgData = String(evt.data || '');
        const connectionTime = Date.now() - this.connectionStartTime;
        // Diagnostics
        this.totalRawFrames += 1;
        this.lastRawAt = Date.now();
        if (this.framesRing.length >= this.framesMax) this.framesRing.shift();
        this.framesRing.push(msgData);
        
        // **DIAGNOSTIC**: Log ALL messages for complete visibility
        log(`[IRC] RAW MESSAGE at ${Math.floor(connectionTime/1000)}s:`, msgData);
        
        // **TEST 2**: Verify we receive ANY IRC messages
        if (msgData.includes('PRIVMSG')) {
          log('🔴 [TEST 2] PRIVMSG DETECTED - SUCCESS!', msgData);
        }
        
        this.handleIrcMessage(msgData);
      };
      
      socket.onclose = (evt: any) => { 
        clearTimeout(connectTimeout);
        this.stopKeepalive();
        const closeTime = Date.now();
        const connectionDuration = this.connectionStartTime ? closeTime - this.connectionStartTime : 0;
        
        log('[IRC] WEBSOCKET DISCONNECTED - CRITICAL', { 
          code: evt.code, 
          reason: evt.reason, 
          wasClean: evt.wasClean,
          connectionDuration: `${Math.floor(connectionDuration/1000)}s`,
          totalTime: closeTime - connectStartTime,
          ready: this.ready,
          joined: this.didInitialJoin,
          channelsJoined: this.channelSet.size
        }); 
        
        // Track 1006 disconnections specifically
        if (evt.code === 1006) {
          log('[IRC] 1006 DISCONNECT ANALYSIS', {
            timeConnected: `${Math.floor(connectionDuration/1000)}s`,
            expectedKeepalive: '30s intervals',
            likelyCloudflareIssue: connectionDuration < 120000 // Less than 2 minutes
          });
        }
        
        this.scheduleReconnect(); 
      };
      
      socket.onerror = (evt: any) => { 
        clearTimeout(connectTimeout);
        console.error('[IRC] socket error:', evt, { connectTime: Date.now() - connectStartTime }); 
        this.scheduleReconnect(); 
      };
      
    } catch (e) {
      console.error('[IRC] connection failed:', e);
      this.scheduleReconnect();
    } finally {
      this.connecting = false;
    }
  }

  scheduleReconnect() {
    this.stopKeepalive();
    this.ws = null;
    this.ready = false;
    this.didInitialJoin = false;
    this.reconnectAttempts++;
    
    // Exponential backoff: 1s * 2^attempts, max 32s
    const backoffMs = Math.min(32_000, 1_000 * Math.pow(2, this.reconnectAttempts));
    const jitterMs = Math.floor(Math.random() * 1_000); // Add jitter
    const totalDelayMs = backoffMs + jitterMs;
    
    console.warn('[IRC] scheduling reconnect', { 
      attempt: this.reconnectAttempts, 
      backoffMs, 
      totalDelayMs 
    });
    
    setTimeout(() => this.connectIrc(), totalDelayMs);
  }

  sendRaw(line: string) {
    try {
      log('[IRC] >> SEND', { readyState: this.ws?.readyState, line });
      if (this.ws && this.ws.readyState === 1) this.ws.send(line);
    } catch {}
  }

  async joinAssigned() {
    if (!this.ws || this.ws.readyState !== 1 || !this.ready) {
      log('[IRC] not ready for joins', { hasSocket: !!this.ws, readyState: this.ws?.readyState, ready: this.ready });
      return;
    }
    
    const channels = Array.from(this.channelSet);
    if (channels.length === 0) {
      log('[IRC] no channels to join');
      return;
    }
    
    log('[IRC] starting joins', { count: channels.length, interval: this.joinIntervalMs });
    const joinStartTime = Date.now();
    
    for (const chan of channels) {
      const now = Date.now();
      if (now - this.lastJoinAt < this.joinIntervalMs) {
        await this.sleep(this.joinIntervalMs - (now - this.lastJoinAt));
      }
      const joinTime = Date.now();
      log(`[IRC] JOIN ${chan} at +${joinTime - joinStartTime}ms`);
      this.sendRaw(`JOIN ${chan}`);
      this.lastJoinAt = Date.now();
    }
    
    const totalJoinTime = Date.now() - joinStartTime;
    log(`[IRC] finished joining all channels in ${totalJoinTime}ms`);
  }

  async handleIrcMessage(raw: string) {
    const lines = raw.split('\r\n');
    log(`[IRC] processing ${lines.length} lines`);
    for (const line of lines) {
      if (!line) continue;
      const lineStartTime = Date.now();
      
      if (line.startsWith('PING ')) {
        this.totalPing += 1;
        this.lastPingAt = Date.now();
        log('[IRC] handling PING');
        this.sendRaw(line.replace('PING', 'PONG'));
        continue;
      }
      const msg = this.parseIrc(line);
      if (!msg) {
        log('[IRC] failed to parse line', line);
        continue;
      }
      
      const lineProcessTime = Date.now() - lineStartTime;
      if (msg.command === 'PRIVMSG') {
        this.totalPrivmsg += 1;
        this.lastPrivmsgAt = Date.now();
        log(`[IRC] parsed PRIVMSG in ${lineProcessTime}ms`, { command: msg.command, params: msg.params?.slice(0,1) });
      }
      // Handle authentication errors first
      if (msg.command === 'NOTICE' && msg.params?.[1]?.includes('Login unsuccessful')) {
        console.error('[IRC] Authentication failed:', msg.params?.[1]);
        this.scheduleReconnect();
        continue;
      }
      if (msg.command === '464') { // ERR_PASSWDMISMATCH
        console.error('[IRC] Password mismatch error');
        this.scheduleReconnect(); 
        continue;
      }
      
      // Mark ready on welcome or global user state, then join channels once
      if (msg.command === '001' || msg.command === 'GLOBALUSERSTATE') {
        if (!this.ready) {
          this.ready = true;
          this.reconnectAttempts = 0; // Reset reconnection counter on successful connection
          console.log('[IRC] ready. Will join channels:', this.channelSet.size);
          if (!this.didInitialJoin) {
            await this.joinAssigned();
            this.didInitialJoin = true;
          }
        }
        // Ensure echo-message capability so we receive our own PRIVMSG (helps testing)
        this.sendRaw('CAP REQ :twitch.tv/echo-message');
        continue;
      }
      if (msg.command === 'CAP') {
        // Example: :tmi.twitch.tv CAP * ACK :twitch.tv/membership
        console.log('[IRC] CAP', msg.params?.join(' '));
        continue;
      }
      if (msg.command === 'ROOMSTATE') {
        log('[IRC] ROOMSTATE', { params: msg.params, tags: msg.tags });
        continue;
      }
      if (msg.command === '353') { // NAMES reply
        // Params often like: [me, '=', '#channel', 'name list...']
        log('[IRC] 353 (NAMES) received', { params: msg.params });
        continue;
      }
      if (msg.command === '366') { // End of NAMES
        log('[IRC] 366 (End of NAMES)', { params: msg.params?.slice(0, 1) });
        continue;
      }
      if (msg.command === 'PART') {
        log('[IRC] PART detected', { prefix: msg.prefix, params: msg.params });
        continue;
      }
      if (msg.command === 'USERNOTICE') {
        log('[IRC] USERNOTICE', { params: msg.params, tags: msg.tags });
        continue;
      }
      if (msg.command === 'WHISPER') {
        log('[IRC] WHISPER', { params: msg.params, tags: msg.tags });
        continue;
      }
      if (msg.command === 'USERSTATE' && msg.params?.[0]) {
        // **TEST 4**: Verify mod permission detection
        const channel = msg.params[0];
        const isMod = msg.tags?.mod === '1' || msg.tags?.badges?.includes('moderator');
        log('🔵 [TEST 4] MOD PERMISSION CHECK', { channel, isMod, mod: msg.tags?.mod, badges: msg.tags?.badges });
        if (isMod) {
          this.modChannels.add(channel);
          // Persist mod status to survive hibernation
          this.saveStateToStorage();
          log('🟢 [TEST 4] BOT HAS MOD PERMISSIONS', channel);
        } else {
          this.modChannels.delete(channel);
          log('🔴 [TEST 4] BOT LACKS MOD PERMISSIONS', { channel, message: 'will ignore messages' });
        }
        continue;
      }
      if (msg.command === 'NOTICE') {
        const noticeText = msg.params?.join(' ') || '';
        console.warn('[IRC] NOTICE', noticeText);
        // NOTE: IRC moderation commands (.timeout, /timeout) were deprecated by Twitch on Feb 24, 2023
        // All moderation must now use Helix API - no IRC fallback is possible
        if ((msg.tags?.['msg-id'] || '').includes('unrecognized_cmd')) {
          log('[IRC] Ignoring unrecognized_cmd - IRC moderation commands deprecated since Feb 2023');
        }
        continue;
      }
      if (msg.command === 'RECONNECT') {
        this.scheduleReconnect();
        continue;
      }
      if (msg.command === 'JOIN') {
        // **TEST 1**: Log ALL joins (bot and users) for verification
        try {
          const who = (msg.prefix?.split('!')[0] || '').toLowerCase();
          const chan = (msg.params?.[0] || '').toLowerCase();
          log(`[IRC] JOIN detected: ${who} joined ${chan}`);
          if (who && this.botLogin && who === this.botLogin) {
            log(`🟢 [TEST 1] BOT SUCCESSFULLY JOINED ${chan}!`);
          }
        } catch {}
        continue;
      }
      if (msg.command === 'PRIVMSG') {
        // **TEST 3**: Verify PRIVMSG parsing and detection
        log('🟡 [TEST 3] PRIVMSG PARSING SUCCESS', { command: msg.command, paramsCount: msg.params?.length });
        
        const privmsgStartTime = Date.now();
        const channel = msg.params?.[0] || '';
        const message = msg.params?.[1] || '';
        const login = (msg.prefix?.split('!')[0] || '').toLowerCase();
        const chanLogin = channel.startsWith('#') ? channel.slice(1).toLowerCase() : channel.toLowerCase();
        
        log('[IRC] PRIVMSG received', { 
          channel, 
          chanLogin, 
          user: login, 
          messageLength: message.length,
          hasChannel: this.channelSet.has(`#${chanLogin}`),
          hasMod: this.modChannels.has(`#${chanLogin}`)
        });
        
        if (!this.channelSet.has(`#${chanLogin}`)) {
          log('[IRC] PRIVMSG ignored - not in assigned channels');
          continue;
        }
        
        // Skip if bot lacks mod permissions in this channel
        if (!this.modChannels.has(`#${chanLogin}`)) {
          log(`[IRC] PRIVMSG ignored - bot lacks mod permissions in ${chanLogin}`);
          continue;
        }
        
        log(`[IRC] PRIVMSG processing started for ${login} in ${chanLogin}`, {
          messagePreview: message.slice(0, 50),
          tags: msg.tags,
          assignedChannels: this.assignedChannels.length,
          modChannels: Array.from(this.modChannels)
        });

        // Let hibernation work naturally with hibernatable WebSockets

        // Skip privileged roles using tags
        const badgeCheckStartTime = Date.now();
        const badges = this.getBadgeSet(msg.tags?.badges || '');
        log('[IRC] Badge check completed', { 
          duration: Date.now() - badgeCheckStartTime,
          badges: Array.from(badges),
          rawBadges: msg.tags?.badges
        });
        
        if (badges.has('broadcaster') || badges.has('moderator') || badges.has('vip')) {
          log('[IRC] PRIVMSG privileged user skipped', { 
            user: login, 
            badges: Array.from(badges),
            reason: 'privileged_role'
          });
          // For diagnostics, still log the content from privileged users
          log('[IRC] PRIVMSG privileged content', { user: login, channel: chanLogin, message });
          // No processing tracking needed with hibernatable WebSockets
          continue;
        }
        
        log('[IRC] User passed privilege check - proceeding with enforcement', { user: login });

        // Enforce rules
        try {
          const enforceStartTime = Date.now();
          log('[Enforce] Starting enforcement check', { user: login, channel: chanLogin });
          
          const configStartTime = Date.now();
          const cfg = await getChannelConfig(this.env, chanLogin);
          log('[Enforce] Channel config loaded', { 
            duration: Date.now() - configStartTime,
            hasConfig: !!cfg,
            botEnabled: cfg?.bot_enabled,
            timeoutSeconds: cfg?.timeout_seconds,
            enforcementMode: cfg?.enforcement_mode
          });
          
          if (!cfg || !cfg.bot_enabled) {
            log(`[Enforce] Enforcement skipped - bot disabled for ${chanLogin}`, { hasConfig: !!cfg, botEnabled: cfg?.bot_enabled });
            // No processing tracking needed
            continue;
          }

          let shouldTimeout = false;
          let hasPlus = false;
          log('[Enforce] Starting rank check', { user: login, enforcementMode: cfg.enforcement_mode });
          
          try {
            const rankStartTime = Date.now();
            const rankReq = new Request(`https://internal/api/ranks/lol/${encodeURIComponent(login)}`);
            log('[Enforce] Rank API request created', { url: rankReq.url });
            
            // Add 800ms timeout to avoid stalled subrequests during spikes
            const timeoutPromise = new Promise<Response>((_, reject) => setTimeout(() => reject(new Error('rank timeout')), 800));
            let rankRes: Response | null = null;
            try {
              rankRes = await Promise.race([this.env.RANK_WORKER.fetch(rankReq), timeoutPromise]);
              log('[Enforce] Rank API response received', { status: rankRes?.status, ok: rankRes?.ok });
            } catch (e) {
              log('[Enforce] Rank API fetch timed out', { user: login, error: String(e) });
            }
            const rankTime = Date.now() - rankStartTime;
            let rank: any = null;
            if (rankRes && rankRes.ok) {
              rank = await rankRes.json();
              hasPlus = rank?.plus_active || false;
              log(`[Enforce] Rank found in ${rankTime}ms`, { 
                user: login, 
                tier: rank?.rank_tier, 
                division: rank?.rank_division,
                lp: rank?.lp,
                region: rank?.region,
                hasPlus
              });
            } else if (rankRes) {
              log(`[Enforce] Rank not found in ${rankTime}ms`, { user: login, status: rankRes.status });
            } else {
              log('[Enforce] Rank API no response (timeout)', { user: login, duration: rankTime });
            }
            
            const mode = (cfg.enforcement_mode || 'has_rank') as 'has_rank' | 'min_rank';
            log('[Enforce] Evaluating enforcement rules', { 
              mode, 
              hasRank: !!(rank && rank.rank_tier),
              userTier: rank?.rank_tier,
              userDivision: rank?.rank_division,
              minTier: cfg.min_rank_tier,
              minDivision: cfg.min_rank_division
            });
            
            if (mode === 'has_rank') {
              shouldTimeout = !(rank && rank.rank_tier);
              log('[Enforce] has_rank mode evaluation', { shouldTimeout, hasRankTier: !!(rank && rank.rank_tier) });
            } else {
              const threshold = {
                tier: String(cfg.min_rank_tier || '').toUpperCase(),
                division: Number(cfg.min_rank_division || 0),
              };
              const meetsThreshold = threshold.tier ? meetsMinRank(rank, threshold) : !!(rank && rank.rank_tier);
              shouldTimeout = !meetsThreshold;
              log('[Enforce] min_rank mode evaluation', { 
                shouldTimeout, 
                threshold, 
                meetsThreshold,
                userRank: rank ? `${rank.rank_tier} ${rank.rank_division}` : 'none'
              });
            }
            
            const decisionTime = Date.now() - enforceStartTime;
            log(`[Enforce] Final decision in ${decisionTime}ms`, { 
              channel: chanLogin, 
              user: login, 
              mode, 
              shouldTimeout,
              rankCheckDuration: rankTime
            });
          } catch (e) {
            log('[Rank] lookup failed', { user: login, error: e });
          }

          if (!shouldTimeout) {
            // No processing tracking needed
            continue;
          }

          // **TEST 5**: Verify message processing pipeline
          log('🟠 [TEST 5] TIMEOUT COMMAND TRIGGERED', { channel: chanLogin, user: login, shouldTimeout });

          // Send timeout via IRC command
          const timeoutStartTime = Date.now();
          const duration = cfg.timeout_seconds || 30;
          const reason = resolveReasonTemplate(this.env, cfg, login);
          await this.sleep(this.rateLimitSleepMs);
          
          const timeoutExecuteTime = Date.now() - timeoutStartTime;
          const totalProcessTime = Date.now() - privmsgStartTime;
          log(`[Enforce] timeout sending in ${timeoutExecuteTime}ms, total ${totalProcessTime}ms`, { channel: chanLogin, user: login, duration });
          const chan = `#${chanLogin}`;
          const key = `${chan}:${login}`;
          
          // USE HELIX API ONLY (IRC moderation commands deprecated Feb 24, 2023)
          try {
            await this.timeoutViaHelix(chanLogin, login, duration, reason);
            log('[Enforce] ✅ HELIX timeout successful', { channel: chanLogin, user: login, duration });
          } catch (helixError) {
            log('[Enforce] ❌ HELIX timeout failed - no fallback available (IRC deprecated)', { 
              error: String(helixError), 
              channel: chanLogin, 
              user: login 
            });
            // Note: IRC commands like .timeout and /timeout were deprecated by Twitch on Feb 24, 2023
            // The only way to timeout users is via Helix API with proper OAuth scopes
          } finally {
            // No processing tracking needed with hibernatable WebSockets
          }
        } catch (e) {
          log('[Enforce] processing failed', { user: login, channel: chanLogin, error: e });
          // No processing tracking needed
        }
      }
      // Fallback: log any unhandled command to aid debugging
      log('[IRC] UNHANDLED', { command: msg.command, params: msg.params });
      if (this.traceAll) {
        try { log('[IRC] TRACE MSG', msg); } catch {}
      }
    }
  }

  parseIrc(line: string): { tags?: Record<string, string>; prefix?: string; command: string; params: string[] } | null {
    let rest = line;
    const msg: any = { params: [] };
    if (rest.startsWith('@')) {
      const sp = rest.indexOf(' ');
      const rawTags = rest.slice(1, sp);
      rest = rest.slice(sp + 1);
      msg.tags = {};
      for (const part of rawTags.split(';')) {
        const [k, v] = part.split('=');
        msg.tags[k] = v || '';
      }
    }
    if (rest.startsWith(':')) {
      const sp = rest.indexOf(' ');
      msg.prefix = rest.slice(1, sp);
      rest = rest.slice(sp + 1);
    }
    const sp = rest.indexOf(' ');
    if (sp === -1) return null;
    msg.command = rest.slice(0, sp);
    rest = rest.slice(sp + 1);
    if (rest.startsWith(':')) {
      msg.params = [rest.slice(1)];
    } else {
      const parts: string[] = [];
      while (rest) {
        if (rest.startsWith(':')) { parts.push(rest.slice(1)); break; }
        const idx = rest.indexOf(' ');
        if (idx === -1) { parts.push(rest); break; }
        parts.push(rest.slice(0, idx));
        rest = rest.slice(idx + 1);
      }
      msg.params = parts;
    }
    return msg;
  }

  getBadgeSet(badgesStr: string): Set<string> {
    const s = new Set<string>();
    String(badgesStr || '').split(',').forEach((b) => {
      const name = b.split('/')[0];
      if (name) s.add(name);
    });
    return s;
  }

  sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

  startKeepalive() {
    this.stopKeepalive();
    log('[IRC] starting aggressive keepalive - every 30s');
    this.keepaliveInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === 1) {
        const connectionTime = Date.now() - this.connectionStartTime;
        log(`[IRC] sending keepalive PING after ${Math.floor(connectionTime/1000)}s connected`);
        this.sendRaw('PING :keepalive-test');
        // Reassert JOIN periodically if idle and assigned
        const now = Date.now();
        const idleMs = this.lastRawAt ? now - this.lastRawAt : Number.MAX_SAFE_INTEGER;
        if (idleMs > this.joinReassertIntervalMs && this.channelSet.size > 0 && now - this.lastJoinReassertAt > this.joinReassertIntervalMs) {
          this.lastJoinReassertAt = now;
          for (const chan of this.channelSet) {
            log('[IRC] reassert JOIN', chan);
            this.sendRaw(`JOIN ${chan}`);
          }
        }
      } else {
        log(`[IRC] keepalive check - websocket not ready`, { readyState: this.ws?.readyState });
      }
    }, 30000); // Every 30 seconds for aggressive testing
  }

  stopKeepalive() {
    if (this.keepaliveInterval) {
      console.log('[IRC] stopping keepalive');
      clearInterval(this.keepaliveInterval);
      this.keepaliveInterval = null;
    }
    this.stopStatusHeartbeat();
  }
}

export { IrcClientShard };

