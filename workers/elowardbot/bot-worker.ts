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
  // Internal write key for trusted calls from website backend
  INTERNAL_WRITE_KEY?: string;
  BOT_WRITE_KEY?: string;
}

const router = Router();

// Scopes for IRC: chat read/write only
const BOT_SCOPES = [
  'chat:read',
  'chat:edit',
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
    console.log('[Bot] /irc/reload called');
    const id = env.BOT_MANAGER.idFromName('manager');
    ctx.waitUntil(env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' }));
    return json(202, { accepted: true });
  } catch (e: any) {
    return json(500, { error: e?.message || 'reload failed' });
  }
});

// Quick shard state probe: /irc/state?shard=0
router.get('/irc/state', async (req: Request, env: Env) => {
  try {
    const u = new URL(req.url);
    const shard = Number(u.searchParams.get('shard') || '0') || 0;
    if (!env.IRC_CLIENT) return json(200, { ok: false, note: 'IRC_CLIENT binding not configured' });
    const id = env.IRC_CLIENT.idFromName(`irc_v2:${shard}`);
    const res = await env.IRC_CLIENT.get(id).fetch('https://do/state');
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'state failed' });
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
  const expected = (env as any).INTERNAL_WRITE_KEY || (env as any).BOT_WRITE_KEY || '';
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
  try {
    const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, enforcement_mode, min_rank_tier, min_rank_division
               FROM twitch_bot_users
               WHERE bot_enabled = 1`;
    const res = await env.DB.prepare(q).all();
    const rows = res?.results || [];
    return rows as ChannelConfig[];
  } catch {
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
  try {
    const raw = (await env.BOT_KV.get('bot_tokens', 'json')) as any;
    if (!raw?.tokens?.access_token) return null;
    let access = raw.tokens.access_token as string;
    let refresh = raw.tokens.refresh_token as string | undefined;
    let exp = raw.tokens.expires_at as number | undefined;
    
    // Only refresh if token expires soon and we have refresh token
    if (refresh && exp && Date.now() > exp - 60_000) {
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
        
        console.log('[Token] refreshed successfully');
      } catch (e) {
        console.error('[Token] refresh failed, using existing token:', e);
        // Use existing token even if refresh fails
      }
    }
    
    return { access, refresh, expires_at: exp, user: raw.user };
  } catch (e) {
    console.error('[Token] getBotUserAndToken failed:', e);
    return null;
  }
}

// IRC client is implemented via Durable Object IrcClientShard; no EventSub ingestion.

class BotManager {
  state: DurableObjectState;
  env: Env;
  constructor(state: DurableObjectState, env: Env) {
    console.log('[BotManager] constructor start:', Date.now());
    this.state = state;
    this.env = env;
    // keep warm more aggressively
    if (typeof this.state.setAlarm === 'function') {
      const next = Date.now() + 60_000 * 2; // every 2 minutes for better performance  
      try { this.state.setAlarm!(next); } catch {}
    }
    console.log('[BotManager] constructor end:', Date.now());
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
        const id = this.env.IRC_CLIENT!.idFromName(`irc_v2:${a.shard}`);
        console.log('[BotManager] dispatch /assign', { shard: a.shard, count: a.channels.length, time: shardStartTime });
        
        const assignPromise = this.env.IRC_CLIENT!.get(id).fetch('https://do/assign', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ channels: a.channels }),
        });
        
        // 10s timeout per shard to prevent hanging
        const timeoutPromise = new Promise<Response>((_, reject) => 
          setTimeout(() => reject(new Error(`Shard ${a.shard} assign timeout`)), 10000)
        );
        
        try {
          await Promise.race([assignPromise, timeoutPromise]);
          const shardEndTime = Date.now();
          console.log(`[BotManager] shard ${a.shard} assignment completed`, { time: shardEndTime - shardStartTime });
        } catch (e) {
          console.error(`[BotManager] shard ${a.shard} assignment failed:`, e);
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
}

export { BotManager };

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

  constructor(state: DurableObjectState, env: Env) {
    console.log('[IrcClient] constructor start:', Date.now());
    this.state = state;
    this.env = env;
    // keep warm more aggressively
    if (typeof this.state.setAlarm === 'function') {
      const next = Date.now() + 60_000 * 2; // every 2 minutes for better performance  
      try { this.state.setAlarm!(next); } catch {}
    }
    console.log('[IrcClient] constructor end:', Date.now());
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
      // Connect or refresh joins (non-blocking)
      console.log('[IRC] /assign received', { assigned: this.assignedChannels.length });
      this.ensureConnectedAndJoined().catch(e => console.error('[IRC] connection failed during assign:', e));
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
      console.log('[IRC] /reload received', { assigned: this.assignedChannels.length });
      this.ensureConnectedAndJoined().catch(e => console.error('[IRC] connection failed during reload:', e));
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
    return json(404, { error: 'not found' });
  }

  async ensureConnectedAndJoined() {
    if (!this.ws || this.ws.readyState !== 1) {
      await this.connectIrc();
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
        clearTimeout(connectTimeout);
        this.ready = false;
        this.didInitialJoin = false;
        this.botLogin = login;
        console.log('[IRC] socket open, authenticating as', { login, openTime: openTime - connectStartTime });
        
        // Send auth commands immediately with proper format
        const authToken = token.startsWith('oauth:') ? token : `oauth:${token}`;
        console.log('[IRC] sending auth...', { tokenFormat: authToken.substring(0, 15) + '...' });
        this.sendRaw(`PASS ${authToken}`);
        this.sendRaw(`NICK ${login}`);
        this.sendRaw('CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership');
        console.log('[IRC] auth commands sent');
        
        // Start keepalive to prevent connection timeout  
        this.startKeepalive();
      };
      
      socket.onmessage = (evt: any) => {
        const msgData = String(evt.data || '');
        console.log('[IRC] raw message received:', msgData);
        this.handleIrcMessage(msgData);
      };
      
      socket.onclose = (evt: any) => { 
        clearTimeout(connectTimeout);
        this.stopKeepalive();
        const closeTime = Date.now();
        console.error('[IRC] socket closed:', { 
          code: evt.code, 
          reason: evt.reason, 
          wasClean: evt.wasClean,
          totalTime: closeTime - connectStartTime,
          ready: this.ready,
          joined: this.didInitialJoin
        }); 
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
    console.warn('[IRC] scheduling reconnect');
    setTimeout(() => this.connectIrc(), 3_000 + Math.floor(Math.random() * 4_000));
  }

  sendRaw(line: string) {
    try { if (this.ws && this.ws.readyState === 1) this.ws.send(line); } catch {}
  }

  async joinAssigned() {
    if (!this.ws || this.ws.readyState !== 1 || !this.ready) {
      console.log('[IRC] not ready for joins:', { hasSocket: !!this.ws, readyState: this.ws?.readyState, ready: this.ready });
      return;
    }
    
    const channels = Array.from(this.channelSet);
    if (channels.length === 0) {
      console.log('[IRC] no channels to join');
      return;
    }
    
    console.log('[IRC] joining channels:', { count: channels.length, interval: this.joinIntervalMs });
    
    for (const chan of channels) {
      const now = Date.now();
      if (now - this.lastJoinAt < this.joinIntervalMs) {
        await this.sleep(this.joinIntervalMs - (now - this.lastJoinAt));
      }
      console.log('[IRC] JOIN', chan);
      this.sendRaw(`JOIN ${chan}`);
      this.lastJoinAt = Date.now();
    }
    
    console.log('[IRC] finished joining all channels');
  }

  async handleIrcMessage(raw: string) {
    const lines = raw.split('\r\n');
    console.log('[IRC] processing', lines.length, 'lines');
    for (const line of lines) {
      if (!line) continue;
      console.log('[IRC] processing line:', line);
      if (line.startsWith('PING ')) {
        console.log('[IRC] handling PING');
        this.sendRaw(line.replace('PING', 'PONG'));
        continue;
      }
      const msg = this.parseIrc(line);
      if (!msg) {
        console.log('[IRC] failed to parse line:', line);
        continue;
      }
      console.log('[IRC] parsed message:', { command: msg.command, params: msg.params });
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
          console.log('[IRC] ready. Will join channels:', this.channelSet.size);
          if (!this.didInitialJoin) {
            await this.joinAssigned();
            this.didInitialJoin = true;
          }
        }
        continue;
      }
      if (msg.command === 'CAP') {
        // Example: :tmi.twitch.tv CAP * ACK :twitch.tv/membership
        console.log('[IRC] CAP', msg.params?.join(' '));
        continue;
      }
      if (msg.command === 'USERSTATE' && msg.params?.[0]) {
        // Track our mod status in channels  
        const channel = msg.params[0];
        const isMod = msg.tags?.mod === '1' || msg.tags?.badges?.includes('moderator');
        console.log('[IRC] USERSTATE', { channel, isMod, mod: msg.tags?.mod, badges: msg.tags?.badges });
        if (isMod) {
          this.modChannels.add(channel);
          console.log('[IRC] Bot has mod permissions in', channel);
        } else {
          this.modChannels.delete(channel);
          console.log('[IRC] Bot lacks mod permissions in', channel, '- will ignore messages');
        }
        continue;
      }
      if (msg.command === 'NOTICE') {
        console.warn('[IRC] NOTICE', msg.params?.join(' '));
        continue;
      }
      if (msg.command === 'RECONNECT') {
        this.scheduleReconnect();
        continue;
      }
      if (msg.command === 'JOIN') {
        // Log our own joins for verification
        try {
          const who = (msg.prefix?.split('!')[0] || '').toLowerCase();
          const chan = (msg.params?.[0] || '').toLowerCase();
          if (who && this.botLogin && who === this.botLogin) console.log('[IRC] joined', chan);
        } catch {}
        continue;
      }
      if (msg.command === 'PRIVMSG') {
        const channel = msg.params?.[0] || '';
        const message = msg.params?.[1] || '';
        const login = (msg.prefix?.split('!')[0] || '').toLowerCase();
        const chanLogin = channel.startsWith('#') ? channel.slice(1).toLowerCase() : channel.toLowerCase();
        console.log('[IRC] PRIVMSG received', { 
          channel, 
          chanLogin, 
          user: login, 
          messageLength: message.length,
          channelSet: Array.from(this.channelSet).slice(0, 3), 
          hasChannel: this.channelSet.has(`#${chanLogin}`)
        });
        
        if (!this.channelSet.has(`#${chanLogin}`)) {
          console.log('[IRC] PRIVMSG ignored - not in assigned channels');
          continue;
        }
        
        // Skip if bot lacks mod permissions in this channel
        if (!this.modChannels.has(`#${chanLogin}`)) {
          console.log('[IRC] PRIVMSG ignored - bot lacks mod permissions in', `#${chanLogin}`);
          continue;
        }
        
        console.log('[IRC] PRIVMSG processing', { channel: `#${chanLogin}`, user: login, message });

        // Skip privileged roles using tags
        const badges = this.getBadgeSet(msg.tags?.badges || '');
        if (badges.has('broadcaster') || badges.has('moderator') || badges.has('vip')) continue;

        // Enforce rules
        try {
          const cfg = await getChannelConfig(this.env, chanLogin);
          if (!cfg || !cfg.bot_enabled) continue;

          let shouldTimeout = false;
          try {
            const rankReq = new Request(`https://internal/api/ranks/lol/${encodeURIComponent(login)}`);
            const rankRes = await this.env.RANK_WORKER.fetch(rankReq);
            let rank: any = null;
            if (rankRes.ok) {
              rank = await rankRes.json();
              console.log('[Rank] found', { user: login, tier: rank?.rank_tier, division: rank?.rank_division });
            } else {
              console.log('[Rank] not found or error', { user: login, status: rankRes.status });
            }
            const mode = (cfg.enforcement_mode || 'has_rank') as 'has_rank' | 'min_rank';
            if (mode === 'has_rank') {
              shouldTimeout = !(rank && rank.rank_tier);
            } else {
              const threshold = {
                tier: String(cfg.min_rank_tier || '').toUpperCase(),
                division: Number(cfg.min_rank_division || 0),
              };
              shouldTimeout = threshold.tier ? !meetsMinRank(rank, threshold) : !(rank && rank.rank_tier);
            }
            console.log('[Enforce] decision', { channel: chanLogin, user: login, mode, shouldTimeout });
          } catch {}

          if (!shouldTimeout) continue;

          // Send timeout via IRC command
          const duration = cfg.timeout_seconds || 30;
          const reason = resolveReasonTemplate(this.env, cfg, login);
          await this.sleep(this.rateLimitSleepMs);
          console.log('[Enforce] timeout', { channel: chanLogin, user: login, duration });
          this.sendRaw(`PRIVMSG #${chanLogin} :.timeout ${login} ${duration} ${reason}`);
        } catch {
          // swallow
        }
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
    console.log('[IRC] starting keepalive');
    this.keepaliveInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === 1) {
        console.log('[IRC] sending keepalive PING');
        this.sendRaw('PING :tmi.twitch.tv');
      }
    }, 120000); // 2 minutes
  }

  stopKeepalive() {
    if (this.keepaliveInterval) {
      console.log('[IRC] stopping keepalive');
      clearInterval(this.keepaliveInterval);
      this.keepaliveInterval = null;
    }
  }
}

export { IrcClientShard };


