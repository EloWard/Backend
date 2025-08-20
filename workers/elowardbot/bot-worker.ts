/**
 * EloWard Bot Worker
 * - Handles OAuth for the EloWardBot account (moderation + chat scopes)
 * - Handles broadcaster grant of channel:bot to this app (for EventSub joins)
 * - Subscribes to channel.chat.message via EventSub (webhook transport)
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
  // HMAC secret for EventSub webhook verification
  EVENTSUB_SECRET: string;
  // Optional: KV for tokens and config
  BOT_KV: KVNamespace;
  // Service binding to ranks worker
  RANK_WORKER: Fetcher;
  // Durable Objects
  BOT_MANAGER: DurableObjectNamespace;
  IRC_SHARD: DurableObjectNamespace;
  // D1 database
  DB: D1Database;
  // Optional site base url for reasons
  SITE_BASE_URL?: string;
  // Internal write key for trusted calls from website backend
  INTERNAL_WRITE_KEY?: string;
  BOT_WRITE_KEY?: string;
}

const router = Router();

// Scopes
const BOT_SCOPES = [
  'user:write:chat',
  'moderator:manage:banned_users',
  'moderator:manage:chat_messages',
];
const BROADCASTER_SCOPES = ['channel:bot'];

// OAuth URLs
const AUTH_URL = 'https://id.twitch.tv/oauth2/authorize';
const TOKEN_URL = 'https://id.twitch.tv/oauth2/token';

// Helix endpoints
const HELIX_USERS = 'https://api.twitch.tv/helix/users';
const HELIX_BANS = 'https://api.twitch.tv/helix/moderation/bans';
const HELIX_EVENTSUB = 'https://api.twitch.tv/helix/eventsub/subscriptions';

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
async function getUserFromAnyUserToken(_env: Env, userAccessToken: string) {
  const res = await fetch('https://id.twitch.tv/oauth2/validate', {
    headers: { Authorization: `OAuth ${userAccessToken}` },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data?.user_id || !data?.login) {
    throw new Error('token validate failed');
  }
  return { id: String(data.user_id), login: String(data.login), display_name: String(data.login) } as { id: string; login: string; display_name: string };
}

async function sendTimeout(env: Env, botToken: string, broadcasterId: string, botUserId: string, targetUserId: string, seconds: number, reason: string) {
  const url = `${HELIX_BANS}?${toQuery({ broadcaster_id: broadcasterId, moderator_id: botUserId })}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${botToken}`,
      'Client-Id': env.TWITCH_CLIENT_ID,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ data: { user_id: targetUserId, duration: seconds, reason } }),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`timeout failed ${res.status}: ${t}`);
  }
}

async function verifyEventSubSignature(env: Env, req: Request, bodyText: string) {
  const id = req.headers.get('Twitch-Eventsub-Message-Id') || '';
  const ts = req.headers.get('Twitch-Eventsub-Message-Timestamp') || '';
  const sig = req.headers.get('Twitch-Eventsub-Message-Signature') || '';
  const msg = id + ts + bodyText;
  const keyData = new TextEncoder().encode(env.EVENTSUB_SECRET);
  const cryptoKey = await crypto.subtle.importKey('raw', keyData, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sigBuf = await crypto.subtle.sign('HMAC', cryptoKey, new TextEncoder().encode(msg));
  const hex = [...new Uint8Array(sigBuf)].map(b => b.toString(16).padStart(2, '0')).join('');
  const expectedSig = `sha256=${hex}`;
  return expectedSig === sig;
}

// CORS preflight
router.options('*', () => json(200, {}));

router.get('/health', () => json(200, { status: 'ok', service: 'eloward-bot' }));

// Start / reload IRC via Durable Objects
router.post('/irc/start', async (_req: Request, env: Env) => {
  try {
    const id = env.BOT_MANAGER.idFromName('manager');
    const res = await env.BOT_MANAGER.get(id).fetch('https://do/start', { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'start failed' });
  }
});

router.post('/irc/reload', async (_req: Request, env: Env) => {
  try {
    const id = env.BOT_MANAGER.idFromName('manager');
    const res = await env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    return json(res.status, data);
  } catch (e: any) {
    return json(500, { error: e?.message || 'reload failed' });
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
  const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, cooldown_seconds, enforcement_mode, min_rank_tier, min_rank_division
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
    cooldown_seconds = COALESCE(?, cooldown_seconds),
    updated_at = CURRENT_TIMESTAMP
    WHERE twitch_id = ?`;
  const result = await env.DB.prepare(upd).bind(
    login,
    to01(patch.bot_enabled),
    v(patch.timeout_seconds),
    v(patch.reason_template),
    v(patch.ignore_roles),
    v(patch.cooldown_seconds),
    twitchId
  ).run();
  if ((result as any)?.meta?.changes === 0) {
    const ins = `INSERT INTO twitch_bot_users (twitch_id, channel_name, bot_enabled, timeout_seconds, reason_template, ignore_roles, cooldown_seconds)
                 VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 30), COALESCE(?, "⏱️ {seconds}s timeout: link your EloWard rank at {site}"), COALESCE(?, "broadcaster,moderator,vip"), COALESCE(?, 60))`;
    await env.DB.prepare(ins).bind(
      twitchId,
      login,
      to01(patch.bot_enabled),
      v(patch.timeout_seconds),
      v(patch.reason_template),
      v(patch.ignore_roles),
      v(patch.cooldown_seconds)
    ).run();
  }
  return await getChannelConfig(env, login);
}

router.get('/bot/config/:login', async (req: Request, env: Env) => {
  try {
    const login = ((req as any).params?.login) || new URL(req.url).pathname.split('/').pop();
    const cfg = await getChannelConfig(env, String(login));
    if (!cfg) return json(404, { error: 'not found' });
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/config/:login', async (req: Request, env: Env) => {
  const bodyText = await req.text();
  const ok = await hmacValid(env, bodyText, req.headers.get('X-Signature'));
  if (!ok) return json(401, { error: 'invalid signature' });
  try {
    const login = ((req as any).params?.login) || new URL(req.url).pathname.split('/').pop();
    const patch = JSON.parse(bodyText || '{}');
    const cfg = await upsertChannelConfig(env, String(login), patch);
    // Instruct DO to reload
    const id = env.BOT_MANAGER.idFromName('manager');
    await env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' });
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/enable', async (req: Request, env: Env) => {
  const bodyText = await req.text();
  const ok = await hmacValid(env, bodyText, req.headers.get('X-Signature'));
  if (!ok) return json(401, { error: 'invalid signature' });
  try {
    const { channel_login } = JSON.parse(bodyText || '{}');
    if (!channel_login) return json(400, { error: 'channel_login required' });
    const cfg = await upsertChannelConfig(env, channel_login, { bot_enabled: 1 });
    const id = env.BOT_MANAGER.idFromName('manager');
    await env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' });
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

router.post('/bot/disable', async (req: Request, env: Env) => {
  const bodyText = await req.text();
  const ok = await hmacValid(env, bodyText, req.headers.get('X-Signature'));
  if (!ok) return json(401, { error: 'invalid signature' });
  try {
    const { channel_login } = JSON.parse(bodyText || '{}');
    if (!channel_login) return json(400, { error: 'channel_login required' });
    const cfg = await upsertChannelConfig(env, channel_login, { bot_enabled: 0 });
    const id = env.BOT_MANAGER.idFromName('manager');
    await env.BOT_MANAGER.get(id).fetch('https://do/reload', { method: 'POST' });
    return json(200, cfg);
  } catch (e: any) {
    return json(500, { error: e?.message || 'error' });
  }
});

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
    if (typeof body.cooldown_seconds === 'number') patch.cooldown_seconds = body.cooldown_seconds;
    if (typeof body.bot_enabled === 'number' || typeof body.bot_enabled === 'boolean') patch.bot_enabled = body.bot_enabled;
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
      // Broadcaster OAuth: auto-enable bot, subscribe EventSub, and warm cooldown shard
      try {
        await upsertChannelConfig(env, String(user.login).toLowerCase(), { bot_enabled: 1, twitch_id: user.id });
      } catch {}
      try {
        // Subscribe EventSub for this broadcaster
        const appToken = await getAppAccessToken(env);
        const callback = `${env.WORKER_PUBLIC_URL}/eventsub/callback`;
        const payload = {
          type: 'channel.chat.message',
          version: '1',
          condition: { broadcaster_user_id: user.id },
          transport: { method: 'webhook', callback, secret: env.EVENTSUB_SECRET },
        };
        await fetch(HELIX_EVENTSUB, {
          method: 'POST',
          headers: { Authorization: `Bearer ${appToken}`, 'Client-Id': env.TWITCH_CLIENT_ID, 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
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

// Create EventSub subscription for a channel (requires channel:bot grant or will count toward join limits)
router.post('/eventsub/subscribe', async (req: Request, env: Env) => {
  const body = await req.json().catch(() => ({} as any));
  const broadcaster_login = body.broadcaster_login as string;
  if (!broadcaster_login) return json(400, { error: 'broadcaster_login required' });
  try {
    const appToken = await getAppAccessToken(env);
    const user = await getUserByLogin(env, appToken, broadcaster_login);
    const callback = `${env.WORKER_PUBLIC_URL}/eventsub/callback`;
    const payload = {
      type: 'channel.chat.message',
      version: '1',
      // Subscribe to all chat messages for the broadcaster. Do not include user_id or you'll filter to a single chatter.
      condition: { broadcaster_user_id: user.id },
      transport: {
        method: 'webhook',
        callback,
        secret: env.EVENTSUB_SECRET,
      },
    };
    const res = await fetch(HELIX_EVENTSUB, {
      method: 'POST',
      headers: { Authorization: `Bearer ${appToken}`, 'Client-Id': env.TWITCH_CLIENT_ID, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) return json(res.status, { error: 'subscribe failed', details: data });
    return json(200, { success: true, subscription: data });
  } catch (e: any) {
    return json(500, { error: e?.message || 'subscribe error' });
  }
});

// EventSub webhook receiver
router.post('/eventsub/callback', async (req: Request, env: Env) => {
  const bodyText = await req.text();
  const msgType = req.headers.get('Twitch-Eventsub-Message-Type');

  // Verify signature
  try {
    const ok = await verifyEventSubSignature(env, req, bodyText);
    if (!ok) return new Response('signature mismatch', { status: 403 });
  } catch {
    // In dev, allow
  }

  const payload = JSON.parse(bodyText);
  if (msgType === 'webhook_callback_verification') {
    return new Response(payload.challenge, { status: 200 });
  }

  if (msgType === 'notification') {
    const evt = payload.event;
    if (payload.subscription?.type === 'channel.chat.message') {
      // Extract details
      const broadcasterId = evt.broadcaster_user_id as string;
      const chatterId = evt.chatter_user_id as string;
      const chatterLogin = (evt.chatter_user_login as string || '').toLowerCase();

      // Skip broadcaster/mods/VIPs if tags are present
      const badges: Array<{ set_id: string; id: string }>|undefined = evt.badges;
      const isPrivileged = badges?.some(b => ['moderator','broadcaster','vip'].includes(b.set_id)) || false;
      if (isPrivileged) return json(200, { ok: true });

      // Load channel config
      let channelConfig: any = null;
      try {
        channelConfig = await getChannelConfigByTwitchId(env, broadcasterId);
      } catch {}
      if (!channelConfig || !channelConfig.bot_enabled) return json(200, { ok: true });

      // Check EloWard rank DB & enforce mode
      let shouldTimeout = false;
      try {
        const rankReq = new Request(`https://ranks-worker/api/ranks/lol/${encodeURIComponent(chatterLogin)}`);
        const rankRes = await env.RANK_WORKER.fetch(rankReq);
        let rank: any = null;
        if (rankRes.ok) rank = await rankRes.json();

        const mode = (channelConfig.enforcement_mode || 'has_rank') as 'has_rank' | 'min_rank';
        if (mode === 'has_rank') {
          shouldTimeout = !(rank && rank.rank_tier);
        } else {
          // min_rank: compare chatter rank vs min threshold
          const threshold = {
            tier: String(channelConfig.min_rank_tier || '').toUpperCase(),
            division: Number(channelConfig.min_rank_division || 0)
          };
          if (!threshold.tier) {
            // fallback to has_rank if misconfigured
            shouldTimeout = !(rank && rank.rank_tier);
          } else {
            shouldTimeout = !meetsMinRank(rank, threshold);
          }
        }
      } catch {}

      if (shouldTimeout) {
        // Per-user cooldown via Durable Object
        try {
          const shardId = env.IRC_SHARD.idFromName(`cooldown:${broadcasterId}`);
          const res = await env.IRC_SHARD.get(shardId).fetch('https://do/cooldown/check', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              // Use a consistent key name expected by the DO for namespacing cooldowns
              channel_login: broadcasterId,
              user_login: chatterLogin,
              cooldown_seconds: channelConfig.cooldown_seconds || 60
            })
          });
          const cd = await res.json();
          if (cd && cd.allowed === false) {
            return json(200, { ok: true });
          }
        } catch {}
        // Timeout user via Helix
        try {
          const bot = await getBotUserAndToken(env);
          if (!bot?.access || !bot?.user?.id) throw new Error('bot not connected');
          const reason = resolveReasonTemplate(env, channelConfig, chatterLogin);
          const duration = channelConfig.timeout_seconds || 30;
          await sendTimeout(env, bot.access, broadcasterId, bot.user.id, chatterId, duration, reason);
        } catch (e) {
          // swallow errors to avoid retries causing spam
        }
      }
    }
    return json(200, { ok: true });
  }

  return json(200, { ok: true });
});

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
    const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, cooldown_seconds, enforcement_mode, min_rank_tier, min_rank_division
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
    const q = `SELECT channel_name AS channel_login, twitch_id, bot_enabled, timeout_seconds, reason_template, ignore_roles, cooldown_seconds, enforcement_mode, min_rank_tier, min_rank_division
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
  const tpl = channelConfig?.reason_template || 'Link your EloWard rank at {site}';
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
  const thrDiv = divisionToNumber(threshold.division);
  const userIdx = RANK_ORDER.indexOf(userTier as any);
  const thrIdx = RANK_ORDER.indexOf(thrTier as any);
  if (userIdx < 0 || thrIdx < 0) return false;
  if (userIdx > thrIdx) return true; // strictly higher tier
  if (userIdx < thrIdx) return false; // strictly lower tier
  // same tier: lower division number is higher (I=1 > IV=4)
  return userDiv <= thrDiv;
}

async function getBotUserAndToken(env: Env): Promise<{ access: string; refresh?: string; expires_at?: number; user?: { id: string; login: string } } | null> {
  try {
    const raw = (await env.BOT_KV.get('bot_tokens', 'json')) as any;
    if (!raw?.tokens?.access_token) return null;
    let access = raw.tokens.access_token as string;
    let refresh = raw.tokens.refresh_token as string | undefined;
    let exp = raw.tokens.expires_at as number | undefined;
    if (!exp || Date.now() > exp - 60_000) {
      if (refresh) {
        const nt = await refreshUserToken(env, refresh);
        access = nt.access_token;
        refresh = nt.refresh_token || refresh;
        exp = Date.now() + nt.expires_in * 1000;
        await env.BOT_KV.put('bot_tokens', JSON.stringify({ user: raw.user, tokens: { access_token: access, refresh_token: refresh, expires_at: exp } }));
      }
    }
    return { access, refresh, expires_at: exp, user: raw.user };
  } catch {
    return null;
  }
}

// (IRC helpers were removed; EventSub is the authoritative ingestion path.)

class BotManager {
  state: DurableObjectState;
  env: Env;
  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === 'POST' && (url.pathname === '/start' || url.pathname === '/reload')) {
      const channels = await listEnabledChannels(this.env);
      // Warm cooldown shards for current channels
      for (const ch of channels) {
        // Warm the same shard naming used during EventSub processing. Prefer twitch_id when available.
        const shardKey = String((ch as any).twitch_id || ch.channel_login);
        const id = this.env.IRC_SHARD.idFromName(`cooldown:${shardKey}`);
        await this.env.IRC_SHARD.get(id).fetch('https://do/warm', { method: 'POST' });
      }
      return json(200, { ok: true, channels: channels.length });
    }
    return json(404, { error: 'not found' });
  }
}

class IrcShard {
  state: DurableObjectState;
  env: Env;
  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === 'POST' && url.pathname === '/warm') {
      // no-op, ensures DO instance exists
      return json(200, { ok: true });
    }
    if (req.method === 'POST' && url.pathname === '/cooldown/check') {
      const body = await req.json().catch(() => ({}));
      const key = `${String(body.channel_login || '')}|${String(body.user_login || '')}`;
      const cooldownMs = Math.max(0, (Number(body.cooldown_seconds) || 60) * 1000);
      const last = (await this.state.storage.get(key)) as number | undefined;
      const now = Date.now();
      if (last && now - last < cooldownMs) {
        return json(200, { allowed: false, last });
      }
      await this.state.storage.put(key, now);
      return json(200, { allowed: true, last: last || 0 });
    }
    return json(404, { error: 'not found' });
  }
}

export { BotManager, IrcShard };


