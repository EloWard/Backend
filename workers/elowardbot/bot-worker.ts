/**
 * EloWard Bot Worker - Production Grade
 * Always-on Twitch IRC connection with immediate message processing
 * Optimized for high-volume chat monitoring and real-time timeouts
 */

import { Router } from 'itty-router';

// Minimal Cloudflare Workers types
type KVNamespace = {
  get: (key: string, type?: 'text' | 'json' | 'arrayBuffer' | 'stream') => Promise<any>;
  put: (key: string, value: string, options?: Record<string, any>) => Promise<void>;
};
type ExecutionContext = { waitUntil(promise: Promise<any>): void; passThroughOnException(): void };

interface Fetcher { fetch: (request: Request) => Promise<Response> }
interface DurableObjectState { 
  storage: { 
    get: (key: string) => Promise<any>; 
    put: (key: string, value: any) => Promise<void>; 
    delete: (key: string) => Promise<void> 
  }; 
  setAlarm?(when: number): void 
}
interface DurableObjectNamespace { 
  idFromName(name: string): any; 
  get(id: any): { fetch: (input: RequestInfo, init?: RequestInit) => Promise<Response> } 
}
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
  EVENTSUB_SECRET: string;
  BOT_KV: KVNamespace;
  RANK_WORKER: Fetcher;
  BOT_MANAGER: DurableObjectNamespace;
  IRC_CLIENT: DurableObjectNamespace;
  DB: D1Database;
  SITE_BASE_URL?: string;
  INTERNAL_WRITE_KEY?: string;
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

function log(msg: string, obj?: any): void {
  if (obj) {
    console.log(`(log) [${new Date().toISOString()}] ${msg}`, obj);
  } else {
    console.log(`(log) ${msg}`);
  }
}

// OAuth and token management (keeping existing logic)
const BOT_SCOPES = 'chat:read chat:edit moderator:manage:banned_users';

async function getBotUserAndToken(env: Env) {
  const startTime = Date.now();
  log('[getBotUserAndToken] START');

  try {
    const stored = await env.BOT_KV.get('bot_tokens', 'json');
    if (!stored) {
      log('[getBotUserAndToken] No stored bot tokens');
      return null;
    }

    log('[getBotUserAndToken] Raw stored data structure', { 
      hasTokensProperty: !!stored.tokens,
      hasDirectAccess: !!stored.access_token,
      hasUser: !!stored.user,
      keys: Object.keys(stored)
    });

    // Handle both storage formats: direct tokens or nested tokens
    let access_token, refresh_token, expires_at, user;
    
    if (stored.tokens) {
      // New format: { user: {...}, tokens: { access_token, refresh_token, expires_at } }
      access_token = stored.tokens.access_token;
      refresh_token = stored.tokens.refresh_token;
      expires_at = stored.tokens.expires_at;
      user = stored.user;
    } else {
      // Old format: { access_token, refresh_token, expires_at, user }
      access_token = stored.access_token;
      refresh_token = stored.refresh_token;
      expires_at = stored.expires_at;
      user = stored.user;
    }

    const now = Date.now();
    const expiresInMinutes = expires_at ? Math.floor((expires_at - now) / 60000) : 0;
    const needsRefresh = expires_at && now >= expires_at - 300000; // 5min buffer

    log('[getBotUserAndToken] Token info', {
      hasAccess: !!access_token,
      hasRefresh: !!refresh_token,
      expiresAt: expires_at,
      expiresInMinutes,
      needsRefresh,
      userLogin: user?.login
    });

    if (needsRefresh && refresh_token) {
      log('[getBotUserAndToken] Refreshing token');
      const refreshed = await refreshBotToken(env, refresh_token);
      if (refreshed) {
        const duration = Date.now() - startTime;
        log('[getBotUserAndToken] END - Refreshed', { duration, hasUser: !!refreshed.user, userLogin: refreshed.user?.login, tokenLength: refreshed.access?.length });
        return refreshed;
      }
    }

    const duration = Date.now() - startTime;
    log('[getBotUserAndToken] END - Success', { duration, hasUser: !!user, userLogin: user?.login, tokenLength: access_token?.length });
    return { access: access_token, refresh: refresh_token, user };
  } catch (e) {
    const duration = Date.now() - startTime;
    log('[getBotUserAndToken] END - Error', { duration, error: String(e) });
    return null;
  }
}

async function refreshBotToken(env: Env, refreshToken: string) {
  const startTime = Date.now();
  try {
    log('[refreshBotToken] Starting token refresh');
    
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
      log('[refreshBotToken] ❌ Token refresh failed', { 
        status: response.status, 
        error: errorText,
        duration: Date.now() - startTime 
      });
      return null;
    }

    const data = await response.json();
    const user = await validateToken(env, data.access_token);
    if (!user) {
      log('[refreshBotToken] ❌ Token validation failed');
      return null;
    }

    // Store in consistent nested format to match existing storage
    const tokenData = {
      user,
      tokens: {
        access_token: data.access_token,
        refresh_token: data.refresh_token || refreshToken, // Keep existing refresh token if not provided
        expires_at: Date.now() + data.expires_in * 1000,
      }
    };

    await env.BOT_KV.put('bot_tokens', JSON.stringify(tokenData));
    
    const duration = Date.now() - startTime;
    log('[refreshBotToken] ✅ Token refresh successful', { 
      tokenLength: data.access_token.length,
      expiresIn: data.expires_in,
      duration 
    });
    
    return { access: data.access_token, refresh: data.refresh_token || refreshToken, user };
  } catch (e) {
    const duration = Date.now() - startTime;
    log('[refreshBotToken] ❌ Token refresh crashed', { error: String(e), duration });
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

// Dashboard and config routes for frontend
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

// Main worker routes
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

// Dynamic channel management - no service interruption
router.post('/irc/channel/add', async (req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    const body = await req.json();
    const { channel_login, twitch_id } = body;
    
    if (!channel_login) {
      return json(400, { error: 'channel_login required' });
    }
    
    log('[Bot] Adding channel dynamically', { channel_login, twitch_id });
    
    const id = env.IRC_CLIENT.idFromName('irc:0');
    const response = await env.IRC_CLIENT.get(id).fetch('https://do/channel/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_login, twitch_id }),
    });
    
    const result = await response.json();
    return json(response.ok ? 200 : 500, result);
  } catch (e: any) {
    return json(500, { error: e?.message || 'add channel failed' });
  }
});

router.post('/irc/channel/remove', async (req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    const body = await req.json();
    const { channel_login } = body;
    
    if (!channel_login) {
      return json(400, { error: 'channel_login required' });
    }
    
    log('[Bot] Removing channel dynamically', { channel_login });
    
    const id = env.IRC_CLIENT.idFromName('irc:0');
    const response = await env.IRC_CLIENT.get(id).fetch('https://do/channel/remove', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_login }),
    });
    
    const result = await response.json();
    return json(response.ok ? 200 : 500, result);
  } catch (e: any) {
    return json(500, { error: e?.message || 'remove channel failed' });
  }
});

router.get('/irc/health', async (req: Request, env: Env) => {
  if (!env.IRC_CLIENT) {
    return json(500, { error: 'IRC_CLIENT not configured' });
  }

  const shardId = new URL(req.url).searchParams.get('shard') || '0';
  const id = env.IRC_CLIENT.idFromName(`irc:${shardId}`);
  const stub = env.IRC_CLIENT.get(id);
  
  try {
    const response = await stub.fetch('https://do/health');
    const health = await response.json();
    return json(200, { shard: shardId, ...health });
  } catch (e) {
    return json(500, { error: 'Health check failed', shard: shardId });
  }
});

// Main worker handler
// Allowed origins for CORS
const allowedOrigins = [
  'https://www.eloward.com',
  'https://eloward.com', 
  'http://localhost:3000'  // Development
];

// Helper function to generate CORS headers based on the request
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
};

export default worker;

// Production BotManager - simplified
export class BotManager {
  state: DurableObjectState;
  env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    
    if (req.method === 'POST' && url.pathname === '/reload') {
      return this.handleReload();
    }
    
    return json(404, { error: 'not found' });
  }

  async handleReload() {
    const startTime = Date.now();
    console.log('[BotManager] Reload initiated');
    
    try {
      // Get enabled channels
      const channels = await this.getEnabledChannels();
      console.log('[BotManager] Loaded channels', { count: channels.length });
      
      // Simple single-shard assignment for production
      const shardId = 0;
      const id = this.env.IRC_CLIENT!.idFromName(`irc:${shardId}`);
      const stub = this.env.IRC_CLIENT!.get(id);
      
      // Assign channels
      await stub.fetch('https://do/assign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ channels }),
      });
      
      // Establish connection
      await stub.fetch('https://do/connect', { method: 'POST' });
      
      const duration = Date.now() - startTime;
      console.log('[BotManager] Reload completed', { channels: channels.length, duration });
      
      return json(200, { ok: true, channels: channels.length, duration });
    } catch (e) {
      console.error('[BotManager] Reload failed:', e);
      return json(500, { error: String(e) });
    }
  }

  async getEnabledChannels() {
    const result = await this.env.DB.prepare(`
      SELECT channel_name as channel_login, twitch_id 
      FROM twitch_bot_users 
      WHERE bot_enabled = 1
    `).all();
    
    return result.results || [];
  }
}

// Production IRC Client - always-on, optimized
export class IrcClientShard {
  state: DurableObjectState;
  env: Env;
  
  // Core connection
  ws: WebSocket | null = null;
  connecting = false;
  ready = false;
  connectionStartTime = 0;
  
  // Bot identity  
  botLogin: string | null = null;
  botToken: string | null = null;
  
  // Channel management
  assignedChannels: Array<{ channel_login: string; twitch_id?: string | null }> = [];
  channelSet: Set<string> = new Set();
  channelIdByLogin: Map<string, string> = new Map();
  modChannels: Set<string> = new Set();
  
  // Connection management
  keepaliveInterval: any = null;
  reconnectAttempts = 0;
  maxReconnectAttempts = 5;
  
  // Production metrics
  messagesProcessed = 0;
  timeoutsIssued = 0;
  timeoutsFailed = 0;
  lastActivity = 0;
  
  // Circuit breaker for timeout operations
  timeoutFailures = 0;
  lastTimeoutFailure = 0;
  timeoutCircuitOpen = false;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    
    // Restore state
    this.reloadStateFromStorage().catch(console.error);
    
    // Set 5-minute maintenance alarm
    if (typeof this.state.setAlarm === 'function') {
      try { 
        this.state.setAlarm(Date.now() + 300_000);
      } catch (e) {
        console.error('[IRC] Failed to set alarm:', e);
      }
    }
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    
    if (req.method === 'POST' && url.pathname === '/assign') {
      return this.handleAssign(req);
    }
    
    if (req.method === 'POST' && url.pathname === '/connect') {
      return this.handleConnect();
    }
    
    if (req.method === 'GET' && url.pathname === '/health') {
      return this.handleHealthCheck();
    }
    
    if (req.method === 'POST' && url.pathname === '/channel/add') {
      return this.handleAddChannel(req);
    }
    
    if (req.method === 'POST' && url.pathname === '/channel/remove') {
      return this.handleRemoveChannel(req);
    }
    
    return json(404, { error: 'not found' });
  }

  async handleAssign(req: Request) {
    try {
      const body = await req.json();
      this.assignedChannels = body.channels || [];
      this.channelSet = new Set(this.assignedChannels.map((c: any) => `#${c.channel_login}`));
      
      // Build channel ID mapping
      this.channelIdByLogin.clear();
      for (const c of this.assignedChannels) {
        if (c.twitch_id) {
          this.channelIdByLogin.set(c.channel_login, String(c.twitch_id));
        }
      }
      
      // Save to storage
      await this.saveStateToStorage();
      
      log('[IRC] Channels assigned', { count: this.assignedChannels.length });
      return json(200, { assigned: this.assignedChannels.length });
    } catch (e) {
      console.error('[IRC] Assign failed:', e);
      return json(500, { error: String(e) });
    }
  }

  async handleConnect() {
    try {
      await this.connectToTwitch();
      return json(200, { 
        connected: true, 
        channels: this.assignedChannels.length,
        timestamp: Date.now() 
      });
    } catch (e) {
      console.error('[IRC] Connection failed:', e);
      return json(500, { 
        error: 'Connection failed', 
        message: String(e) 
      });
    }
  }

  async handleHealthCheck() {
    const now = Date.now();
    const connectionAge = this.connectionStartTime ? now - this.connectionStartTime : 0;
    
    return json(200, {
      connected: !!this.ws && this.ws.readyState === WebSocket.OPEN,
      ready: this.ready,
      channels: this.assignedChannels.length,
      modChannels: this.modChannels.size,
      connectionAge,
      messagesProcessed: this.messagesProcessed,
      timeoutsIssued: this.timeoutsIssued,
      lastActivity: this.lastActivity,
      reconnectAttempts: this.reconnectAttempts
    });
  }

  // Dynamic channel addition - zero downtime
  async handleAddChannel(req: Request) {
    try {
      const body = await req.json();
      const { channel_login, twitch_id } = body;
      
      if (!channel_login) {
        return json(400, { error: 'channel_login required' });
      }
      
      // Check if already assigned
      const existing = this.assignedChannels.find(c => c.channel_login === channel_login);
      if (existing) {
        return json(200, { message: 'Channel already assigned', channel: channel_login });
      }
      
      // Add to assigned channels
      this.assignedChannels.push({ channel_login, twitch_id });
      this.channelSet.add(`#${channel_login}`);
      
      if (twitch_id) {
        this.channelIdByLogin.set(channel_login, String(twitch_id));
      }
      
      // Join immediately if connected
      if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.sendRaw(`JOIN #${channel_login}`);
        log('[IRC] Dynamically joined channel', { channel: channel_login });
      }
      
      // Save state
      await this.saveStateToStorage();
      
      return json(200, { 
        message: 'Channel added successfully', 
        channel: channel_login,
        totalChannels: this.assignedChannels.length 
      });
    } catch (e) {
      console.error('[IRC] Add channel failed:', e);
      return json(500, { error: String(e) });
    }
  }

  // Dynamic channel removal - zero downtime  
  async handleRemoveChannel(req: Request) {
    try {
      const body = await req.json();
      const { channel_login } = body;
      
      if (!channel_login) {
        return json(400, { error: 'channel_login required' });
      }
      
      // Remove from assigned channels
      this.assignedChannels = this.assignedChannels.filter(c => c.channel_login !== channel_login);
      this.channelSet.delete(`#${channel_login}`);
      this.channelIdByLogin.delete(channel_login);
      this.modChannels.delete(`#${channel_login}`);
      
      // Leave immediately if connected
      if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.sendRaw(`PART #${channel_login}`);
        log('[IRC] Dynamically left channel', { channel: channel_login });
      }
      
      // Save state
      await this.saveStateToStorage();
      
      return json(200, { 
        message: 'Channel removed successfully', 
        channel: channel_login,
        totalChannels: this.assignedChannels.length 
      });
    } catch (e) {
      console.error('[IRC] Remove channel failed:', e);
      return json(500, { error: String(e) });
    }
  }

  // Direct Twitch IRC connection with robust error handling
  async connectToTwitch() {
    if (this.connecting || (this.ws && this.ws.readyState === WebSocket.OPEN)) {
      log('[IRC] Connection already in progress or established');
      return;
    }
    
    try {
      const bot = await getBotUserAndToken(this.env);
      if (!bot?.user?.login || !bot?.access) {
        throw new Error('Bot credentials not available');
      }
      
      this.botLogin = String(bot.user.login).toLowerCase();
      this.botToken = bot.access;
      this.connecting = true;
      this.connectionStartTime = Date.now();
      
      log('[IRC] Connecting to Twitch IRC', { 
        login: this.botLogin,
        tokenLength: this.botToken?.length || 0 
      });
      
      // Add connection timeout
      const connectionTimeout = setTimeout(() => {
        if (this.connecting) {
          log('[IRC] ❌ Connection timeout after 10 seconds');
          this.handleDisconnection();
        }
      }, 10000);
      
      this.ws = new WebSocket('wss://irc-ws.chat.twitch.tv:443');
      
      this.ws.addEventListener('open', () => {
        clearTimeout(connectionTimeout);
        log('[IRC] ✅ WebSocket connected', { 
          login: this.botLogin,
          connectionTime: Date.now() - this.connectionStartTime 
        });
        
        // Authenticate with error handling
        try {
          const authToken = this.botToken!.startsWith('oauth:') ? this.botToken : `oauth:${this.botToken}`;
          this.sendRaw(`PASS ${authToken}`);
          this.sendRaw(`NICK ${this.botLogin}`);
          this.sendRaw('CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership');
          
          this.connecting = false;
          this.reconnectAttempts = 0;
          this.startKeepalive();
        } catch (authError) {
          log('[IRC] ❌ Authentication failed', { error: String(authError) });
          this.handleDisconnection();
        }
      });
      
      this.ws.addEventListener('message', (event) => {
        try {
          this.lastActivity = Date.now();
          this.messagesProcessed++;
          const messageData = String(event.data || '');
          if (messageData) {
            this.handleIrcMessage(messageData);
          }
        } catch (msgError) {
          log('[IRC] ❌ Message handling error', { error: String(msgError) });
        }
      });
      
      this.ws.addEventListener('close', (event) => {
        clearTimeout(connectionTimeout);
        log('[IRC] Connection closed', { 
          code: event.code, 
          reason: event.reason || 'No reason provided',
          wasClean: event.wasClean,
          connectionAge: Date.now() - this.connectionStartTime
        });
        this.handleDisconnection();
      });
      
      this.ws.addEventListener('error', (event) => {
        clearTimeout(connectionTimeout);
        log('[IRC] ❌ WebSocket error', { 
          error: String(event),
          readyState: this.ws?.readyState 
        });
        this.handleDisconnection();
      });
      
    } catch (e) {
      this.connecting = false;
      log('[IRC] ❌ Connection setup failed', { error: String(e) });
      throw e;
    }
  }

  sendRaw(line: string) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      log('[IRC] ❌ Cannot send - WebSocket not ready', { 
        hasWebSocket: !!this.ws,
        readyState: this.ws?.readyState,
        line: line.substring(0, 50) + (line.length > 50 ? '...' : '')
      });
      return;
    }
    
    try {
      this.ws.send(line);
      // Only log non-PING/PONG messages to reduce noise
      if (!line.startsWith('PING') && !line.startsWith('PONG')) {
        log('[IRC] >> SENT', { line });
      }
    } catch (e) {
      log('[IRC] ❌ Send failed', { line, error: String(e) });
    }
  }

  startKeepalive() {
    if (this.keepaliveInterval) {
      clearInterval(this.keepaliveInterval);
    }
    
    this.keepaliveInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.sendRaw('PING :keepalive');
      }
    }, 30000); // 30 seconds
  }

  handleDisconnection() {
    log('[IRC] Handling disconnection', { 
      wasReady: this.ready,
      wasConnecting: this.connecting,
      reconnectAttempts: this.reconnectAttempts,
      maxAttempts: this.maxReconnectAttempts
    });
    
    this.ready = false;
    this.connecting = false;
    
    if (this.keepaliveInterval) {
      clearInterval(this.keepaliveInterval);
      this.keepaliveInterval = null;
      log('[IRC] Keepalive interval cleared');
    }
    
    // Reconnect with exponential backoff
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      this.reconnectAttempts++;
      
      log('[IRC] Scheduling reconnection', { 
        attempt: this.reconnectAttempts,
        delayMs: delay,
        maxAttempts: this.maxReconnectAttempts 
      });
      
      setTimeout(() => {
        log('[IRC] Attempting reconnection', { attempt: this.reconnectAttempts });
        this.connectToTwitch().catch((e) => {
          log('[IRC] ❌ Reconnection failed', { attempt: this.reconnectAttempts, error: String(e) });
        });
      }, delay);
    } else {
      log('[IRC] ❌ Max reconnection attempts reached - giving up', { 
        maxAttempts: this.maxReconnectAttempts 
      });
    }
  }

  // IRC message handling with detailed logging
  handleIrcMessage(data: string) {
    // Handle both \n and \r\n line endings properly
    const lines = data.trim().split(/\r?\n/).filter(line => line.length > 0);
    
    for (const line of lines) {
      // Handle PING immediately
      if (line.startsWith('PING ')) {
        const pongResponse = line.replace('PING', 'PONG');
        this.sendRaw(pongResponse);
        log('[IRC] PING/PONG keepalive', { ping: line.trim(), pong: pongResponse });
        continue;
      }
      
      // Parse IRC message
      const msg = this.parseIrc(line.trim());
      if (!msg) {
        log('[IRC] Failed to parse message', { rawLine: line.substring(0, 100) + (line.length > 100 ? '...' : '') });
        continue;
      }
      
      // Log all IRC messages for debugging
      log('[IRC] Message received', { 
        command: msg.command, 
        channel: msg.params[0], 
        user: msg.prefix?.split('!')[0],
        tags: msg.tags ? Object.keys(msg.tags) : []
      });
      
      // Handle authentication errors first
      if (msg.command === 'NOTICE' && msg.params?.[1]?.includes('Login unsuccessful')) {
        log('[IRC] ❌ Authentication failed', { notice: msg.params?.[1] });
        this.handleDisconnection();
        continue;
      }
      if (msg.command === '464') { // ERR_PASSWDMISMATCH
        log('[IRC] ❌ Password mismatch error');
        this.handleDisconnection();
        continue;
      }
      
      // Mark ready on welcome or global user state
      if (msg.command === '001' || msg.command === 'GLOBALUSERSTATE') {
        if (!this.ready) {
          this.ready = true;
          this.reconnectAttempts = 0; // Reset reconnection counter on successful connection
          log('[IRC] ✅ Authentication successful - bot is ready', { 
            botLogin: this.botLogin,
            connectionTime: Date.now() - this.connectionStartTime,
            assignedChannels: this.assignedChannels.length
          });
          this.joinChannels();
        }
        // Ensure echo-message capability so we receive our own PRIVMSG (helps testing)
        this.sendRaw('CAP REQ :twitch.tv/echo-message');
        continue;
      } else if (msg.command === 'JOIN' && msg.prefix?.includes(this.botLogin!)) {
        // Bot successfully joined a channel
        const channel = msg.params[0];
        log('[IRC] ✅ Successfully joined channel', { 
          channel, 
          totalChannels: this.channelSet.size,
          botLogin: this.botLogin 
        });
      } else if (msg.command === 'USERSTATE') {
        // Check mod permissions when bot joins or sends messages
        const channel = msg.params[0];
        const isMod = msg.tags?.mod === '1';
        const wasMod = this.modChannels.has(channel);
        
        if (isMod && !wasMod) {
          this.modChannels.add(channel);
          log('[IRC] ✅ Gained mod permissions', { channel, totalModChannels: this.modChannels.size });
        } else if (!isMod && wasMod) {
          this.modChannels.delete(channel);
          log('[IRC] ❌ Lost mod permissions', { channel, totalModChannels: this.modChannels.size });
        } else if (isMod) {
          log('[IRC] Confirmed mod permissions', { channel });
        } else {
          log('[IRC] ⚠️ No mod permissions in channel', { channel });
        }
        
        this.saveStateToStorage();
      } else if (msg.command === 'PRIVMSG') {
        // Handle chat message - this is where timeouts happen
        this.handlePrivmsg(msg);
      } else if (msg.command === 'NOTICE') {
        // Handle Twitch notices (errors, confirmations)
        const channel = msg.params[0];
        const notice = msg.params[1];
        const msgId = msg.tags?.['msg-id'];
        log('[IRC] Twitch notice received', { channel, notice, msgId });
      }
    }
  }

  async handlePrivmsg(msg: any) {
    const channel = msg.params[0];
    const message = msg.params[1];
    const user = msg.prefix?.split('!')[0];
    
    if (!user || !channel) {
      log('[PRIVMSG] ❌ Invalid message format', { hasUser: !!user, hasChannel: !!channel });
      return;
    }
    
    const chanLogin = channel.replace('#', '');
    log('[PRIVMSG] Processing message', { 
      channel: chanLogin, 
      user, 
      messageLength: message?.length || 0,
      timestamp: new Date().toISOString()
    });
    
    // Skip privileged users
    const badges = (msg.tags?.badges || '').split(',').map((b: string) => b.split('/')[0]);
    const isPrivileged = badges.some((b: string) => ['broadcaster', 'moderator', 'vip'].includes(b));
    
    if (isPrivileged) {
      log('[PRIVMSG] ⏭️ Skipping privileged user', { user, badges, channel: chanLogin });
      return;
    }
    
    // Check if bot has mod permissions
    if (!this.modChannels.has(channel)) {
      log('[PRIVMSG] ⚠️ No mod permissions in channel', { 
        channel: chanLogin, 
        user,
        modChannels: Array.from(this.modChannels) 
      });
      return;
    }
    
    // Get channel config
    log('[PRIVMSG] Checking channel configuration', { channel: chanLogin });
    const config = await this.getChannelConfig(chanLogin);
    if (!config) {
      log('[PRIVMSG] ❌ Channel config not found', { channel: chanLogin });
      return;
    }
    if (!config.bot_enabled) {
      log('[PRIVMSG] ⚠️ Bot disabled in channel config', { channel: chanLogin });
      return;
    }
    
    log('[PRIVMSG] ✅ Channel config valid', { 
      channel: chanLogin,
      timeout_seconds: config.timeout_seconds,
      has_reason_template: !!config.reason_template 
    });
    
    // Check rank
    log('[PRIVMSG] Checking user rank', { user, channel: chanLogin });
    const hasRank = await this.checkUserRank(user);
    if (hasRank) {
      log('[PRIVMSG] ✅ User has valid rank - no timeout needed', { user, channel: chanLogin });
      return;
    }
    
    log('[PRIVMSG] ❌ User lacks required rank - issuing timeout', { user, channel: chanLogin });
    
    // Issue timeout
    await this.timeoutUser(chanLogin, user, config.timeout_seconds || 20, config.reason_template);
  }

  async checkUserRank(username: string): Promise<boolean> {
    const startTime = Date.now();
    try {
      log('[RANK] Checking user rank', { username });
      const response = await this.env.RANK_WORKER.fetch(new Request(`https://internal/api/ranks/lol/${username}`));
      const duration = Date.now() - startTime;
      
      if (response.ok) {
        log('[RANK] ✅ User has valid rank', { username, status: response.status, duration });
        return true;
      } else {
        log('[RANK] ❌ User lacks required rank', { username, status: response.status, duration });
        return false;
      }
    } catch (e) {
      const duration = Date.now() - startTime;
      log('[RANK] ❌ Rank check failed with error', { username, error: String(e), duration });
      return false;
    }
  }

  async getChannelConfig(channelLogin: string) {
    const startTime = Date.now();
    try {
      log('[CONFIG] Fetching channel configuration', { channel: channelLogin });
      const result = await this.env.DB.prepare(`
        SELECT * FROM twitch_bot_users 
        WHERE channel_name = ? AND bot_enabled = 1
      `).bind(channelLogin).first();
      
      const duration = Date.now() - startTime;
      
      if (result) {
        log('[CONFIG] ✅ Channel config found', { 
          channel: channelLogin,
          bot_enabled: result.bot_enabled,
          timeout_seconds: result.timeout_seconds,
          has_reason_template: !!result.reason_template,
          duration 
        });
      } else {
        log('[CONFIG] ❌ Channel config not found or disabled', { channel: channelLogin, duration });
      }
      
      return result;
    } catch (e) {
      const duration = Date.now() - startTime;
      log('[CONFIG] ❌ Database query failed', { channel: channelLogin, error: String(e), duration });
      return null;
    }
  }

  async timeoutUser(channelLogin: string, userLogin: string, duration: number, reason: string) {
    const startTime = Date.now();
    const timeoutId = `${channelLogin}:${userLogin}:${Date.now()}`;
    
    // Circuit breaker: if too many recent failures, skip timeout
    if (this.timeoutCircuitOpen) {
      const timeSinceLastFailure = Date.now() - this.lastTimeoutFailure;
      if (timeSinceLastFailure < 60000) { // 1 minute circuit breaker
        log('[TIMEOUT] ⚡ Circuit breaker OPEN - skipping timeout', { 
          timeoutId,
          failures: this.timeoutFailures,
          timeSinceLastFailure 
        });
        return;
      } else {
        // Reset circuit breaker after cooldown
        this.timeoutCircuitOpen = false;
        this.timeoutFailures = 0;
        log('[TIMEOUT] ⚡ Circuit breaker RESET', { timeoutId });
      }
    }
    
    try {
      log('[TIMEOUT] Starting timeout process', { 
        timeoutId, 
        channel: channelLogin, 
        user: userLogin, 
        duration, 
        hasReason: !!reason 
      });
      
      // Get bot credentials
      const bot = await getBotUserAndToken(this.env);
      if (!bot?.access) {
        log('[TIMEOUT] ❌ No bot access token available', { timeoutId });
        return;
      }
      if (!bot?.user?.id) {
        log('[TIMEOUT] ❌ No bot user ID available', { timeoutId });
        return;
      }
      
      log('[TIMEOUT] ✅ Bot credentials obtained', { 
        timeoutId, 
        botUserId: bot.user.id,
        tokenLength: bot.access.length 
      });
      
      // Get channel ID
      const channelId = this.channelIdByLogin.get(channelLogin);
      if (!channelId) {
        log('[TIMEOUT] ❌ Channel ID not found', { 
          timeoutId, 
          channel: channelLogin,
          availableChannels: Array.from(this.channelIdByLogin.keys()) 
        });
        return;
      }
      
      log('[TIMEOUT] ✅ Channel ID found', { timeoutId, channelId });
      
      // Get user ID from Twitch API
      log('[TIMEOUT] Fetching user ID from Twitch API', { timeoutId, user: userLogin });
      const userResponse = await fetch(`https://api.twitch.tv/helix/users?login=${userLogin}`, {
        headers: {
          'Authorization': `Bearer ${bot.access}`,
          'Client-Id': this.env.TWITCH_CLIENT_ID,
        },
      });
      
      if (!userResponse.ok) {
        const errorText = await userResponse.text();
        log('[TIMEOUT] ❌ Failed to fetch user ID', { 
          timeoutId, 
          user: userLogin, 
          status: userResponse.status,
          error: errorText 
        });
        return;
      }
      
      const userData = await userResponse.json();
      const userId = userData.data?.[0]?.id;
      if (!userId) {
        log('[TIMEOUT] ❌ User not found in Twitch API response', { 
          timeoutId, 
          user: userLogin, 
          responseData: userData 
        });
        return;
      }
      
      log('[TIMEOUT] ✅ User ID obtained', { timeoutId, user: userLogin, userId });
      
      // Issue timeout via Helix API
      const timeoutReason = reason || `${duration}s timeout: not enough elo to speak. Link your EloWard at https://www.eloward.com`;
      const timeoutUrl = `https://api.twitch.tv/helix/moderation/bans?broadcaster_id=${channelId}&moderator_id=${bot.user.id}`;
      
      log('[TIMEOUT] Issuing timeout via Helix API', { 
        timeoutId, 
        url: timeoutUrl,
        userId, 
        duration, 
        reason: timeoutReason 
      });
      
      const timeoutResponse = await fetch(timeoutUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${bot.access}`,
          'Client-Id': this.env.TWITCH_CLIENT_ID,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          data: {
            user_id: userId,
            duration,
            reason: timeoutReason,
          },
        }),
      });
      
      const totalDuration = Date.now() - startTime;
      
      if (timeoutResponse.ok) {
        this.timeoutsIssued++;
        const responseData = await timeoutResponse.json();
        log('[TIMEOUT] ✅ Timeout successful', { 
          timeoutId,
          channel: channelLogin, 
          user: userLogin, 
          userId,
          duration,
          totalTimeouts: this.timeoutsIssued,
          processingTime: totalDuration,
          responseData
        });
      } else {
        const errorText = await timeoutResponse.text();
        this.timeoutsFailed++;
        this.timeoutFailures++;
        this.lastTimeoutFailure = Date.now();
        
        // Open circuit breaker if too many failures
        if (this.timeoutFailures >= 5) {
          this.timeoutCircuitOpen = true;
          log('[TIMEOUT] ⚡ Circuit breaker OPENED due to failures', { failures: this.timeoutFailures });
        }
        
        log('[TIMEOUT] ❌ Timeout failed', { 
          timeoutId,
          channel: channelLogin, 
          user: userLogin, 
          userId,
          status: timeoutResponse.status,
          error: errorText,
          processingTime: totalDuration,
          totalFailures: this.timeoutsFailed
        });
      }
    } catch (e) {
      const totalDuration = Date.now() - startTime;
      this.timeoutsFailed++;
      this.timeoutFailures++;
      this.lastTimeoutFailure = Date.now();
      
      // Open circuit breaker if too many failures
      if (this.timeoutFailures >= 5) {
        this.timeoutCircuitOpen = true;
        log('[TIMEOUT] ⚡ Circuit breaker OPENED due to crashes', { failures: this.timeoutFailures });
      }
      
      log('[TIMEOUT] ❌ Timeout process crashed', { 
        timeoutId,
        channel: channelLogin, 
        user: userLogin, 
        error: String(e),
        processingTime: totalDuration,
        totalFailures: this.timeoutsFailed
      });
    }
  }

  joinChannels() {
    log('[IRC] Joining all assigned channels', { 
      channelCount: this.channelSet.size,
      channels: Array.from(this.channelSet) 
    });
    
    for (const channel of this.channelSet) {
      this.sendRaw(`JOIN ${channel}`);
      log('[IRC] Sent JOIN command', { channel });
    }
  }

  parseIrc(line: string): { tags?: Record<string, string>; prefix?: string; command: string; params: string[] } | null {
    let rest = line;
    const msg: any = { params: [] };
    
    // Parse tags (@key=value;key2=value2)
    if (rest.startsWith('@')) {
      const sp = rest.indexOf(' ');
      if (sp === -1) return null; // Invalid format
      const rawTags = rest.slice(1, sp);
      rest = rest.slice(sp + 1);
      msg.tags = {};
      for (const part of rawTags.split(';')) {
        const [k, v] = part.split('=');
        msg.tags[k] = v || '';
      }
    }
    
    // Parse prefix (:nick!user@host)
    if (rest.startsWith(':')) {
      const sp = rest.indexOf(' ');
      if (sp === -1) return null; // Invalid format
      msg.prefix = rest.slice(1, sp);
      rest = rest.slice(sp + 1);
    }
    
    // Parse command
    const sp = rest.indexOf(' ');
    if (sp === -1) {
      // Command only, no params
      msg.command = rest;
      return msg;
    }
    
    msg.command = rest.slice(0, sp);
    rest = rest.slice(sp + 1);
    
    // Parse parameters
    if (rest.startsWith(':')) {
      // Trailing parameter (everything after :)
      msg.params = [rest.slice(1)];
    } else {
      // Multiple parameters
      const parts: string[] = [];
      while (rest) {
        if (rest.startsWith(':')) {
          // Trailing parameter found
          parts.push(rest.slice(1));
          break;
        }
        const idx = rest.indexOf(' ');
        if (idx === -1) {
          // Last parameter
          parts.push(rest);
          break;
        }
        parts.push(rest.slice(0, idx));
        rest = rest.slice(idx + 1);
      }
      msg.params = parts;
    }
    
    return msg;
  }

  async saveStateToStorage() {
    const startTime = Date.now();
    try {
      await Promise.all([
        this.state.storage.put('assigned', this.assignedChannels),
        this.state.storage.put('modChannels', Array.from(this.modChannels))
      ]);
      
      const duration = Date.now() - startTime;
      log('[IRC] ✅ State saved to storage', { 
        channels: this.assignedChannels.length,
        modChannels: this.modChannels.size,
        duration 
      });
    } catch (e) {
      const duration = Date.now() - startTime;
      log('[IRC] ❌ Save state failed', { error: String(e), duration });
      // Don't throw - this is not critical for operation
    }
  }

  async reloadStateFromStorage() {
    try {
      const stored = await this.state.storage.get('assigned') as any[];
      if (stored) {
        this.assignedChannels = stored;
        this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
        this.channelIdByLogin.clear();
        for (const c of this.assignedChannels) {
          if (c.twitch_id) {
            this.channelIdByLogin.set(c.channel_login, String(c.twitch_id));
          }
        }
      }
      
      const modChannels = await this.state.storage.get('modChannels') as string[];
      if (modChannels) {
        this.modChannels = new Set(modChannels);
      }
    } catch (e) {
      console.error('[IRC] Reload state failed:', e);
    }
  }

  // Automatic channel discovery - sync with database every 5 minutes
  async syncChannelsFromDatabase() {
    try {
      const result = await this.env.DB.prepare(`
        SELECT channel_name as channel_login, twitch_id 
        FROM twitch_bot_users 
        WHERE bot_enabled = 1
      `).all();
      
      const dbChannels = result.results || [];
      const currentChannels = new Set(this.assignedChannels.map(c => c.channel_login));
      
      // Find new channels to add
      const newChannels = dbChannels.filter(c => !currentChannels.has(c.channel_login));
      
      // Find channels to remove (disabled in database)
      const dbChannelSet = new Set(dbChannels.map(c => c.channel_login));
      const channelsToRemove = this.assignedChannels.filter(c => !dbChannelSet.has(c.channel_login));
      
      // Add new channels
      for (const channel of newChannels) {
        this.assignedChannels.push(channel);
        this.channelSet.add(`#${channel.channel_login}`);
        
        if (channel.twitch_id) {
          this.channelIdByLogin.set(channel.channel_login, String(channel.twitch_id));
        }
        
        // Join immediately if connected
        if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.sendRaw(`JOIN #${channel.channel_login}`);
          log('[IRC] Auto-joined new channel', { channel: channel.channel_login });
        }
      }
      
      // Remove disabled channels
      for (const channel of channelsToRemove) {
        this.assignedChannels = this.assignedChannels.filter(c => c.channel_login !== channel.channel_login);
        this.channelSet.delete(`#${channel.channel_login}`);
        this.channelIdByLogin.delete(channel.channel_login);
        this.modChannels.delete(`#${channel.channel_login}`);
        
        // Leave immediately if connected
        if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.sendRaw(`PART #${channel.channel_login}`);
          log('[IRC] Auto-left disabled channel', { channel: channel.channel_login });
        }
      }
      
      // Save state if changes were made
      if (newChannels.length > 0 || channelsToRemove.length > 0) {
        await this.saveStateToStorage();
        log('[IRC] Channel sync completed', { 
          added: newChannels.length, 
          removed: channelsToRemove.length,
          total: this.assignedChannels.length 
        });
      }
    } catch (e) {
      console.error('[IRC] Channel sync failed:', e);
    }
  }

  // Maintenance alarm
  async alarm() {
    log('[IRC] Maintenance check');
    
    await this.reloadStateFromStorage();
    
    // Auto-sync new channels from database (zero-downtime channel discovery)
    await this.syncChannelsFromDatabase();
    
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      try {
        await this.connectToTwitch();
      } catch (e) {
        console.error('[IRC] Maintenance reconnect failed:', e);
      }
    } else if (this.ready && this.assignedChannels.length > 0) {
      this.joinChannels();
    }
    
    // Re-arm alarm
    if (typeof this.state.setAlarm === 'function') {
      try { 
        this.state.setAlarm(Date.now() + 300_000); // 5 minutes
      } catch (e) {
        console.error('[IRC] Failed to set alarm:', e);
      }
    }
  }
}
