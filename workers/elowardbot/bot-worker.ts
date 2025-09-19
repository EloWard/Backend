/**
 * EloWard Twitch Bot - Production Grade Single-DO Architecture
 * 
 * CRITICAL: Removed all hibernation complexity for always-on reliability
 * - Single Durable Object with direct IRC connection
 * - Immediate message processing (no forwarding/proxying)  
 * - Production-grade error handling and auto-recovery
 * - Zero-downtime channel management
 */

import { Router } from 'itty-router';

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
  setAlarm?(when: number): void;
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
  BOT_KV: KVNamespace;
  RANK_WORKER: Fetcher;
  TWITCH_BOT: DurableObjectNamespace;  // Single DO binding
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

function log(msg: string, obj?: any): void {
  if (obj) {
    console.log(`(log) [${new Date().toISOString()}] ${msg}`, obj);
  } else {
    console.log(`(log) ${msg}`);
  }
}

// Token management
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
    const expiresInMinutes = expires_at ? Math.floor((expires_at - now) / 60000) : 0;
    const needsRefresh = expires_at && now >= expires_at - 300000; // 5min buffer

    log('[getBotUserAndToken] Token info', {
      hasAccess: !!access_token,
      hasRefresh: !!refresh_token,
      expiresAt: expires_at,
      expiresInMinutes,
      needsRefresh,
      userLogin: user?.login,
      userIdAvailable: !!user?.id,
      fullUserObject: user
    });

    if (needsRefresh && refresh_token) {
      log('[getBotUserAndToken] Refreshing token');
      const refreshed = await refreshBotToken(env, refresh_token);
      if (refreshed) {
        const duration = Date.now() - startTime;
        log('[getBotUserAndToken] END - Refreshed', { duration, hasUser: !!refreshed.user, userLogin: refreshed.user?.login, userId: refreshed.user?.id, tokenLength: refreshed.access?.length });
        return refreshed;
      }
    }

    const duration = Date.now() - startTime;
    log('[getBotUserAndToken] END - Success', { duration, hasUser: !!user, userLogin: user?.login, userId: user?.id, tokenLength: access_token?.length });
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

// Simplified bot control - no complex chains
router.post('/irc/connect', async (_req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    log('[Bot] /irc/connect called');
    const id = env.TWITCH_BOT.idFromName('twitch-bot');
    ctx.waitUntil(env.TWITCH_BOT.get(id).fetch('https://do/connect', { method: 'POST' }));
    return json(202, { accepted: true });
  } catch (e: any) {
    return json(500, { error: e?.message || 'connect failed' });
  }
});

router.post('/irc/reload', async (_req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    log('[Bot] /irc/reload called');
    const id = env.TWITCH_BOT.idFromName('twitch-bot');
    ctx.waitUntil(env.TWITCH_BOT.get(id).fetch('https://do/reload', { method: 'POST' }));
    return json(202, { accepted: true });
  } catch (e: any) {
    return json(500, { error: e?.message || 'reload failed' });
  }
});

router.get('/irc/health', async (req: Request, env: Env) => {
  try {
    const id = env.TWITCH_BOT.idFromName('twitch-bot');
    const response = await env.TWITCH_BOT.get(id).fetch('https://do/health');
    const health = await response.json();
    return json(200, health);
  } catch (e) {
    return json(500, { error: 'Health check failed' });
  }
});

// Zero-downtime channel management
router.post('/irc/channel/add', async (req: Request, env: Env, ctx: ExecutionContext) => {
  try {
    const body = await req.json();
    const { channel_login, twitch_id } = body;
    
    if (!channel_login) {
      return json(400, { error: 'channel_login required' });
    }
    
    log('[Bot] Adding channel dynamically', { channel_login, twitch_id });
    
    const id = env.TWITCH_BOT.idFromName('twitch-bot');
    const response = await env.TWITCH_BOT.get(id).fetch('https://do/channel/add', {
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
    
    const id = env.TWITCH_BOT.idFromName('twitch-bot');
    const response = await env.TWITCH_BOT.get(id).fetch('https://do/channel/remove', {
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

// CORS handling
const allowedOrigins = [
  'https://www.eloward.com',
  'https://eloward.com', 
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
};

export default worker;

/**
 * PRODUCTION-GRADE SINGLE TWITCH BOT
 * 
 * This is the complete bot implementation in a single Durable Object:
 * - Direct IRC connection with no proxy/forwarding layers
 * - Immediate message processing (no hibernation delays)
 * - Robust auto-recovery and reconnection
 * - Zero-downtime channel management
 */
export class TwitchBot {
  state: DurableObjectState;
  env: Env;
  
  // Connection state
  ws: WebSocket | null = null;
  connecting = false;
  ready = false;
  connectionStartTime = 0;
  
  // Bot identity  
  botLogin: string | null = null;
  botUserId: string | null = null;
  botToken: string | null = null;
  
  // Channel management
  assignedChannels: Array<{ channel_login: string; twitch_id?: string | null }> = [];
  channelSet: Set<string> = new Set();
  channelIdByLogin: Map<string, string> = new Map();
  modChannels: Set<string> = new Set();
  
  // Connection health
  keepaliveInterval: any = null;
  reconnectAttempts = 0;
  maxReconnectDelay = 30000; // 30 seconds max
  
  // Production metrics
  messagesProcessed = 0;
  timeoutsIssued = 0;
  lastActivity = 0;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    
    log('[TwitchBot] 🚀 Starting production bot');
    
    // Immediate state restoration and connection
    this.initialize();
  }

  async initialize() {
    try {
      // Restore state immediately
      await this.restoreState();
      
      // Auto-connect immediately if we have channels
      if (this.assignedChannels.length > 0) {
        this.connectToTwitch().catch(e => {
          log('[TwitchBot] ❌ Initial connection failed', { error: String(e) });
        });
      }
      
      // Set maintenance alarm (5 minutes)
      if (typeof this.state.setAlarm === 'function') {
        try { 
          this.state.setAlarm(Date.now() + 300_000);
          log('[TwitchBot] ✅ Maintenance alarm set');
        } catch (e) {
          log('[TwitchBot] ❌ Failed to set alarm', { error: String(e) });
        }
      }
    } catch (e) {
      log('[TwitchBot] ❌ Initialization failed', { error: String(e) });
    }
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    
    if (req.method === 'POST' && url.pathname === '/connect') {
      return this.handleConnect();
    }
    
    if (req.method === 'POST' && url.pathname === '/reload') {
      return this.handleReload();
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

  async handleConnect() {
    try {
      log('[TwitchBot] Manual connect requested');
      await this.loadChannelsFromDatabase();
      await this.connectToTwitch();
      return json(200, { 
        connected: true, 
        channels: this.assignedChannels.length,
        timestamp: new Date().toISOString()
      });
    } catch (e) {
      log('[TwitchBot] ❌ Manual connect failed', { error: String(e) });
      return json(500, { 
        error: 'Connection failed', 
        message: String(e) 
      });
    }
  }

  async handleReload() {
    try {
      log('[TwitchBot] Reload requested');
      await this.loadChannelsFromDatabase();
      
      // Reconnect if needed
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        await this.connectToTwitch();
      } else if (this.ready) {
        // Just rejoin channels if already connected
        this.joinAllChannels();
      }
      
      return json(200, { 
        reloaded: true, 
        channels: this.assignedChannels.length,
        timestamp: new Date().toISOString()
      });
    } catch (e) {
      log('[TwitchBot] ❌ Reload failed', { error: String(e) });
      return json(500, { error: String(e) });
    }
  }

  async handleHealthCheck() {
    const now = Date.now();
    const connectionAge = this.connectionStartTime ? now - this.connectionStartTime : 0;
    const wsState = this.ws ? this.ws.readyState : -1;
    
    // Detailed health status
    const health = {
      connected: wsState === WebSocket.OPEN,
      ready: this.ready,
      channels: this.assignedChannels.length,
      modChannels: this.modChannels.size,
      connectionAge,
      messagesProcessed: this.messagesProcessed,
      timeoutsIssued: this.timeoutsIssued,
      lastActivity: this.lastActivity,
      reconnectAttempts: this.reconnectAttempts,
      botLogin: this.botLogin,
      wsReadyState: wsState,
      wsReadyStates: { 0: 'CONNECTING', 1: 'OPEN', 2: 'CLOSING', 3: 'CLOSED' },
      timestamp: new Date().toISOString(),
      // Diagnostics
      channelList: this.assignedChannels.map(c => c.channel_login).slice(0, 10),
      modChannelList: Array.from(this.modChannels).slice(0, 10)
    };
    
    return json(200, health);
  }

  // Zero-downtime channel management
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
        log('[TwitchBot] ✅ Dynamically joined channel', { channel: channel_login });
      }
      
      // Save state
      await this.saveState();
      
      return json(200, { 
        message: 'Channel added successfully', 
        channel: channel_login,
        totalChannels: this.assignedChannels.length 
      });
    } catch (e) {
      log('[TwitchBot] ❌ Add channel failed', { error: String(e) });
      return json(500, { error: String(e) });
    }
  }

  async handleRemoveChannel(req: Request) {
    try {
      const body = await req.json();
      const { channel_login } = body;
      
      if (!channel_login) {
        return json(400, { error: 'channel_login required' });
      }
      
      // Remove from all data structures
      this.assignedChannels = this.assignedChannels.filter(c => c.channel_login !== channel_login);
      this.channelSet.delete(`#${channel_login}`);
      this.channelIdByLogin.delete(channel_login);
      this.modChannels.delete(`#${channel_login}`);
      
      // Leave immediately if connected
      if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.sendRaw(`PART #${channel_login}`);
        log('[TwitchBot] ✅ Dynamically left channel', { channel: channel_login });
      }
      
      // Save state
      await this.saveState();
      
      return json(200, { 
        message: 'Channel removed successfully', 
        channel: channel_login,
        totalChannels: this.assignedChannels.length 
      });
    } catch (e) {
      log('[TwitchBot] ❌ Remove channel failed', { error: String(e) });
      return json(500, { error: String(e) });
    }
  }

  // Load channels from database
  async loadChannelsFromDatabase() {
    try {
      const result = await this.env.DB.prepare(`
        SELECT channel_name as channel_login, twitch_id 
        FROM twitch_bot_users 
        WHERE bot_enabled = 1
      `).all();
      
      const dbChannels = result.results || [];
      this.assignedChannels = dbChannels as any[];
      this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
      
      // Build channel ID mapping
      this.channelIdByLogin.clear();
      for (const c of this.assignedChannels) {
        if (c.twitch_id) {
          this.channelIdByLogin.set(c.channel_login, String(c.twitch_id));
        }
      }
      
      await this.saveState();
      
      log('[TwitchBot] ✅ Channels loaded from database', { count: this.assignedChannels.length });
    } catch (e) {
      log('[TwitchBot] ❌ Failed to load channels from database', { error: String(e) });
    }
  }

  // DIRECT IRC CONNECTION - No proxying, no hibernation complexity
  async connectToTwitch() {
    if (this.connecting || (this.ws && this.ws.readyState === WebSocket.OPEN)) {
      log('[TwitchBot] Connection already in progress or established');
      return;
    }
    
    try {
      log('[TwitchBot] 🔌 Connecting to Twitch IRC...');
      
      // Get bot credentials
      const bot = await getBotUserAndToken(this.env);
      if (!bot?.user?.login || !bot?.access || !bot?.user?.id) {
        throw new Error('Bot credentials not available');
      }
      
      this.botLogin = String(bot.user.login).toLowerCase();
      this.botUserId = String(bot.user.id);
      this.botToken = bot.access;
      this.connecting = true;
      this.connectionStartTime = Date.now();
      
      log('[TwitchBot] 🔑 Bot credentials loaded', { 
        login: this.botLogin,
        userId: this.botUserId,
        tokenLength: this.botToken?.length || 0 
      });
      
      // Direct WebSocket connection
      this.ws = new WebSocket('wss://irc-ws.chat.twitch.tv:443');
      
      // Connection timeout
      const connectionTimeout = setTimeout(() => {
        if (this.connecting) {
          log('[TwitchBot] ❌ Connection timeout');
          this.handleDisconnection();
        }
      }, 15000); // 15 second timeout
      
      this.ws.addEventListener('open', () => {
        clearTimeout(connectionTimeout);
        log('[TwitchBot] ✅ WebSocket connected to Twitch IRC', { 
          login: this.botLogin,
          connectionTime: Date.now() - this.connectionStartTime 
        });
        
        // Authenticate immediately
        const authToken = this.botToken!.startsWith('oauth:') ? this.botToken : `oauth:${this.botToken}`;
        this.sendRaw(`PASS ${authToken}`);
        this.sendRaw(`NICK ${this.botLogin}`);
        this.sendRaw('CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership');
        log('[TwitchBot] 🔐 Authentication sent');
        
        this.connecting = false;
        this.reconnectAttempts = 0;
        this.startKeepalive();
      });
      
      this.ws.addEventListener('message', (event) => {
        this.lastActivity = Date.now();
        this.messagesProcessed++;
        const messageData = String(event.data || '');
        
        if (messageData.trim()) {
          this.handleIrcMessage(messageData);
        }
      });
      
      this.ws.addEventListener('close', (event) => {
        clearTimeout(connectionTimeout);
        log('[TwitchBot] 🔌 Connection closed', { 
          code: event.code, 
          reason: event.reason || 'No reason provided',
          wasClean: event.wasClean,
          connectionAge: Date.now() - this.connectionStartTime
        });
        this.handleDisconnection();
      });
      
      this.ws.addEventListener('error', (event) => {
        clearTimeout(connectionTimeout);
        log('[TwitchBot] ❌ WebSocket error', { 
          error: String(event),
          readyState: this.ws?.readyState 
        });
        this.handleDisconnection();
      });
      
    } catch (e) {
      this.connecting = false;
      log('[TwitchBot] ❌ Connection setup failed', { error: String(e) });
      throw e;
    }
  }

  sendRaw(line: string) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      log('[TwitchBot] ❌ Cannot send - WebSocket not ready', { 
        hasWebSocket: !!this.ws,
        readyState: this.ws?.readyState,
        line: line.substring(0, 50)
      });
      return;
    }
    
    try {
      this.ws.send(line);
      // Only log important messages (not PING/PONG spam)
      if (!line.startsWith('PING') && !line.startsWith('PONG')) {
        log('[TwitchBot] >> SENT', { line });
      }
    } catch (e) {
      log('[TwitchBot] ❌ Send failed', { line: line.substring(0, 50), error: String(e) });
    }
  }

  startKeepalive() {
    this.stopKeepalive();
    
    log('[TwitchBot] 🔄 Anti-hibernation keepalive started');
    this.keepaliveInterval = setInterval(() => {
      const connectionAge = Date.now() - this.connectionStartTime;
      log('[TwitchBot] 🔄 Anti-hibernation keepalive', { 
        connectionAge, 
        messagesProcessed: this.messagesProcessed, 
        timeoutsIssued: this.timeoutsIssued 
      });
      
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.sendRaw('PING :keepalive');
      } else {
        log('[TwitchBot] ❌ Keepalive check failed - WebSocket not ready');
      }
    }, 30000); // Every 30 seconds
  }

  stopKeepalive() {
    if (this.keepaliveInterval) {
      clearInterval(this.keepaliveInterval);
      this.keepaliveInterval = null;
    }
  }

  handleDisconnection() {
    log('[TwitchBot] 🔌 Handling disconnection', { 
      wasReady: this.ready,
      wasConnecting: this.connecting,
      reconnectAttempts: this.reconnectAttempts
    });
    
    this.ready = false;
    this.connecting = false;
    this.stopKeepalive();
    
    // Exponential backoff reconnection
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), this.maxReconnectDelay);
    this.reconnectAttempts++;
    
    log('[TwitchBot] 🔄 Scheduling reconnection', { 
      attempt: this.reconnectAttempts,
      delayMs: delay
    });
    
    setTimeout(() => {
      log('[TwitchBot] 🔄 Attempting reconnection', { attempt: this.reconnectAttempts });
      this.connectToTwitch().catch((e) => {
        log('[TwitchBot] ❌ Reconnection failed', { attempt: this.reconnectAttempts, error: String(e) });
      });
    }, delay);
  }

  // IRC MESSAGE PROCESSING - The core of the bot
  handleIrcMessage(data: string) {
    const lines = data.trim().split(/\r?\n/).filter(line => line.length > 0);
    
    for (const line of lines) {
      // Handle PING immediately
      if (line.startsWith('PING ')) {
        const pongResponse = line.replace('PING', 'PONG');
        this.sendRaw(pongResponse);
        log('[TwitchBot] 🏓 PONG response sent');
        continue;
      }
      
      // Parse IRC message
      const msg = this.parseIrc(line.trim());
      if (!msg) {
        log('[TwitchBot] ❌ Failed to parse message', { rawLine: line.substring(0, 100) });
        continue;
      }
      
      log('[TwitchBot] Message received', { 
        command: msg.command, 
        channel: msg.params?.[0], 
        user: msg.prefix?.split('!')[0],
        tags: msg.tags ? Object.keys(msg.tags) : [],
        fullMessage: { command: msg.command, params: msg.params, prefix: msg.prefix },
        rawLine: line.substring(0, 200)
      });
      
      // Handle authentication success
      if (msg.command === '001') {
        this.ready = true;
        this.reconnectAttempts = 0;
        log('[TwitchBot] ✅ Authentication successful - bot is ready', { 
          botLogin: this.botLogin,
          connectionTime: Date.now() - this.connectionStartTime,
          assignedChannels: this.assignedChannels.length,
          modChannels: Array.from(this.modChannels)
        });
        
        // Join all assigned channels
        this.joinAllChannels();
        continue;
      }
      
      // Track mod permissions
      if (msg.command === 'USERSTATE') {
        const channel = msg.params?.[0];
        const isMod = msg.tags?.mod === '1';
        
        if (channel) {
          if (isMod) {
            this.modChannels.add(channel);
            log('[TwitchBot] Confirmed mod permissions', { channel });
          } else {
            log('[TwitchBot] ❌ No mod permissions', { channel });
          }
        }
        continue;
      }
      
      // Handle chat messages - THE CRITICAL PATH
      if (msg.command === 'PRIVMSG') {
        this.handlePrivmsg(msg);
        continue;
      }
      
      // Handle other IRC events
      if (msg.command === 'JOIN' && msg.prefix?.includes(this.botLogin!)) {
        const channel = msg.params?.[0];
        log('[TwitchBot] ✅ Successfully joined channel', { 
          channel, 
          totalChannels: this.channelSet.size
        });
      }
    }
  }

  // PRIVMSG HANDLER - Where timeouts happen
  async handlePrivmsg(msg: any) {
    const channel = msg.params?.[0];
    const message = msg.params?.[1];
    const user = msg.prefix?.split('!')[0];
    
    log('[PRIVMSG] 🔍 Raw message analysis', {
      fullParams: msg.params,
      prefix: msg.prefix,
      tags: msg.tags,
      parsedChannel: channel,
      parsedMessage: message,
      parsedUser: user,
      messagePreview: message?.substring(0, 50)
    });
    
    if (!user || !channel) {
      log('[PRIVMSG] ❌ Invalid message format', { 
        hasUser: !!user, 
        hasChannel: !!channel
      });
      return;
    }
    
    const chanLogin = channel.replace('#', '');
    log('[PRIVMSG] Processing message', { 
      channel: chanLogin, 
      user, 
      messageLength: message?.length || 0,
      timestamp: new Date().toISOString(),
      assignedChannels: this.assignedChannels.map(c => c.channel_login),
      modChannels: Array.from(this.modChannels)
    });
    
    // Skip privileged users
    const badges = (msg.tags?.badges || '').split(',').map((b: string) => b.split('/')[0]);
    const isPrivileged = badges.some((b: string) => ['broadcaster', 'moderator', 'vip'].includes(b));
    
    if (isPrivileged) {
      log('[PRIVMSG] ⏭️ Skipping privileged user', { user, badges, channel: chanLogin });
      return;
    }
    
    // Check mod permissions
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
    if (!config || !config.bot_enabled) {
      log('[PRIVMSG] ❌ Channel not enabled', { channel: chanLogin, hasConfig: !!config });
      return;
    }
    
    log('[PRIVMSG] ✅ Channel config valid', { 
      channel: chanLogin,
      timeout_seconds: config.timeout_seconds,
      has_reason_template: !!config.reason_template 
    });
    
    // Check user rank
    log('[PRIVMSG] Checking user rank', { user, channel: chanLogin });
    const hasRank = await this.checkUserRank(user);
    if (hasRank) {
      log('[PRIVMSG] ✅ User has valid rank - no timeout needed', { user, channel: chanLogin });
      return;
    }
    
    log('[PRIVMSG] ❌ User lacks required rank - issuing timeout', { user, channel: chanLogin });
    
    // TIMEOUT THE USER
    await this.timeoutUser(chanLogin, user, config.timeout_seconds || 20, config.reason_template);
  }

  async checkUserRank(username: string): Promise<boolean> {
    const startTime = Date.now();
    try {
      log('[RANK] 🔍 Starting rank check', { 
        username,
        requestUrl: `https://internal/api/ranks/lol/${username}`,
        hasRankWorker: !!this.env.RANK_WORKER
      });
      
      const response = await this.env.RANK_WORKER.fetch(
        new Request(`https://internal/api/ranks/lol/${username}`)
      );
      const duration = Date.now() - startTime;
      
      log('[RANK] 📊 Rank API response received', {
        username,
        status: response.status,
        ok: response.ok,
        duration,
        headers: Object.fromEntries(response.headers.entries()),
        statusText: response.statusText
      });
      
      if (response.ok) {
        const data = await response.json();
        log('[RANK] ✅ User has valid rank', { 
          username, 
          status: response.status, 
          duration,
          rankData: data
        });
        return true;
      } else {
        const errorText = await response.text().catch(() => 'Unknown error');
        log('[RANK] ❌ User lacks required rank', { 
          username, 
          status: response.status, 
          duration,
          errorText: errorText.substring(0, 200)
        });
        return false;
      }
    } catch (e) {
      const duration = Date.now() - startTime;
      log('[RANK] ❌ Rank check failed with error', { 
        username, 
        error: String(e), 
        duration
      });
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

  async timeoutUser(channelLogin: string, userLogin: string, duration: number, reason?: string) {
    const startTime = Date.now();
    const timeoutId = `${channelLogin}:${userLogin}:${Date.now()}`;
    
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
      
      log('[TIMEOUT] 🔍 Bot credentials detailed check', {
        timeoutId,
        hasBotObject: !!bot,
        hasAccess: !!bot?.access,
        hasUser: !!bot?.user,
        hasUserId: !!bot?.user?.id,
        userLogin: bot?.user?.login,
        userId: bot?.user?.id,
        accessTokenLength: bot?.access?.length || 0,
        botObjectKeys: bot ? Object.keys(bot) : [],
        userObjectKeys: bot?.user ? Object.keys(bot.user) : [],
        fullBotObject: bot
      });
      
      if (!bot?.access || !bot?.user?.id) {
        log('[TIMEOUT] ❌ Bot credentials not available', { timeoutId });
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
      const timeoutReason = reason || `{seconds}s timeout: not enough elo to speak. Link your EloWard at {site}`.replace('{seconds}', String(duration)).replace('{site}', this.env.SITE_BASE_URL || 'https://www.eloward.com');
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
        log('[TIMEOUT] ❌ Timeout failed', { 
          timeoutId,
          channel: channelLogin, 
          user: userLogin, 
          userId,
          status: timeoutResponse.status,
          error: errorText,
          processingTime: totalDuration
        });
      }
    } catch (e) {
      const totalDuration = Date.now() - startTime;
      log('[TIMEOUT] ❌ Timeout process crashed', { 
        timeoutId,
        channel: channelLogin, 
        user: userLogin, 
        error: String(e),
        processingTime: totalDuration
      });
    }
  }

  joinAllChannels() {
    if (this.assignedChannels.length === 0) {
      log('[TwitchBot] No channels to join');
      return;
    }
    
    log('[TwitchBot] Joining all assigned channels', { 
      channelCount: this.channelSet.size,
      channels: Array.from(this.channelSet) 
    });
    
    for (const channel of this.channelSet) {
      this.sendRaw(`JOIN ${channel}`);
      log('[TwitchBot] Sent JOIN command', { channel });
    }
  }

  parseIrc(line: string): { tags?: Record<string, string>; prefix?: string; command: string; params: string[] } | null {
    let rest = line;
    const msg: any = { params: [] };
    
    // Parse tags (@key=value;key2=value2)
    if (rest.startsWith('@')) {
      const sp = rest.indexOf(' ');
      if (sp === -1) return null;
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
      if (sp === -1) return null;
      msg.prefix = rest.slice(1, sp);
      rest = rest.slice(sp + 1);
    }
    
    // Parse command
    const sp = rest.indexOf(' ');
    if (sp === -1) {
      msg.command = rest;
      return msg;
    }
    
    msg.command = rest.slice(0, sp);
    rest = rest.slice(sp + 1);
    
    // Parse parameters
    if (rest.startsWith(':')) {
      msg.params = [rest.slice(1)];
    } else {
      const parts: string[] = [];
      while (rest) {
        if (rest.startsWith(':')) {
          parts.push(rest.slice(1));
          break;
        }
        const idx = rest.indexOf(' ');
        if (idx === -1) {
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

  async saveState() {
    try {
      await Promise.all([
        this.state.storage.put('assignedChannels', this.assignedChannels),
        this.state.storage.put('modChannels', Array.from(this.modChannels))
      ]);
      
      log('[TwitchBot] ✅ State saved to storage', { 
        channels: this.assignedChannels.length,
        modChannels: this.modChannels.size
      });
    } catch (e) {
      log('[TwitchBot] ❌ Save state failed', { error: String(e) });
    }
  }

  async restoreState() {
    try {
      const stored = await this.state.storage.get('assignedChannels') as any[];
      if (stored) {
        this.assignedChannels = stored;
        this.channelSet = new Set(this.assignedChannels.map(c => `#${c.channel_login}`));
        this.channelIdByLogin.clear();
        
        for (const c of this.assignedChannels) {
          if (c.twitch_id) {
            this.channelIdByLogin.set(c.channel_login, String(c.twitch_id));
          }
        }
        
        log('[TwitchBot] ✅ Channels restored from storage', { 
          count: this.assignedChannels.length,
          channels: this.assignedChannels.map(c => c.channel_login)
        });
      }
      
      const modChannels = await this.state.storage.get('modChannels') as string[];
      if (modChannels) {
        this.modChannels = new Set(modChannels);
        log('[TwitchBot] ✅ Mod channels restored from storage', { 
          count: this.modChannels.size,
          modChannels: Array.from(this.modChannels)
        });
      }
    } catch (e) {
      log('[TwitchBot] ❌ Restore state failed', { error: String(e) });
    }
  }

  // Maintenance alarm - 5 minutes
  async alarm() {
    const startTime = Date.now();
    log('[TwitchBot] 🔧 Maintenance alarm fired', {
      connected: this.ws?.readyState === WebSocket.OPEN,
      ready: this.ready,
      channels: this.assignedChannels.length,
      lastActivity: this.lastActivity ? Date.now() - this.lastActivity : null,
      messagesProcessed: this.messagesProcessed,
      timeoutsIssued: this.timeoutsIssued
    });
    
    try {
      // Restore state from storage
      await this.restoreState();
      
      // Sync channels from database (zero-downtime discovery)
      await this.syncChannelsFromDatabase();
      
      // Ensure connection is healthy
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        log('[TwitchBot] Connection unhealthy - attempting reconnection');
        this.connectToTwitch().catch(e => {
          log('[TwitchBot] ❌ Maintenance reconnect failed', { error: String(e) });
        });
      } else if (this.ready && this.assignedChannels.length > 0) {
        // Verify all channels are joined
        this.joinAllChannels();
      }
      
      log('[TwitchBot] 🔧 Maintenance completed', { 
        duration: Date.now() - startTime,
        channels: this.assignedChannels.length
      });
    } catch (e) {
      log('[TwitchBot] ❌ Maintenance failed', { 
        error: String(e), 
        duration: Date.now() - startTime 
      });
    }
    
    // Re-arm alarm for next maintenance
    if (typeof this.state.setAlarm === 'function') {
      try { 
        this.state.setAlarm(Date.now() + 300_000); // 5 minutes
      } catch (e) {
        log('[TwitchBot] ❌ Failed to re-arm alarm', { error: String(e) });
      }
    }
  }

  // Zero-downtime channel sync
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
      const newChannels = dbChannels.filter(c => !currentChannels.has((c as any).channel_login));
      
      // Find channels to remove
      const dbChannelSet = new Set(dbChannels.map(c => (c as any).channel_login));
      const channelsToRemove = this.assignedChannels.filter(c => !dbChannelSet.has(c.channel_login));
      
      // Add new channels
      for (const channel of newChannels) {
        const ch = channel as any;
        this.assignedChannels.push(ch);
        this.channelSet.add(`#${ch.channel_login}`);
        
        if (ch.twitch_id) {
          this.channelIdByLogin.set(ch.channel_login, String(ch.twitch_id));
        }
        
        // Join immediately if connected
        if (this.ready && this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.sendRaw(`JOIN #${ch.channel_login}`);
          log('[TwitchBot] ✅ Auto-joined new channel', { channel: ch.channel_login });
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
          log('[TwitchBot] ✅ Auto-left disabled channel', { channel: channel.channel_login });
        }
      }
      
      // Save state if changes were made
      if (newChannels.length > 0 || channelsToRemove.length > 0) {
        await this.saveState();
        log('[TwitchBot] 🔄 Channel sync completed', { 
          added: newChannels.length, 
          removed: channelsToRemove.length,
          total: this.assignedChannels.length 
        });
      }
    } catch (e) {
      log('[TwitchBot] ❌ Channel sync failed', { error: String(e) });
    }
  }
}