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

interface ExecutionContext {
  waitUntil(promise: Promise<any>): void;
  passThroughOnException(): void;
}

// Define environment variable interface
interface Env {
  RIOT_CLIENT_ID: string;
  RIOT_API_KEY: string;
  RIOT_CLIENT_SECRET: string;
  RANK_WORKER: Fetcher; // Service binding to rank-worker
  TWITCH_AUTH_WORKER: Fetcher; // Service binding to twitchauth-worker
  USERS_WORKER: Fetcher; // Service binding to users-worker
  STRIPE_WORKER: Fetcher; // Service binding to stripe-worker
  // Secret for internal service-to-service calls
  RANK_WRITE_KEY: string;
  USERS_WRITE_KEY: string;
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

// Region mapping for op.gg URLs (matches extension logic)
const REGION_MAPPING: Record<string, string> = {
  'na1': 'na', 'euw1': 'euw', 'eun1': 'eune', 'kr': 'kr', 'br1': 'br',
  'jp1': 'jp', 'la1': 'lan', 'la2': 'las', 'oc1': 'oce', 'tr1': 'tr',
  'ru': 'ru', 'me1': 'me', 'sea': 'sea', 'sg2': 'sea', 'tw2': 'tw', 'vn2': 'vn'
};

// Rank hierarchy for comparison (matches test-opgg-scraper.js)
const RANK_ORDER: Record<string, number> = {
  'IRON': 1, 'BRONZE': 2, 'SILVER': 3, 'GOLD': 4,
  'PLATINUM': 5, 'EMERALD': 6, 'DIAMOND': 7,
  'MASTER': 8, 'GRANDMASTER': 9, 'CHALLENGER': 10
};

// Non-apex division ordering. Apex tiers (Master+) have no division -- LP
// alone decides ordering within the tier, so division is ignored there.
const DIVISION_ORDER: Record<string, number> = { '4': 1, '3': 2, '2': 3, '1': 4 };

const VALID_TIERS: Set<string> = new Set(Object.keys(RANK_ORDER));

// Roman division strings from Riot API -> Arabic strings op.gg emits. Keeping
// one internal representation avoids silently mis-ordering cross-source data.
const ROMAN_TO_ARABIC: Record<string, string> = { I: '1', II: '2', III: '3', IV: '4' };

interface ScrapedRank {
  tier: string;
  division: string | null;
  lp: number;
  season: string;
  type: string;
}

// OpGG scraper for Cloudflare Workers
class OpGGScraper {
  private static readonly HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
    'Cache-Control': 'no-cache',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1'
  } as const;

  async scrapeUrl(url: string): Promise<string> {
    const response = await fetch(url, {
      method: 'GET',
      headers: OpGGScraper.HEADERS
    });

    if (!response.ok) {
      const status = response.status;
      switch (status) {
        case 404:
          throw new Error(`Profile not found: HTTP 404`);
        case 403:
          throw new Error(`Access forbidden: HTTP 403`);
        case 429:
          throw new Error(`Rate limit exceeded: HTTP 429`);
        case 503:
          throw new Error(`Service unavailable: HTTP 503`);
        default:
          throw new Error(`HTTP ${status}: ${response.statusText}`);
      }
    }

    return await response.text();
  }

  /**
   * Extract every usable rank from an op.gg solo-queue profile page.
   *
   * op.gg renders with Next.js RSC streaming: the real per-season peak data
   * lives inside self.__next_f.push([1,"<escaped-JSON>"]) blobs, not in the
   * HTML markup. For each season the payload contains:
   *   rank_entries.high_rank_info -- peak reached mid-season (hover tooltip).
   *                                  tier="" / lp=null when op.gg didn't track
   *                                  hover-peak for that season.
   *   rank_entries.rank_info      -- end-of-season placement (always present
   *                                  when the user played ranked).
   * We emit every valid block; findHighestRank picks the overall best.
   */
  extractAllRanksFromHTML(html: string): ScrapedRank[] {
    const payloads = this.collectNextPushPayloads(html);
    if (payloads.length === 0) return [];

    let decoded: string;
    try {
      decoded = JSON.parse('"' + payloads.join('') + '"');
    } catch {
      return [];
    }

    const ranks: ScrapedRank[] = [];
    const kinds: Array<[string, string]> = [['high_rank_info', 'high'], ['rank_info', 'ended']];
    for (const season of this.collectSeasonObjects(decoded)) {
      const entries = season.rank_entries || {};
      for (const [field, type] of kinds) {
        const info = entries[field];
        if (!info || typeof info.tier !== 'string' || !info.tier || info.lp == null) continue;
        const r = this.normalizeRank(info.tier, info.lp, season.season, type);
        if (r) ranks.push(r);
      }
    }
    return ranks;
  }

  private collectNextPushPayloads(html: string): string[] {
    const re = /self\.__next_f\.push\(\[1,"([\s\S]*?)"\]\)/g;
    const out: string[] = [];
    let m: RegExpExecArray | null;
    while ((m = re.exec(html)) !== null) out.push(m[1]);
    return out;
  }

  private collectSeasonObjects(text: string): any[] {
    const needle = '{"season":"';
    const out: any[] = [];
    let idx = 0;
    while ((idx = text.indexOf(needle, idx)) !== -1) {
      const end = this.matchBalancedBrace(text, idx);
      if (end === -1) break;
      const slice = text.substring(idx, end + 1);
      idx = end + 1;
      let obj: any;
      try { obj = JSON.parse(slice); } catch { continue; }
      if (obj && typeof obj.season === 'string' && obj.rank_entries) out.push(obj);
    }
    return out;
  }

  // Given text[start] === '{', return the index of the matching '}', or -1.
  // Skips over JSON string literals so braces inside strings don't confuse us.
  private matchBalancedBrace(text: string, start: number): number {
    if (text[start] !== '{') return -1;
    let depth = 0;
    for (let i = start; i < text.length; i++) {
      const c = text[i];
      if (c === '"') {
        i++;
        while (i < text.length && text[i] !== '"') {
          if (text[i] === '\\') i++;
          i++;
        }
        continue;
      }
      if (c === '{') depth++;
      else if (c === '}') {
        depth--;
        if (depth === 0) return i;
      }
    }
    return -1;
  }

  private normalizeRank(tierStr: string, lpStr: string | number, season: string, type: string): ScrapedRank | null {
    const parts = String(tierStr).trim().toLowerCase().split(/\s+/);
    const tier = (parts[0] || '').toUpperCase();
    if (!VALID_TIERS.has(tier)) return null;
    const division = parts[1] || null;
    const lp = parseInt(String(lpStr).replace(/[^0-9]/g, ''), 10) || 0;
    return { tier, division, lp, season: (season || '').trim(), type };
  }

  // Convert a Riot API league entry ({tier, rank, leaguePoints}) into the same
  // shape the scraper emits, so it can participate in findHighestRank.
  normalizeRiotCurrentRank(current: any): ScrapedRank | null {
    if (!current || !current.tier) return null;
    const tier = String(current.tier).toUpperCase();
    if (!VALID_TIERS.has(tier)) return null;
    const rawDiv = current.rank ? String(current.rank).toUpperCase() : null;
    const division = rawDiv ? (ROMAN_TO_ARABIC[rawDiv] || rawDiv) : null;
    return {
      tier,
      division,
      lp: parseInt(current.leaguePoints, 10) || 0,
      season: 'CURRENT',
      type: 'current_riot'
    };
  }

  findHighestRank(ranks: ScrapedRank[]): ScrapedRank | null {
    if (!ranks || ranks.length === 0) return null;
    return ranks.reduce((best, cur) => (this.isRankHigher(cur, best) ? cur : best));
  }

  // Mirrors isRankHigher in rank-worker.js so seeding and the live rank-write
  // path agree on ordering.
  private isRankHigher(a: ScrapedRank, b: ScrapedRank): boolean {
    if (!b || !b.tier) return true;
    if (!a || !a.tier) return false;
    const aTier = RANK_ORDER[a.tier] || 0;
    const bTier = RANK_ORDER[b.tier] || 0;
    if (aTier === 0 || bTier === 0) return false;
    if (aTier !== bTier) return aTier > bTier;
    if (aTier >= 8) return (a.lp || 0) > (b.lp || 0); // Master+: LP only.
    const aDiv = DIVISION_ORDER[a.division || ''] || 0;
    const bDiv = DIVISION_ORDER[b.division || ''] || 0;
    if (aDiv !== bDiv) return aDiv > bDiv;
    return (a.lp || 0) > (b.lp || 0);
  }
}


// Async peak rank seeding function
async function seedPeakRankAsync(
  puuid: string, 
  riotId: string, 
  region: string, 
  currentRank: any, 
  env: Env
): Promise<void> {
  try {
    // Validate region
    const opggRegion = REGION_MAPPING[region.toLowerCase()];
    if (!opggRegion) {
      console.warn(`[PeakSeed] Unsupported region ${region} for ${riotId}`);
      return;
    }

    // Construct op.gg URL
    const riotIdParts = riotId.split('#');
    const encodedName = encodeURIComponent(riotIdParts[0] || '');
    const tagLine = riotIdParts[1] || region.toUpperCase();
    const opggUrl = `https://op.gg/lol/summoners/${opggRegion}/${encodedName}-${tagLine}?queue_type=SOLORANKED`;
    
    console.log(`[PeakSeed] Seeding peak rank for ${riotId}`);

    // Create scraper
    const scraper = new OpGGScraper();
    let peakRank: any = null;
    
    try {
      // Scrape op.gg HTML
      const html = await scraper.scrapeUrl(opggUrl);

      // Extract every per-season rank op.gg exposes in its RSC stream, then
      // mix in the live current rank from Riot API. Riot data is always
      // fresher than op.gg's scrape and occasionally op.gg lags a split, so
      // including both guarantees we never seed a peak below current.
      const allRanks = scraper.extractAllRanksFromHTML(html);
      const currentNorm = scraper.normalizeRiotCurrentRank(currentRank);
      if (currentNorm) allRanks.push(currentNorm);

      if (allRanks.length === 0) {
        console.log(`[PeakSeed] No rank history found for ${riotId}`);
        return; // No data to process
      }

      peakRank = scraper.findHighestRank(allRanks);

      if (!peakRank) {
        console.log(`[PeakSeed] No valid peak rank extracted for ${riotId}`);
        return; // Invalid data
      }

      console.log(`[PeakSeed] Peak rank found: ${peakRank.tier} ${peakRank.division || 'N/A'} ${peakRank.lp}LP for ${riotId}`);
      
    } catch (error) {
      if (error instanceof Error) {
        // Handle specific error cases
        if (error.message.includes('404')) {
          console.log(`[PeakSeed] Profile not found for ${riotId} - likely new account`);
        } else if (error.message.includes('403')) {
          console.warn(`[PeakSeed] Access blocked for ${riotId} - will retry later`);
        } else if (error.message.includes('429')) {
          console.warn(`[PeakSeed] Rate limited for ${riotId} - will retry later`);
        } else {
          console.error(`[PeakSeed] Scraping failed for ${riotId}: ${error.message}`);
        }
      }
      return; // Exit on scraping failure
    }

    // Update database only if we have valid peak rank data
    await updatePeakRank(puuid, riotId, region, peakRank, env);

  } catch (error) {
    // Top-level error handling - should rarely occur
    console.error(`[PeakSeed] Critical error seeding ${riotId}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

// Database update for peak ranks
async function updatePeakRank(puuid: string, riotId: string, region: string, peakRank: any, env: Env): Promise<void> {
  try {
    // Format rank data
    const formattedRank = formatPeakRankForDatabase(peakRank);
    
    // Fetch existing user data
    const lookupRequest = new Request('https://rank-worker/api/ranks/lol/by-puuid', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify({ puuid, raw: true })
    });

    const lookupResponse = await env.RANK_WORKER.fetch(lookupRequest);
    if (!lookupResponse.ok) {
      console.error(`[PeakSeed] User lookup failed for ${riotId}: ${lookupResponse.status}`);
      return;
    }

    const existingData = await lookupResponse.json();
    
    // Construct update payload - preserve current rank, update peak only
    const updateData = {
      riot_puuid: puuid,
      twitch_username: existingData.twitch_username,
      riot_id: riotId,
      rank_tier: existingData.rank_tier,
      rank_division: existingData.rank_division,
      lp: existingData.lp,
      region: region,
      plus_active: existingData.plus_active,
      // Peak rank override
      peak_rank_tier: formattedRank.tier,
      peak_rank_division: formattedRank.division,
      peak_lp: formattedRank.lp
    };

    // Single database update call
    const updateRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify(updateData)
    });

    const response = await env.RANK_WORKER.fetch(updateRequest);
    
    if (!response.ok) {
      console.error(`[PeakSeed] Database update failed for ${riotId}: ${response.status}`);
      return;
    }
    
    const result = await response.json();
    if (result.success) {
      console.log(`[PeakSeed] Updated peak rank for ${riotId}: ${formattedRank.tier} ${formattedRank.division || 'NULL'} ${formattedRank.lp}LP`);
    } else {
      console.warn(`[PeakSeed] Peak rank update returned non-success for ${riotId}`);
    }
    
  } catch (error) {
    console.error(`[PeakSeed] Critical error updating ${riotId}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

// Peak rank formatting - matches manual script exactly
function formatPeakRankForDatabase(peakRank: any): { tier: string; division: string | null; lp: number } {
  const tier = peakRank.tier.toUpperCase();
  let division: string | null = null;
  let lp = parseInt(peakRank.lp) || 0;

  // Handle division formatting to match database format (identical to manual script)
  if (['MASTER', 'GRANDMASTER', 'CHALLENGER'].includes(tier)) {
    division = 'I'; // Master+ always gets 'I'
  } else if (tier === 'UNRANKED') {
    division = null; // Unranked gets NULL division and 0 LP
    lp = 0;
  } else if (peakRank.division && peakRank.division.trim() !== '') {
    // Convert numeric divisions to Roman numerals
    const divisionMap: Record<string, string> = { '1': 'I', '2': 'II', '3': 'III', '4': 'IV' };
    division = divisionMap[peakRank.division] || peakRank.division.toUpperCase();
  } else {
    division = null; // No division data
  }

  return { tier, division, lp };
}

// Create a new router
const router = Router();

// Allowed origins for CORS
const allowedOrigins = [
  'https://www.eloward.com',
  'https://eloward.com',
  'http://localhost:3000'  // Development
];

// Helper function to generate CORS headers based on the request
function getCorsHeaders(request: Request) {
  const origin = request.headers.get('Origin');
  
  // Allow browser extensions (they send chrome-extension:// or moz-extension:// origins)
  const isExtensionOrigin = origin && (
    origin.startsWith('chrome-extension://') || 
    origin.startsWith('moz-extension://')
  );
  
  // Allow specific domains or extension origins
  const allowedOrigin = (origin && allowedOrigins.includes(origin)) || isExtensionOrigin 
    ? origin 
    : allowedOrigins[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin || allowedOrigins[0],
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}

// Define the standard redirect URI throughout the app
const STANDARD_REDIRECT_URI = "https://www.eloward.com/riot/auth/redirect";

// Add a helper function for consistent CORS responses
function corsResponse(status: number, data: any, request?: Request): Response {
  const corsHeaders = request ? getCorsHeaders(request) : {
    'Access-Control-Allow-Origin': allowedOrigins[0],
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
  
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
router.options('*', (request) => {
  return new Response(null, {
    status: 204,
    headers: getCorsHeaders(request),
  });
});


// Authentication initialization endpoint
// Removed legacy /auth/init endpoint

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
      const corsHeaders = getCorsHeaders(request);
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
      const corsHeaders = getCorsHeaders(request);
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
    const corsHeaders = getCorsHeaders(request);
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
router.post('/auth/complete', async (request: Request, env: Env, ctx: ExecutionContext) => {
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
      headers: { 'Content-Type': 'application/json', 'X-Internal-Auth': env.USERS_WRITE_KEY },
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

    // Step 5: Check subscription status for plus_active flag
    let plusActive = false;
    try {
      const subRequest = new Request('https://stripe-worker/subscription/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ twitch_id })
      });

      const subResponse = await env.STRIPE_WORKER.fetch(subRequest);
      if (subResponse.ok) {
        const subData = await subResponse.json();
        plusActive = subData.plus_active || false;
      }
    } catch (e) {
      // Subscription check failed, continue with false
      console.warn('Subscription status check failed during auth:', e);
    }

    // Step 6: Store rank data in database with subscription status
    const riotId = `${accountData.gameName}#${accountData.tagLine}`;
    const rankData = {
      riot_puuid: accountData.puuid,
      twitch_username: channelName,
      riot_id: riotId,
      rank_tier: currentRank ? currentRank.tier : 'UNRANKED',
      rank_division: currentRank ? currentRank.rank : null,
      lp: currentRank ? currentRank.leaguePoints : 0,
      region: region,
      plus_active: plusActive
    };

    const storeRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
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

    // Trigger async peak rank seeding after successful connection
    const storeResult = await storeResponse.json();
    if (storeResult.success) {
      console.log(`[RiotAuth] Triggering peak rank seeding for ${riotId}`);
      
      // Use ctx.waitUntil with timeout protection
      ctx.waitUntil(
        Promise.race([
          seedPeakRankAsync(accountData.puuid, riotId, region, currentRank, env),
          // Timeout after 25 seconds
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Peak seeding timeout')), 25000)
          )
        ]).catch(error => {
          const errorMsg = error instanceof Error ? error.message : String(error);
          if (errorMsg.includes('timeout')) {
            console.warn(`[RiotAuth] Peak seeding timeout for ${riotId} - will seed on next rank update`);
          } else {
            console.error(`[RiotAuth] Peak seeding failed for ${riotId}: ${errorMsg}`);
          }
        })
      );
    }

    // Step 7: Return complete user data for frontend
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
// Removed legacy /auth/token endpoint


// Get account info from Riot Account v1 API - uses americas endpoint for all regions
// Removed legacy /riot/account endpoint

// League entries endpoint with PUUID support
// Removed legacy /riot/league/entries endpoint

// Get league entries for a player by PUUID
// Removed legacy /riot/league/:platform/:puuid endpoint

// Simplified rank refresh endpoint that uses PUUID only
router.post('/riot/refreshrank', async (request: Request, env: Env) => {
  try {
    const { puuid } = await request.json() as { puuid?: string };

    if (!puuid) {
      return corsResponse(400, {
        status: 'error',
        message: 'Missing required parameter: puuid',
      });
    }

    // Get user data from database to find region and user info
    const getUserRequest = new Request('https://rank-worker/api/ranks/lol/by-puuid', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify({ puuid })
    });

    const getUserResponse = await env.RANK_WORKER.fetch(getUserRequest);
    
    if (!getUserResponse.ok) {
      return corsResponse(getUserResponse.status === 404 ? 404 : 500, {
        status: 'error',
        message: getUserResponse.status === 404 
          ? 'Account not found. Please reconnect your League account.'
          : 'Failed to retrieve account data. Please try again.',
        ...(getUserResponse.status === 404 && { action: 'clear_persistent_data' })
      });
    }

    const userData = await getUserResponse.json();

    // Fetch current rank from Riot API
    const leagueUrl = `https://${userData.region}.api.riotgames.com/lol/league/v4/entries/by-puuid/${puuid}`;
    const leagueResponse = await fetch(leagueUrl, {
      headers: { 'X-Riot-Token': env.RIOT_API_KEY }
    });

    if (!leagueResponse.ok) {
      throw new Error(`Riot API error: ${leagueResponse.status}`);
    }

    const leagueData = await leagueResponse.json() as LeagueEntry[];
    const currentRank = leagueData.find(entry => entry.queueType === 'RANKED_SOLO_5x5');

    // Update current rank in database (rank-worker handles peak rank logic)
    const updateRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify({
        riot_puuid: puuid,
        twitch_username: userData.twitch_username,
        riot_id: userData.riot_id,
        rank_tier: currentRank ? currentRank.tier : 'UNRANKED',
        rank_division: currentRank ? currentRank.rank : null,
        lp: currentRank ? currentRank.leaguePoints : 0,
        region: userData.region
      })
    });

    const updateResponse = await env.RANK_WORKER.fetch(updateRequest);
    if (!updateResponse.ok) {
      throw new Error(`Failed to update rank: ${updateResponse.status}`);
    }

    // Peak-rank seeding intentionally does NOT run on refresh. It only runs
    // once, on initial Riot account connect (see the /riot/auth handler).
    // Re-scraping op.gg on every refresh risked overwriting a true peak with
    // whatever partial/lagging data op.gg happened to expose -- which is how
    // users got their peak silently demoted. Regular refreshes go through
    // rank-worker.storeRank, which already raises peak monotonically from
    // Riot's current-rank data when current > stored peak.

    // Get final user data (rank-worker returns correct current/peak based on show_peak setting)
    const getFinalUserRequest = new Request('https://rank-worker/api/ranks/lol/by-puuid', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify({ puuid })
    });
    
    const finalUserResponse = await env.RANK_WORKER.fetch(getFinalUserRequest);
    if (!finalUserResponse.ok) {
      throw new Error(`Failed to retrieve updated data: ${finalUserResponse.status}`);
    }

    const finalUserData = await finalUserResponse.json();

    return corsResponse(200, {
      status: 'success',
      message: 'Rank refreshed successfully',
      data: {
        rank_tier: finalUserData.rank_tier,
        rank_division: finalUserData.rank_division,
        lp: finalUserData.lp,
        region: finalUserData.region,
        plus_active: finalUserData.plus_active || false
      }
    });
  } catch (error) {
    return corsResponse(500, {
      status: 'error',
      message: 'Failed to refresh rank',
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// Secure endpoint for fetching and storing rank data
// Removed legacy /store-rank endpoint

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
        'X-Internal-Auth': env.RANK_WRITE_KEY
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
router.all('*', (request) => {
  return corsResponse(404, { 
    error: 'not_found',
    error_description: 'Endpoint not found' 
  }, request);
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
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
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