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
  SCRAPERAPI_KEY: string; // ScraperAPI key for bypassing Cloudflare
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

const DIVISION_ORDER: Record<string, number> = { '4': 1, '3': 2, '2': 3, '1': 4 };

// OpGG Scraper class using ScraperAPI to bypass Cloudflare
class OpGGScraper {
  private scraperApiKey: string;

  constructor(scraperApiKey: string) {
    this.scraperApiKey = scraperApiKey;
  }

  async scrapeUrl(url: string): Promise<string> {
    // Use ScraperAPI to bypass Cloudflare blocking
    const scraperApiUrl = `http://api.scraperapi.com?api_key=${this.scraperApiKey}&url=${encodeURIComponent(url)}`;
    
    console.log(`[OpGGScraper] Using ScraperAPI to fetch: ${url}`);
    
    const response = await fetch(scraperApiUrl, {
      method: 'GET',
      // ScraperAPI handles headers and retries automatically
    });

    console.log(`[OpGGScraper] ScraperAPI response status: ${response.status} ${response.statusText}`);

    if (!response.ok) {
      // ScraperAPI specific error handling
      if (response.status === 404) {
        throw new Error(`Profile not found: HTTP 404`);
      } else if (response.status === 429) {
        throw new Error(`Rate limit exceeded: HTTP 429`);
      } else if (response.status === 403) {
        throw new Error(`Access forbidden: HTTP 403`);
      } else {
        throw new Error(`ScraperAPI error: HTTP ${response.status} ${response.statusText}`);
      }
    }

    const text = await response.text();
    console.log(`[OpGGScraper] Successfully fetched ${text.length} characters via ScraperAPI`);
    return text;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  extractAllRanksFromHTML(html: string): any[] {
    const allRanks: any[] = [];
    
    // Extract current rank
    const currentRank = this.extractCurrentRank(html);
    if (currentRank) {
      allRanks.push(currentRank);
    }

    // Extract peak rank
    const peakRank = this.extractPeakRank(html);
    if (peakRank) {
      allRanks.push(peakRank);
    }

    // Extract historical ranks
    const historicalRanks = this.extractHistoricalRanks(html);
    allRanks.push(...historicalRanks);

    return allRanks;
  }

  private extractCurrentRank(html: string): any | null {
    const currentPattern = /<strong class="text-xl first-letter:uppercase">([^<]+)<\/strong>[\s\S]*?<span class="text-xs text-gray-500">([0-9,]+)(?:<!--[^>]*-->)?\s*LP<\/span>[\s\S]*?<span class="leading-\[26px\]">(\d+)(?:<!--[^>]*-->)?W(?:<!--[^>]*-->)?\s*(?:<!--[^>]*-->)?(\d+)(?:<!--[^>]*-->)?L<\/span>[\s\S]*?<span>Win rate(?:<!--[^>]*-->)?\s*(?:<!--[^>]*-->)?(\d+)(?:<!--[^>]*-->)?%<\/span>/;
    
    const match = html.match(currentPattern);
    if (match) {
      const rankText = match[1]?.trim().toLowerCase();
      const lp = parseInt(match[2]?.replace(/[^0-9]/g, '')) || 0;
      const wins = parseInt(match[3]) || 0;
      const losses = parseInt(match[4]) || 0;
      const winRate = parseInt(match[5]) || 0;
      
      const rankParts = rankText.split(/\s+/);
      const tier = rankParts[0]?.toUpperCase();
      const division = rankParts[1] || null;
      
      if (this.isValidTier(tier)) {
        return {
          tier,
          division,
          lp,
          wins,
          losses,
          winRate,
          type: 'current'
        };
      }
    }
    
    return null;
  }

  private extractPeakRank(html: string): any | null {
    const peakPattern = /<strong class="text-sm first-letter:uppercase">([^<]+)<\/strong>[\s\S]*?<span class="text-xs text-gray-500">([0-9,]+)(?:<!--[^>]*-->)?\s*LP<\/span>[\s\S]*?<span[^>]*>Top Tier<\/span>/;
    
    const match = html.match(peakPattern);
    if (match) {
      const rankText = match[1]?.trim().toLowerCase();
      const lp = parseInt(match[2]?.replace(/[^0-9]/g, '')) || 0;
      
      const rankParts = rankText.split(/\s+/);
      const tier = rankParts[0]?.toUpperCase();
      const division = rankParts[1] || null;
      
      if (this.isValidTier(tier)) {
        return {
          tier,
          division,
          lp,
          type: 'peak'
        };
      }
    }
    
    return null;
  }

  private extractHistoricalRanks(html: string): any[] {
    const ranks: any[] = [];
    
    const tableRowPattern = /<tr class="bg-main-100[^"]*"[^>]*>.*?<strong[^>]*>(S\d{4}[^<]*)<\/strong>.*?<span class="text-xs lowercase first-letter:uppercase">([^<]+)<\/span>.*?<td align="right" class="text-xs text-gray-500">([0-9,]+)<\/td>/gs;
    
    let match;
    while ((match = tableRowPattern.exec(html)) !== null) {
      const season = match[1]?.trim();
      const rankText = match[2]?.trim().toLowerCase();
      const lp = parseInt(match[3]?.replace(/[^0-9]/g, '')) || 0;
      
      const rankParts = rankText.split(/\s+/);
      const tier = rankParts[0]?.toUpperCase();
      const division = rankParts[1] || null;
      
      if (this.isValidTier(tier)) {
        ranks.push({
          tier,
          division,
          lp,
          season,
          type: 'historical'
        });
      }
    }
    
    return ranks;
  }

  private isValidTier(tier: string): boolean {
    const validTiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER'];
    return validTiers.includes(tier);
  }

  findHighestRank(ranks: any[]): any | null {
    if (ranks.length === 0) return null;
    
    return ranks.reduce((highest, current) => {
      const currentValue = this.calculateRankValue(current);
      const highestValue = this.calculateRankValue(highest);
      
      if (currentValue > highestValue) {
        return current;
      } else if (currentValue === highestValue && current.lp > highest.lp) {
        return current;
      }
      
      return highest;
    });
  }

  private calculateRankValue(rank: any): number {
    const tierValue = RANK_ORDER[rank.tier] || 0;
    const divisionValue = DIVISION_ORDER[rank.division] || 5;
    return tierValue * 10000 + divisionValue * 1000 + Math.min(rank.lp || 0, 9999);
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
    console.log(`[PeakSeed] Starting peak rank seeding for ${riotId} (${region})`);
    
    const opggRegion = REGION_MAPPING[region.toLowerCase()];
    if (!opggRegion) {
      console.log(`[PeakSeed] Unknown region ${region} - keeping existing peak (current rank)`);
      return;
    }

    // Construct op.gg URL like extension: gameName-tagLine?queue_type=SOLORANKED
    const riotIdParts = riotId.split('#');
    const encodedName = encodeURIComponent(riotIdParts[0] || '');
    const tagLine = riotIdParts[1] || region.toUpperCase();
    const opggUrl = `https://op.gg/lol/summoners/${opggRegion}/${encodedName}-${tagLine}?queue_type=SOLORANKED`;
    
    console.log(`[PeakSeed] Scraping op.gg URL: ${opggUrl}`);

    // Attempt scraping with ScraperAPI (handles retries automatically for up to 70 seconds)
    let peakRank: any = null;
    const scraper = new OpGGScraper(env.SCRAPERAPI_KEY);
    
    try {
      console.log(`[PeakSeed] Scraping via ScraperAPI...`);
      const html = await scraper.scrapeUrl(opggUrl);
      console.log(`[PeakSeed] HTML scraped successfully, extracting ranks...`);
      
      const allRanks = scraper.extractAllRanksFromHTML(html);
      console.log(`[PeakSeed] Extracted ${allRanks.length} ranks:`, allRanks);
      
      if (allRanks.length > 0) {
        peakRank = scraper.findHighestRank(allRanks);
        console.log(`[PeakSeed] Highest rank found:`, peakRank);
        if (peakRank) {
          console.log(`[PeakSeed] Peak rank confirmed: ${peakRank.tier} ${peakRank.division} ${peakRank.lp}LP`);
        }
      } else {
        console.log(`[PeakSeed] No ranks found in scraped HTML data`);
      }
    } catch (error) {
      console.error(`[PeakSeed] ScraperAPI scraping failed:`, error instanceof Error ? error.message : String(error));
    }

    // Update peak rank in database only if we found historical data
    if (peakRank) {
      console.log(`[PeakSeed] Successfully scraped peak rank, updating database`);
      await updatePeakRank(puuid, riotId, region, peakRank, env);
    } else {
      console.log(`[PeakSeed] No historical peak rank found - keeping existing peak (current rank)`);
    }

  } catch (error) {
    console.error(`[PeakSeed] Error during peak seeding: ${error instanceof Error ? error.message : String(error)}`);
    console.log(`[PeakSeed] Keeping existing peak rank (current rank) due to scraping failure`);
  }
}

async function updatePeakRank(puuid: string, riotId: string, region: string, peakRank: any, env: Env): Promise<void> {
  try {
    console.log(`[PeakSeed] Starting database update for ${riotId}`);
    
    // Get current data (optimized - only fetch what we need)
    const lookupRequest = new Request('https://rank-worker/api/ranks/lol/by-puuid', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify({ puuid: puuid })
    });

    console.log(`[PeakSeed] Fetching existing user data...`);
    const lookupResponse = await env.RANK_WORKER.fetch(lookupRequest);
    if (!lookupResponse.ok) {
      const errorText = await lookupResponse.text();
      console.error(`[PeakSeed] Failed to lookup existing user data: ${lookupResponse.status} - ${errorText}`);
      return;
    }

    const existingData = await lookupResponse.json();
    console.log(`[PeakSeed] Current user data:`, existingData);
    
    // Normalize division for database (Master+ ALWAYS gets 'I', others keep their divisions)
    let normalizedDivision = peakRank.division;
    if (['MASTER', 'GRANDMASTER', 'CHALLENGER'].includes(peakRank.tier)) {
      normalizedDivision = 'I'; // Master+ always gets 'I' regardless of scraped division
    }

    // Update with explicit peak rank override - preserves current rank data
    const updateData = {
      riot_puuid: puuid,
      twitch_username: existingData.twitch_username,
      riot_id: riotId,
      rank_tier: existingData.rank_tier,
      rank_division: existingData.rank_division,
      lp: existingData.lp,
      region: region,
      plus_active: existingData.plus_active,
      // Explicit peak rank override parameters
      peak_rank_tier: peakRank.tier,
      peak_rank_division: normalizedDivision,
      peak_lp: peakRank.lp || 0
    };

    console.log(`[PeakSeed] Update payload:`, updateData);

    const updateRequest = new Request('https://rank-worker/api/ranks/lol', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Auth': env.RANK_WRITE_KEY
      },
      body: JSON.stringify(updateData)
    });

    console.log(`[PeakSeed] Sending update request to rank worker...`);
    const response = await env.RANK_WORKER.fetch(updateRequest);
    
    if (response.ok) {
      const result = await response.json();
      console.log(`[PeakSeed] Successfully updated peak rank:`, result);
    } else {
      const errorText = await response.text();
      console.error(`[PeakSeed] Failed to update peak rank: ${response.status} - ${errorText}`);
    }
  } catch (error) {
    console.error(`[PeakSeed] Error updating peak rank:`, error instanceof Error ? error.message : String(error));
    console.error(`[PeakSeed] Error stack:`, error instanceof Error ? error.stack : 'No stack trace');
  }
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

    // Step 6.5: Trigger async peak rank seeding after successful connection
    // This runs in background without blocking user response
    const storeResult = await storeResponse.json();
    if (storeResult.success) {
      console.log(`[RiotAuth] Triggering async peak rank seeding for ${riotId}`);
      // Use ctx.waitUntil to keep the async function alive after request completes
      ctx.waitUntil(
        seedPeakRankAsync(
          accountData.puuid,
          riotId,
          region,
          currentRank, // Pass current rank (could be null for unranked)
          env
        ).catch(error => {
          console.error(`[RiotAuth] Peak rank seeding failed for ${riotId}: ${error instanceof Error ? error.message : String(error)}`);
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
        'X-Internal-Auth': env.RANK_WRITE_KEY
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
        'X-Internal-Auth': env.RANK_WRITE_KEY
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