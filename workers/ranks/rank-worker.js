const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const worker = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // Lightweight internal auth for write operations (POST/DELETE)
    const authorizeInternal = () => {
      const provided = request.headers.get('X-Internal-Auth');
      const expected = env.RANK_WRITE_KEY;
      return Boolean(expected) && provided === expected;
    };
    
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }
    
    if (path === "/api/ranks/lol") {
      if (request.method === "POST") {
        if (!authorizeInternal()) {
          return new Response(JSON.stringify({ error: "Forbidden" }), { status: 403, headers: corsHeaders });
        }
        return storeRank(request, env);
      }
      if (request.method === "DELETE") {
        if (!authorizeInternal()) {
          return new Response(JSON.stringify({ error: "Forbidden" }), { status: 403, headers: corsHeaders });
        }
        return deleteRank(request, env);
      }
    }
    
    const getUserMatch = path.match(/^\/api\/ranks\/lol\/([^/]+)$/);
    if (getUserMatch && request.method === "GET") {
      return getRank(getUserMatch[1].toLowerCase(), env);
    }
    
    if (path === "/api/ranks/lol/by-puuid" && request.method === "POST") {
      if (!authorizeInternal()) {
        return new Response(JSON.stringify({ error: "Forbidden" }), { status: 403, headers: corsHeaders });
      }
      return getRankByPuuid(request, env);
    }
    
    return new Response("Not found", { status: 404, headers: corsHeaders });
  },

  async scheduled(event, env, ctx) {
    // Cron job for automatic rank refreshes
    try {
      console.log(`[RankCron] Started scheduled rank refresh at ${new Date().toISOString()}`);
      await batchRefreshAllRanks(env, ctx);
      console.log(`[RankCron] Completed scheduled rank refresh at ${new Date().toISOString()}`);
    } catch (error) {
      console.error(`[RankCron] Error during scheduled refresh:`, error);
      // Don't throw - let the cron complete and try again next time
    }
  }
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders }
  });
}

// Robust rank comparison following League of Legends hierarchy
const TIER_ORDER = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER'];
const TIER_VALUES = Object.fromEntries(TIER_ORDER.map((tier, idx) => [tier, idx]));
const DIVISION_VALUES = { 'I': 1, 'II': 2, 'III': 3, 'IV': 4 }; // Lower number = higher rank

function isRankHigher(newRank, currentRank) {
  // Handle null/undefined cases
  if (!currentRank || !currentRank.rank_tier) return true;
  if (!newRank || !newRank.rank_tier) return false;
  
  // Step 1: Compare tiers (CHALLENGER > GRANDMASTER > MASTER > ... > IRON)
  const newTierValue = TIER_VALUES[newRank.rank_tier?.toUpperCase()] ?? -1;
  const currentTierValue = TIER_VALUES[currentRank.rank_tier?.toUpperCase()] ?? -1;
  
  if (newTierValue === -1 || currentTierValue === -1) {
    return false; // Invalid tier data
  }
  
  if (newTierValue !== currentTierValue) {
    return newTierValue > currentTierValue; // Higher tier value = higher rank
  }
  
  // Step 2: Same tier - for Master+ skip division comparison, go to LP
  if (newTierValue >= 7) { // MASTER, GRANDMASTER, CHALLENGER
    const newLP = parseInt(newRank.lp) || 0;
    const currentLP = parseInt(currentRank.lp) || 0;
    return newLP > currentLP;
  }
  
  // Step 3: For ranks below Master, compare divisions (I > II > III > IV)
  const newDivision = DIVISION_VALUES[newRank.rank_division?.toUpperCase()] || 4; // Default to IV
  const currentDivision = DIVISION_VALUES[currentRank.rank_division?.toUpperCase()] || 4;
  
  if (newDivision !== currentDivision) {
    return newDivision < currentDivision; // Lower division number = higher rank
  }
  
  // Step 4: Same tier and division, compare LP (0 is valid, higher LP = higher rank)
  const newLP = parseInt(newRank.lp) || 0;
  const currentLP = parseInt(currentRank.lp) || 0;
  return newLP > currentLP;
}

async function storeRank(request, env) {
  try {
    const { 
      riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active,
      // Optional peak rank override parameters for seeding
      peak_rank_tier, peak_rank_division, peak_lp
    } = await request.json();
    
    if (!riot_puuid || !twitch_username || !rank_tier) {
      return jsonResponse({ 
        error: "Missing required fields", 
        required: ["riot_puuid", "twitch_username", "rank_tier"] 
      }, 400);
    }
    
    // Handle plus_active conditionally
    const shouldUpdatePlusActive = plus_active !== undefined;
    const plusActiveValue = plus_active ? 1 : 0;
    
    // Determine peak rank values - use explicit override if provided, otherwise calculate
    let peakTier, peakDivision, peakLp, updatePeak = false;
    
    if (peak_rank_tier !== undefined) {
      // Explicit peak rank provided (e.g., from op.gg seeding)
      // Allow null/undefined for peak_rank_division (Master+ ranks) and peak_lp
      peakTier = peak_rank_tier;
      peakDivision = peak_rank_division !== undefined ? peak_rank_division : null;
      peakLp = peak_lp !== undefined ? peak_lp : 0;
      updatePeak = true; // Mark as explicit update
      console.log(`[RankWorker] Using explicit peak rank: ${peakTier} ${peakDivision} ${peakLp}LP`);
    } else {
      // Calculate peak rank automatically (existing logic)
      const existingData = await env.DB.prepare(
        "SELECT peak_rank_tier, peak_rank_division, peak_lp FROM lol_ranks WHERE riot_puuid = ?"
      ).bind(riot_puuid).first();
      
      const newRank = { rank_tier, rank_division, lp: lp || 0 };
      const currentPeak = existingData ? {
        rank_tier: existingData.peak_rank_tier,
        rank_division: existingData.peak_rank_division,
        lp: existingData.peak_lp
      } : null;
      
      // Check if new rank is higher than current peak using robust comparison
      updatePeak = isRankHigher(newRank, currentPeak);
      
      // Update all peak fields if new rank is higher, otherwise preserve existing peak
      peakTier = updatePeak ? rank_tier : (currentPeak?.rank_tier || rank_tier);
      peakDivision = updatePeak ? (rank_division || null) : (currentPeak?.rank_division || rank_division || null);
      peakLp = updatePeak ? (lp ?? 0) : (currentPeak?.lp ?? 0); // Preserve existing peak LP, default to 0 if null
    }
    
    let result;
    if (shouldUpdatePlusActive) {
      result = await env.DB.prepare(`
        INSERT INTO lol_ranks (riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, peak_rank_tier, peak_rank_division, peak_lp, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (riot_puuid) DO UPDATE SET
          twitch_username = excluded.twitch_username,
          riot_id = excluded.riot_id,
          rank_tier = excluded.rank_tier,
          rank_division = excluded.rank_division,
          lp = excluded.lp,
          region = excluded.region,
          plus_active = excluded.plus_active,
          peak_rank_tier = excluded.peak_rank_tier,
          peak_rank_division = excluded.peak_rank_division,
          peak_lp = excluded.peak_lp,
          last_updated = excluded.last_updated
      `).bind(
        riot_puuid,
        twitch_username.toLowerCase(),
        riot_id || null,
        rank_tier,
        rank_division || null,
        lp || 0,
        region || null,
        plusActiveValue,
        peakTier,
        peakDivision,
        peakLp,
        Math.floor(Date.now() / 1000)
      ).run();
    } else {
      result = await env.DB.prepare(`
        INSERT INTO lol_ranks (riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, peak_rank_tier, peak_rank_division, peak_lp, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        ON CONFLICT (riot_puuid) DO UPDATE SET
          twitch_username = excluded.twitch_username,
          riot_id = excluded.riot_id,
          rank_tier = excluded.rank_tier,
          rank_division = excluded.rank_division,
          lp = excluded.lp,
          region = excluded.region,
          peak_rank_tier = excluded.peak_rank_tier,
          peak_rank_division = excluded.peak_rank_division,
          peak_lp = excluded.peak_lp,
          last_updated = excluded.last_updated
      `).bind(
        riot_puuid,
        twitch_username.toLowerCase(),
        riot_id || null,
        rank_tier,
        rank_division || null,
        lp || 0,
        region || null,
        peakTier,
        peakDivision,
        peakLp,
        Math.floor(Date.now() / 1000)
      ).run();
    }
    
    return jsonResponse({ 
      success: true, 
      riot_puuid,
      twitch_username,
      rank_tier,
      rank_division,
      lp,
      region,
      peak_updated: peak_rank_tier !== undefined ? 'explicit_override' : (updatePeak ? 'rank_comparison' : false),
      changes: result.changes || 0
    });
  } catch (error) {
    console.error("Error storing rank data:", error);
    return jsonResponse({ 
      error: "Failed to store rank data", 
      details: error.message 
    }, 500);
  }
}

async function getRank(username, env) {
  try {
    const result = await env.DB.prepare(
      "SELECT twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated, peak_rank_tier, peak_rank_division, peak_lp, show_peak FROM lol_ranks WHERE twitch_username = ?"
    ).bind(username).first();
    
    if (!result) {
      return jsonResponse({ error: "User rank not found" }, 404);
    }
    
    // Return peak rank data if show_peak is true, otherwise current rank
    const responseData = {
      twitch_username: result.twitch_username,
      riot_id: result.riot_id,
      rank_tier: result.show_peak ? result.peak_rank_tier : result.rank_tier,
      rank_division: result.show_peak ? result.peak_rank_division : result.rank_division,
      lp: result.show_peak ? result.peak_lp : result.lp,
      region: result.region,
      plus_active: result.plus_active,
      last_updated: result.last_updated,
      show_peak: result.show_peak
    };
    
    return jsonResponse(responseData);
  } catch (error) {
    console.error(`Error fetching rank for ${username}:`, error);
    return jsonResponse({ error: "Failed to retrieve rank", details: error.message }, 500);
  }
}

async function deleteRank(request, env) {
  try {
    const { puuid } = await request.json();
    
    if (!puuid) {
      return jsonResponse({ error: "Missing puuid parameter" }, 400);
    }
    
    const result = await env.DB.prepare("DELETE FROM lol_ranks WHERE riot_puuid = ?").bind(puuid).run();
    
    if (result.changes === 0) {
      return jsonResponse({ error: "User rank not found" }, 404);
    }
    
    return jsonResponse({ success: true, deleted: result.changes });
  } catch (error) {
    console.error("Error deleting rank:", error);
    return jsonResponse({ error: "Failed to delete rank", details: error.message }, 500);
  }
}

async function getRankByPuuid(request, env) {
  let requestedPuuid = undefined;
  try {
    const body = await request.json();
    requestedPuuid = body?.puuid;
    
    if (!requestedPuuid) {
      return jsonResponse({ error: "Missing puuid parameter" }, 400);
    }
    
    const result = await env.DB.prepare(
      "SELECT twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated, peak_rank_tier, peak_rank_division, peak_lp, show_peak FROM lol_ranks WHERE riot_puuid = ?"
    ).bind(requestedPuuid).first();
    
    if (!result) {
      return jsonResponse({ error: "User rank not found" }, 404);
    }
    
    // Return peak rank data if show_peak is true, otherwise current rank
    const responseData = {
      twitch_username: result.twitch_username,
      riot_id: result.riot_id,
      rank_tier: result.show_peak ? result.peak_rank_tier : result.rank_tier,
      rank_division: result.show_peak ? result.peak_rank_division : result.rank_division,
      lp: result.show_peak ? result.peak_lp : result.lp,
      region: result.region,
      plus_active: result.plus_active,
      last_updated: result.last_updated,
      show_peak: result.show_peak
    };
    
    return jsonResponse(responseData);
  } catch (error) {
    console.error(`Error fetching rank for puuid ${requestedPuuid || 'unknown'}:`, error);
    return jsonResponse({ error: "Failed to retrieve rank", details: error.message }, 500);
  }
}

// Batch refresh all ranks via cron job
async function batchRefreshAllRanks(env, ctx) {
  // Security validation - ensure this is a legitimate cron execution
  if (!env.CRON_SECRET_KEY) {
    throw new Error('CRON_SECRET_KEY not configured');
  }

  console.log('[RankCron] Starting batch rank refresh process');

  try {
    // Fetch PUUIDs that need refreshing (only users updated more than 24 hours ago)
    const twentyFourHoursAgo = Math.floor(Date.now() / 1000) - (24 * 60 * 60); // 86400 seconds
    const result = await env.DB.prepare(
      "SELECT riot_puuid, twitch_username, last_updated FROM lol_ranks WHERE last_updated < ? ORDER BY last_updated ASC"
    ).bind(twentyFourHoursAgo).all();

    if (!result.results || result.results.length === 0) {
      console.log('[RankCron] No ranks found that need refreshing (all updated within 24 hours)');
      return;
    }

    const puuids = result.results;
    console.log(`[RankCron] Found ${puuids.length} ranks to refresh (updated more than 24 hours ago)`);

    // Process in batches of 50 to maximize API utilization (40 req/sec vs 50 req/sec limit - 80% utilization)
    const BATCH_SIZE = 50;
    const DELAY_BETWEEN_BATCHES = 250; // 250ms between batches
    const DELAY_BETWEEN_REQUESTS = 20; // 20ms between individual requests

    let successCount = 0;
    let failureCount = 0;

    for (let i = 0; i < puuids.length; i += BATCH_SIZE) {
      const batch = puuids.slice(i, i + BATCH_SIZE);
      console.log(`[RankCron] Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(puuids.length / BATCH_SIZE)}`);

      // Process each item in the batch with small delays
      for (const user of batch) {
        try {
          await refreshSingleRank(user.riot_puuid, env);
          successCount++;
          console.log(`[RankCron] ✓ Refreshed rank for ${user.twitch_username} (${user.riot_puuid})`);
        } catch (error) {
          failureCount++;
          console.error(`[RankCron] ✗ Failed to refresh rank for ${user.twitch_username} (${user.riot_puuid}):`, error.message);
        }

        // Small delay between individual requests
        if (batch.indexOf(user) < batch.length - 1) {
          await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_REQUESTS));
        }
      }

      // Delay between batches (except for the last batch)
      if (i + BATCH_SIZE < puuids.length) {
        console.log(`[RankCron] Waiting ${DELAY_BETWEEN_BATCHES}ms before next batch...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
      }
    }

    console.log(`[RankCron] Batch refresh completed: ${successCount} successes, ${failureCount} failures out of ${puuids.length} total`);

  } catch (error) {
    console.error('[RankCron] Database error during batch refresh:', error);
    throw error;
  }
}

// Refresh a single rank by calling the riotauth-worker
async function refreshSingleRank(puuid, env) {
  if (!env.RIOTAUTH_WORKER) {
    throw new Error('RIOTAUTH_WORKER service binding not configured');
  }

  const request = new Request('https://riotauth-worker/riot/refreshrank', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ puuid })
  });

  const response = await env.RIOTAUTH_WORKER.fetch(request);
  
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Riotauth worker error (${response.status}): ${errorText}`);
  }

  const result = await response.json();
  if (result.status !== 'success') {
    throw new Error(result.message || 'Unknown error from riotauth worker');
  }

  return result;
}

export default worker;