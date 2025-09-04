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

async function storeRank(request, env) {
  try {
    const { riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active } = await request.json();
    
    if (!riot_puuid || !twitch_username || !rank_tier) {
      return jsonResponse({ 
        error: "Missing required fields", 
        required: ["riot_puuid", "twitch_username", "rank_tier"] 
      }, 400);
    }
    
    // Handle plus_active conditionally - only update if explicitly provided
    const shouldUpdatePlusActive = plus_active !== undefined;
    const plusActiveValue = plus_active ? 1 : 0;
    
    let result;
    if (shouldUpdatePlusActive) {
      // Update plus_active when explicitly provided (e.g., during auth/subscription changes)
      result = await env.DB.prepare(`
        INSERT INTO lol_ranks (riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (riot_puuid) DO UPDATE SET
          twitch_username = excluded.twitch_username,
          riot_id = excluded.riot_id,
          rank_tier = excluded.rank_tier,
          rank_division = excluded.rank_division,
          lp = excluded.lp,
          region = excluded.region,
          plus_active = excluded.plus_active,
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
        Math.floor(Date.now() / 1000)
      ).run();
    } else {
      // Preserve existing plus_active when not provided (e.g., during rank refresh)
      result = await env.DB.prepare(`
        INSERT INTO lol_ranks (riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT (riot_puuid) DO UPDATE SET
          twitch_username = excluded.twitch_username,
          riot_id = excluded.riot_id,
          rank_tier = excluded.rank_tier,
          rank_division = excluded.rank_division,
          lp = excluded.lp,
          region = excluded.region,
          last_updated = excluded.last_updated
      `).bind(
        riot_puuid,
        twitch_username.toLowerCase(),
        riot_id || null,
        rank_tier,
        rank_division || null,
        lp || 0,
        region || null,
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
      "SELECT twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated FROM lol_ranks WHERE twitch_username = ?"
    ).bind(username).first();
    
    if (!result) {
      return jsonResponse({ error: "User rank not found" }, 404);
    }
    
    return jsonResponse(result);
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
      "SELECT twitch_username, riot_id, rank_tier, rank_division, lp, region, plus_active, last_updated FROM lol_ranks WHERE riot_puuid = ?"
    ).bind(requestedPuuid).first();
    
    if (!result) {
      return jsonResponse({ error: "User rank not found" }, 404);
    }
    
    return jsonResponse(result);
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
    // Fetch PUUIDs that need refreshing (only users updated more than 24 hours ago, excluding specific PUUID)
    const twentyFourHoursAgo = Math.floor(Date.now() / 1000) - (24 * 60 * 60); // 86400 seconds
    const excludedPuuid = "MF5og4YOGryMysvRDNx62aYesoyMwErT3rgfwmAq31fEBaeFniXzHn-nLhRAhnjMsNj56ifO_MlXKw";
    const result = await env.DB.prepare(
      "SELECT riot_puuid, twitch_username, last_updated FROM lol_ranks WHERE last_updated < ? AND riot_puuid != ? ORDER BY last_updated ASC"
    ).bind(twentyFourHoursAgo, excludedPuuid).all();

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