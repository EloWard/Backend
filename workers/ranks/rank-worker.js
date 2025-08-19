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
      const expected = env.RANK_WRITE_KEY || env.INTERNAL_WRITE_KEY;
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
      return getRankByPuuid(request, env);
    }
    
    return new Response("Not found", { status: 404, headers: corsHeaders });
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
    const { riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region } = await request.json();
    
    if (!riot_puuid || !twitch_username || !rank_tier) {
      return jsonResponse({ 
        error: "Missing required fields", 
        required: ["riot_puuid", "twitch_username", "rank_tier"] 
      }, 400);
    }
    
    const result = await env.DB.prepare(`
      INSERT INTO lol_ranks (riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
      "SELECT riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, last_updated FROM lol_ranks WHERE twitch_username = ?"
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
      "SELECT riot_puuid, twitch_username, riot_id, rank_tier, rank_division, lp, region, last_updated FROM lol_ranks WHERE riot_puuid = ?"
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

export default worker;