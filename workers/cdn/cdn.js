// EloWard CDN Worker
// Serves rank badge images from R2 with structure: [game]/[filename]

const worker = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // Handle CORS preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Max-Age': '86400'
        }
      });
    }
    
    // Only allow GET requests
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }
    
    // Parse the path: /[game]/[filename]
    const pathMatch = path.match(/^\/([^/]+)\/([^/]+)$/);
    if (!pathMatch) {
      return new Response('Invalid path format. Expected: /[game]/[filename]', { 
        status: 400,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
    
    const [, game, filename] = pathMatch;
    const objectKey = `${game}/${filename}`;
    
    // Validate game and filename
    if (!isValidGame(game)) {
      return new Response(`Invalid game: ${game}`, { 
        status: 400,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
    
    if (!isValidFilename(filename)) {
      return new Response(`Invalid filename: ${filename}`, { 
        status: 400,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
    
    // Check cache first
    const cache = caches.default;
    const cacheKey = new Request(request.url, request);
    let response = await cache.match(cacheKey);
    
    if (response) {
      response = new Response(response.body, response);
      response.headers.set('X-Cache', 'HIT');
      return response;
    }
    
    try {
      // Fetch from R2
      const object = await env.ELOWARD_BADGES.get(objectKey);
      
      if (!object) {
        return new Response(`Badge not found: ${objectKey}`, { 
          status: 404,
          headers: { 
            'Content-Type': 'text/plain',
            'Access-Control-Allow-Origin': '*'
          }
        });
      }
      
      // Determine content type based on file extension
      const contentType = getContentType(filename);
      
      // Create response with optimized headers
      response = new Response(object.body, {
        headers: {
          'Content-Type': contentType,
          'Cache-Control': 'public, max-age=31536000, immutable', // 1 year
          'ETag': object.etag,
          'Last-Modified': object.uploaded.toUTCString(),
          'Access-Control-Allow-Origin': '*',
          'X-Cache': 'MISS',
          'X-Game': game,
          'X-Filename': filename
        }
      });
      
      // Cache the response
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
      
      return response;
      
    } catch (error) {
      console.error(`Error fetching ${objectKey} from R2:`, error);
      
      return new Response('Internal Server Error', {
        status: 500,
        headers: { 
          'Content-Type': 'text/plain',
          'Access-Control-Allow-Origin': '*'
        }
      });
    }
  }
};

export default worker;

// Validate supported games
function isValidGame(game) {
  const supportedGames = [
    'lol',      // League of Legends
    'chess',    // Chess
    'valorant', // Valorant
    'dota2',    // Dota 2
    'csgo',     // Counter-Strike
    'rocket',   // Rocket League
    'apex'      // Apex Legends
  ];
  
  return supportedGames.includes(game.toLowerCase());
}

// Validate filename format and security
function isValidFilename(filename) {
  // Allow alphanumeric, hyphens, underscores, and common image extensions
  const validPattern = /^[a-zA-Z0-9_-]+\.(png|jpg|jpeg|webp|svg)$/i;
  
  // Prevent path traversal attacks
  if (filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
    return false;
  }
  
  return validPattern.test(filename);
}

// Get content type based on file extension
function getContentType(filename) {
  const extension = filename.toLowerCase().split('.').pop();
  
  const contentTypes = {
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'webp': 'image/webp',
    'svg': 'image/svg+xml'
  };
  
  return contentTypes[extension] || 'application/octet-stream';
}
