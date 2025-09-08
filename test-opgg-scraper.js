#!/usr/bin/env node

/**
 * Op.gg Peak Rank Scraper Test Script (Browser-Free Version)
 * 
 * This script tests the complete op.gg scraping functionality using HTTP requests only:
 * - Multiple request strategies to get expanded season data
 * - Smart HTML parsing without browser automation
 * - Finding the absolute peak rank across ALL seasons
 * - Works in Cloudflare Workers environment
 * 
 * Usage: node test-opgg-scraper.js <opgg_url>
 * Example: node test-opgg-scraper.js "https://op.gg/lol/summoners/na/Spankers-CN1"
 */

// Rank hierarchy for comparison
const RANK_ORDER = {
  'IRON': 1, 'BRONZE': 2, 'SILVER': 3, 'GOLD': 4, 
  'PLATINUM': 5, 'EMERALD': 6, 'DIAMOND': 7,
  'MASTER': 8, 'GRANDMASTER': 9, 'CHALLENGER': 10
};

const DIVISION_ORDER = { '4': 1, '3': 2, '2': 3, '1': 4 };

class OpGGScraper {
  constructor() {
    this.userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
    this.defaultHeaders = {
      'User-Agent': this.userAgent,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/avif,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'Cache-Control': 'no-cache',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Connection': 'keep-alive'
    };
  }

  async fetchWithRetry(url, options = {}, retries = 3) {
    const headers = { ...this.defaultHeaders, ...options.headers };
    
    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(url, {
          method: 'GET',
          headers,
          ...options
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const html = await response.text();
        return html;
      } catch (error) {
        if (i === retries - 1) throw error;
        
        // Wait before retry with exponential backoff
        await this.sleep(1000 * Math.pow(2, i));
      }
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async scrapeAllStrategies(baseUrl) {
    // Strategy 1: Multiple URL variations that might contain expanded data
    const urlVariations = [
      baseUrl,
      `${baseUrl}?hl=en_US`,
      `${baseUrl}?region=global`,
      `${baseUrl}?locale=en_US`,
      baseUrl.replace('/summoners/', '/summoner/'), // Alternative format
      baseUrl + '/matches?queueId=420', // Ranked matches page might have more data
    ];

    let bestHtml = '';
    let bestSeasonCount = 0;
    let bestUrl = baseUrl;

    // Try each URL variation
    for (const url of urlVariations) {
      try {
        const html = await this.fetchWithRetry(url, {}, 2);
        const seasonCount = (html.match(/S20\d+/g) || []).length;
        
        if (seasonCount > bestSeasonCount) {
          bestHtml = html;
          bestSeasonCount = seasonCount;
          bestUrl = url;
        }
        
        await this.sleep(500);
      } catch (error) {
        // Silently continue to next URL
      }
    }

    if (!bestHtml) {
      throw new Error('All URL variations failed');
    }

    // Strategy 2: Look for AJAX endpoints in the HTML
    const ajaxUrls = this.findAjaxEndpoints(bestHtml, baseUrl);
    
    for (const ajaxUrl of ajaxUrls) {
      try {
        const ajaxHtml = await this.fetchWithRetry(ajaxUrl, {
          headers: {
            'Referer': bestUrl,
            'X-Requested-With': 'XMLHttpRequest'
          }
        }, 1);
        
        const ajaxSeasons = (ajaxHtml.match(/S20\d+/g) || []).length;
        if (ajaxSeasons > bestSeasonCount) {
          bestHtml = ajaxHtml;
          bestSeasonCount = ajaxSeasons;
        }
      } catch (error) {
        // Silently continue to next AJAX URL
      }
    }

    return bestHtml;
  }

  findAjaxEndpoints(html, baseUrl) {
    const endpoints = [];
    const urlBase = new URL(baseUrl);
    
    // Look for potential AJAX URLs in the HTML
    const patterns = [
      /fetch\(['"`]([^'"`]+)['"`]/g,
      /xhr\.open\([^,]+,\s*['"`]([^'"`]+)['"`]/g,
      /"api[^"]*seasons[^"]*"/g,
      /"[^"]*\/api\/[^"]*"/g,
    ];
    
    for (const pattern of patterns) {
      let match;
      while ((match = pattern.exec(html)) !== null) {
        let endpoint = match[1] || match[0].replace(/"/g, '');
        
        // Convert relative URLs to absolute
        if (endpoint.startsWith('/')) {
          endpoint = `${urlBase.protocol}//${urlBase.host}${endpoint}`;
        } else if (!endpoint.startsWith('http')) {
          continue;
        }
        
        if (endpoint.includes('season') || endpoint.includes('rank') || endpoint.includes('tier')) {
          endpoints.push(endpoint);
        }
      }
    }
    
    return [...new Set(endpoints)]; // Remove duplicates
  }

  extractAllRanksFromHTML(html) {
    const allRanks = [];
    
    const soloQueueHtml = this.isolateRankedSoloSection(html);
    const targetHtml = soloQueueHtml || html;
    
    const jsonRanks = this.extractFromJSON(html);
    allRanks.push(...jsonRanks);

    const htmlRanks = this.extractFromHTML(targetHtml);
    allRanks.push(...htmlRanks);

    const currentRanks = this.extractCurrentRank(html);
    allRanks.push(...currentRanks);

    const topTierRanks = this.extractTopTierRank(html);
    allRanks.push(...topTierRanks);

    const uniqueRanks = this.removeDuplicates(allRanks);
    return uniqueRanks;
  }

  /**
   * Isolates the "Ranked Solo/Duo" section from the full HTML
   * This is crucial to avoid mixing Solo/Duo ranks with Ranked Flex ranks
   */
  isolateRankedSoloSection(html) {
    const soloTablePattern = /<table[^>]*>[\s\S]*?<caption>Ranked Solo\/Duo<\/caption>[\s\S]*?<\/table>/gi;
    const tableMatch = html.match(soloTablePattern);
    
    if (tableMatch) {
      const expandedPattern = /<div[^>]*class="[^"]*flex[^"]*"[^>]*>[\s\S]*?<strong[^>]*class="text-xl[^"]*"[^>]*>[^<]*<\/strong>[\s\S]*?<table[^>]*>[\s\S]*?<caption>Ranked Solo\/Duo<\/caption>[\s\S]*?<\/table>[\s\S]*?(?:<button[^>]*>Close|$)/gi;
      const expandedMatch = html.match(expandedPattern);
      
      if (expandedMatch) {
        return expandedMatch[0];
      } else {
        return tableMatch[0];
      }
    }
    
    const fallbackPattern = /(?:Ranked\s+)?Solo[\s\/]*Duo[\s\S]*?(?=(?:Ranked\s+)?Flex|<\/html>|$)/gi;
    const fallbackMatch = html.match(fallbackPattern);
    
    if (fallbackMatch) {
      return fallbackMatch[0];
    }
    
    return null;
  }

  extractFromJSON(html) {
    const ranks = [];
    
    const tooltipPattern = /data-tooltip-html="[^"]*&lt;strong class=&quot;text-white first-letter:uppercase&quot;&gt;([^&]+)&lt;\/strong&gt;&lt;span class=&quot;text-xs[^&]*&quot;&gt;([0-9,]+) LP&lt;\/span&gt;[^"]*Top Tier/gi;
    let match;
    while ((match = tooltipPattern.exec(html)) !== null) {
      const tier = match[1]?.trim().toUpperCase();
      const lp = parseInt(match[2]?.replace(/[^\d]/g, '')) || 0;
      
      if (this.isValidTier(tier) && lp > 0) {
        ranks.push({ tier, division: null, lp, source: 'tooltip_peak' });
      }
    }

    const challengerJsonPattern = /challenger[^}]*"children":\s*\[\s*"([0-9,\s]+)"/gi;
    while ((match = challengerJsonPattern.exec(html)) !== null) {
      const lpText = match[1].replace(/[\s,]/g, '');
      const lp = parseInt(lpText) || 0;
      
      if (lp > 500) {
        ranks.push({ tier: 'CHALLENGER', division: null, lp, source: 'json_challenger' });
      }
    }
    
    const jsonRankPattern = /(S20\d+)[^}]*"rank_info":\s*\{\s*"tier":\s*"([^"]+)"\s*,\s*"lp":\s*"([^"]+)"/gi;
    while ((match = jsonRankPattern.exec(html)) !== null) {
      const season = match[1]?.trim();
      const tierText = match[2]?.trim().toLowerCase();
      const lp = parseInt(match[3]) || 0;
      
      if (season && tierText && lp >= 0) {
        const tierParts = tierText.split(/\s+/);
        const tier = tierParts[0]?.toUpperCase();
        const division = tierParts[1] || null;
        
        if (this.isValidTier(tier)) {
          ranks.push({ tier, division, lp, source: `json_${season}` });
        }
      }
    }

    return ranks;
  }


  extractFromHTML(html) {
    const ranks = [];
    
    const tableRowPattern = /<tr[^>]*class="[^"]*bg-main-100[^"]*"[^>]*>[\s\S]*?<strong[^>]*>(S20\d+[^<]*)<\/strong>[\s\S]*?<span[^>]*class="[^"]*text-xs[^"]*lowercase[^"]*"[^>]*>\s*([a-z]+(?:\s+\d+)?)\s*<\/span>[\s\S]*?<td[^>]*align="right"[^>]*>\s*([0-9,]+)\s*<\/td>/gi;
    
    const seasonSeen = new Set();
    
    let match;
    while ((match = tableRowPattern.exec(html)) !== null) {
      const season = match[1]?.trim();
      const rankText = match[2]?.trim().toLowerCase();
      const lpText = match[3]?.trim();
      
      if (season && rankText && lpText) {
        if (seasonSeen.has(season)) {
          continue;
        }
        
        const lp = parseInt(lpText.replace(/[^\d]/g, '')) || 0;
        const rankParts = rankText.split(/\s+/);
        const tier = rankParts[0]?.toUpperCase();
        const division = rankParts[1] || null;
        
        if (season.match(/^S20\d+(\s+S\d+)?/) && this.isValidTier(tier)) {
          seasonSeen.add(season);
          ranks.push({ tier, division, lp, source: `html_${season.replace(/\s+/g, '_')}` });
        }
      }
    }
    
    return ranks;
  }

  extractCurrentRank(html) {
    const ranks = [];
    
    const currentPattern = /<strong[^>]*class="[^"]*text-xl[^"]*"[^>]*>\s*([^<]+)\s*<\/strong>[\s\S]{0,100}?<span[^>]*class="[^"]*text-xs[^"]*gray-500[^"]*"[^>]*>\s*([0-9,]+)(?:<!--[^>]*-->)?\s*LP\s*<\/span>/gi;
    
    let match;
    while ((match = currentPattern.exec(html)) !== null) {
      const rankText = match[1]?.trim().toLowerCase();
      const lpText = match[2]?.trim();
      
      if (rankText && lpText) {
        const lp = parseInt(lpText.replace(/[^\d]/g, '')) || 0;
        const rankParts = rankText.split(/\s+/);
        const tier = rankParts[0]?.toUpperCase();
        const division = rankParts[1] || null;
        
        if (this.isValidTier(tier)) {
          ranks.push({ tier, division, lp, source: 'current_rank' });
          break;
        }
      }
    }
    
    return ranks;
  }

  extractTopTierRank(html) {
    const ranks = [];
    
    const topTierPattern = /<strong[^>]*class="[^"]*text-sm[^"]*"[^>]*>\s*([^<]+)\s*<\/strong>[\s\S]{0,200}?<span[^>]*class="[^"]*text-xs[^"]*gray-500[^"]*"[^>]*>\s*([0-9,]+)(?:<!--[^>]*-->)?\s*LP\s*<\/span>[\s\S]{0,400}?Top Tier/gi;
    
    let match;
    while ((match = topTierPattern.exec(html)) !== null) {
      const rankText = match[1]?.trim().toLowerCase();
      const lpText = match[2]?.trim();
      
      if (rankText && lpText) {
        const lp = parseInt(lpText.replace(/[^\d]/g, '')) || 0;
        const rankParts = rankText.split(/\s+/);
        const tier = rankParts[0]?.toUpperCase();
        const division = rankParts[1] || null;
        
        if (this.isValidTier(tier)) {
          ranks.push({ tier, division, lp, source: 'top_tier_peak' });
          break;
        }
      }
    }
    
    return ranks;
  }

  isValidTier(tier) {
    const validTiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER'];
    return validTiers.includes(tier);
  }

  removeDuplicates(ranks) {
    const seen = new Set();
    return ranks.filter(rank => {
      const key = `${rank.tier}-${rank.division}-${rank.lp}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  findHighestRank(ranks) {
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

  calculateRankValue(rank) {
    const tierValue = RANK_ORDER[rank.tier] || 0;
    const divisionValue = DIVISION_ORDER[rank.division] || 5;
    return tierValue * 10000 + divisionValue * 1000 + Math.min(rank.lp || 0, 9999);
  }
}

// Test function
async function testOpGGScraper(opggUrl) {
  const scraper = new OpGGScraper();

  try {
    const html = await scraper.scrapeAllStrategies(opggUrl);
    const allRanks = scraper.extractAllRanksFromHTML(html);
    
    if (allRanks.length === 0) {
      console.log('No ranks found');
      return null;
    }

    // Display detected ranks
    console.log('\nRanks detected:');
    allRanks.forEach((rank, index) => {
      console.log(`  ${index + 1}. ${rank.tier} ${rank.division || ''} ${rank.lp}LP`);
    });

    // Find and display peak rank
    const peakRank = scraper.findHighestRank(allRanks);
    console.log(`\nPeak rank: ${peakRank.tier} ${peakRank.division || ''} ${peakRank.lp}LP`);

    return peakRank;

  } catch (error) {
    console.error('Error:', error.message);
    return null;
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log('Usage: node test-opgg-scraper.js <opgg_url>');
    process.exit(1);
  }
  
  const opggUrl = args[0];
  
  if (!opggUrl.includes('op.gg')) {
    console.error('Invalid URL: Must be an op.gg URL');
    process.exit(1);
  }
  
  try {
    const result = await testOpGGScraper(opggUrl);
    process.exit(result ? 0 : 1);
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { OpGGScraper, testOpGGScraper };
