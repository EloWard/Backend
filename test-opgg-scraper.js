#!/usr/bin/env node

/**
 * Op.gg Rank Scraper - Optimized Production Version
 * 
 * Extracts current, peak, and historical ranks from op.gg ranked solo/duo pages
 * 
 * Usage: node test-opgg-scraper.js <opgg_url>
 * Example: node test-opgg-scraper.js "https://op.gg/lol/summoners/na/Spankers-CN1?queue_type=SOLORANKED"
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

  async scrapeUrl(url) {
    // Fetch only the single provided URL
    return await this.fetchWithRetry(url);
  }


  extractAllRanksFromHTML(html) {
    const allRanks = [];
    
    // Extract current rank (diamond 2, 75 LP with wins/losses)
    const currentRank = this.extractCurrentRank(html);
    if (currentRank) {
      allRanks.push(currentRank);
    }

    // Extract peak rank (challenger, 1,008 LP with "Top Tier" badge)
    const peakRank = this.extractPeakRank(html);
    if (peakRank) {
      allRanks.push(peakRank);
    }

    // Extract all historical season ranks from table
    const historicalRanks = this.extractHistoricalRanks(html);
    allRanks.push(...historicalRanks);

    return allRanks;
  }


  extractCurrentRank(html) {
    // Extract current rank: diamond 2, 75 LP, 156W 122L, Win rate 56%
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

  extractPeakRank(html) {
    // Extract peak rank: challenger, 1,008 LP with "Top Tier" badge
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

  extractHistoricalRanks(html) {
    const ranks = [];
    
    // Extract historical ranks from table: S2024 S3 grandmaster 421, etc.
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

  isValidTier(tier) {
    const validTiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER'];
    return validTiers.includes(tier);
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
    const html = await scraper.scrapeUrl(opggUrl);
    const allRanks = scraper.extractAllRanksFromHTML(html);
    
    if (allRanks.length === 0) {
      console.log('No ranks found');
      return null;
    }

    // Display detected ranks
    console.log('\nRanks detected:');
    allRanks.forEach((rank, index) => {
      const division = (rank.tier === 'MASTER' || rank.tier === 'GRANDMASTER' || rank.tier === 'CHALLENGER') ? '' : ` ${rank.division || ''}`;
      console.log(`  ${index + 1}. ${rank.tier}${division} ${rank.lp}LP`);
    });

    // Find and display peak rank
    const peakRank = scraper.findHighestRank(allRanks);
    const peakDivision = (peakRank.tier === 'MASTER' || peakRank.tier === 'GRANDMASTER' || peakRank.tier === 'CHALLENGER') ? '' : ` ${peakRank.division || ''}`;
    console.log(`\nPeak rank: ${peakRank.tier}${peakDivision} ${peakRank.lp}LP`);

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
