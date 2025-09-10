#!/usr/bin/env node

/**
 * Manual Peak Rank Seeding Script
 * 
 * Fetches all users from lol_ranks database, scrapes their op.gg peak ranks,
 * and updates the database with the results.
 * 
 * Setup:
 *   1. Create .env.local file with required variables (see README)
 *   2. node manual-peak-seed.js [--dry-run] [--start-from-user-id=123]
 * 
 * Features:
 * - Checkpoints for resuming on failure
 * - Staggered timing to avoid rate limits  
 * - Progress logging and error handling
 * - Dry run mode for testing
 */

const fs = require('fs');
const path = require('path');
const { OpGGScraper } = require('./test-opgg-scraper.js');

// Load environment variables from .env.local
function loadEnvLocal() {
  const envPath = path.join(__dirname, '.env.local');
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    envContent.split('\n').forEach(line => {
      line = line.trim();
      // Skip empty lines and comments
      if (!line || line.startsWith('#')) return;
      
      const equalIndex = line.indexOf('=');
      if (equalIndex > 0) {
        const key = line.substring(0, equalIndex).trim();
        const value = line.substring(equalIndex + 1).trim();
        if (key && value) {
          process.env[key] = value;
          console.log(`  ✓ ${key}=${value.substring(0, 8)}...`);
        }
      }
    });
    console.log('📁 Loaded environment variables from .env.local');
  } else {
    throw new Error('❌ .env.local file not found! Please create it with required variables.');
  }
}

// Load environment variables at startup
loadEnvLocal();

// Region mapping for op.gg URLs (matches your backend)
const REGION_MAPPING = {
  'na1': 'na', 'euw1': 'euw', 'eun1': 'eune', 'kr': 'kr', 'br1': 'br',
  'jp1': 'jp', 'la1': 'lan', 'la2': 'las', 'oc1': 'oce', 'tr1': 'tr',
  'ru': 'ru', 'me1': 'me', 'sg2': 'sea', 'tw2': 'tw', 'vn2': 'vn'
};

// Configuration
const CONFIG = {
  CHECKPOINT_FILE: 'peak-seed-progress.json',
  LOG_FILE: 'peak-seed-log.txt',
  MIN_DELAY_MS: 2000,    // Minimum 2 second between requests
  MAX_DELAY_MS: 5000,    // Maximum 5 seconds between requests  
  BATCH_SIZE: 1,         // Save checkpoint after every user (safer)
  MAX_RETRIES: 3         // Retries per user on failure
};

class PeakSeedManager {
  constructor() {
    this.scraper = new OpGGScraper();
    this.db = null;
    this.progress = this.loadProgress();
    this.stats = {
      processed: 0,
      successful: 0,
      failed: 0,
      startTime: new Date()
    };
  }

  // Cloudflare D1 connection
  async connectDatabase() {
    this.accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
    this.databaseId = process.env.CLOUDFLARE_DATABASE_ID;
    this.apiToken = process.env.CLOUDFLARE_API_TOKEN;

    if (!this.accountId || !this.databaseId || !this.apiToken) {
      throw new Error('Required environment variables: CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_DATABASE_ID, CLOUDFLARE_API_TOKEN');
    }

    // Test connection with a simple query
    try {
      const testResult = await this.executeD1Query('SELECT 1 as test');
      console.log('🔌 Cloudflare D1 connection established');
      this.log('Cloudflare D1 connection established');
    } catch (error) {
      throw new Error(`D1 connection failed: ${error.message}`);
    }
  }

  async getAllUsers() {
    const query = 'SELECT riot_puuid, riot_id, region, twitch_username FROM lol_ranks ORDER BY riot_puuid';
    const result = await this.executeD1Query(query);
    
    console.log(`📊 Found ${result.length} users in database`);
    return result;
  }

  async updateUserPeakRank(puuid, peakRank) {
    // Format data first for consistency in both dry run and live mode
    const formattedTier = peakRank.tier.toUpperCase(); // Ensure uppercase (BRONZE, MASTER, etc.)
    let formattedDivision = null;
    let formattedLP = parseInt(peakRank.lp) || 0; // Ensure integer

    // Handle division formatting to match database format
    if (['MASTER', 'GRANDMASTER', 'CHALLENGER'].includes(formattedTier)) {
      // Master+ always gets division = "I" (string, not null)
      formattedDivision = 'I';
    } else if (formattedTier === 'UNRANKED') {
      // Unranked gets actual NULL (not string) division and 0 LP
      formattedDivision = null; // JavaScript null, not "null" string
      formattedLP = 0;
    } else {
      // Convert numeric divisions to Roman numerals if needed
      if (peakRank.division && peakRank.division.trim() !== '') {
        const divisionMap = { '1': 'I', '2': 'II', '3': 'III', '4': 'IV' };
        formattedDivision = divisionMap[peakRank.division] || peakRank.division.toUpperCase();
      } else {
        // Explicitly set to JavaScript null (not undefined or empty string)
        formattedDivision = null;
      }
    }

    if (this.isDryRun) {
      console.log(`🧪 DRY RUN: Would update ${puuid} with peak: ${formattedTier} ${formattedDivision || 'NULL'} ${formattedLP}LP`);
      return;
    }

    const query = `
      UPDATE lol_ranks 
      SET peak_rank_tier = ?, peak_rank_division = ?, peak_lp = ?
      WHERE riot_puuid = ?
    `;
    
    // Ensure we're sending the correct data types
    const params = [
      formattedTier,      // string
      formattedDivision,  // string or null (JavaScript null, not "null")
      formattedLP,        // integer
      puuid               // string
    ];
    
    // Debug: Log actual data types being sent
    console.log(`📝 Sending to DB: tier="${formattedTier}" division=${formattedDivision === null ? 'NULL' : `"${formattedDivision}"`} lp=${formattedLP}`);
    
    await this.executeD1Query(query, params);
    
    console.log(`✅ Updated ${puuid} peak rank: ${formattedTier} ${formattedDivision || 'NULL'} ${formattedLP}LP`);
  }

  constructOpGGUrl(riotId, region) {
    const opggRegion = REGION_MAPPING[region?.toLowerCase()];
    if (!opggRegion) {
      throw new Error(`Unknown region: ${region}`);
    }

    const riotIdParts = riotId.split('#');
    const encodedName = encodeURIComponent(riotIdParts[0] || '');
    const tagLine = riotIdParts[1] || region.toUpperCase();
    
    return `https://op.gg/lol/summoners/${opggRegion}/${encodedName}-${tagLine}?queue_type=SOLORANKED`;
  }

  async scrapeUserPeakRank(user) {
    const { riot_puuid, riot_id, region, twitch_username } = user;
    
    try {
      const opggUrl = this.constructOpGGUrl(riot_id, region);
      console.log(`🔍 Scraping: ${twitch_username} (${riot_id}) -> ${opggUrl}`);
      
      const html = await this.scraper.scrapeUrl(opggUrl);
      const allRanks = this.scraper.extractAllRanksFromHTML(html);
      
      if (allRanks.length === 0) {
        console.log(`⚠️  No ranks found for ${twitch_username}`);
        return null;
      }

      const peakRank = this.scraper.findHighestRank(allRanks);
      console.log(`📈 Found peak: ${twitch_username} -> ${peakRank.tier} ${peakRank.division} ${peakRank.lp}LP`);
      
      return peakRank;
    } catch (error) {
      // Handle 404 errors specifically - profile doesn't exist on op.gg
      if (error.message.includes('404') || error.message.includes('Not Found')) {
        console.log(`⏭️  Profile not found on op.gg: ${twitch_username} - skipping`);
        return null; // Return null to skip this user (no retry needed)
      }
      
      console.error(`❌ Failed to scrape ${twitch_username}: ${error.message}`);
      throw error; // Re-throw for other errors that should be retried
    }
  }

  async processUser(user, retries = 0) {
    const { riot_puuid, twitch_username } = user;
    
    try {
      const peakRank = await this.scrapeUserPeakRank(user);
      
      if (peakRank) {
        await this.updateUserPeakRank(riot_puuid, peakRank);
        this.progress.completed.push(riot_puuid);
        this.stats.successful++;
      } else {
        this.progress.failed.push({ puuid: riot_puuid, reason: 'No ranks found' });
        this.stats.failed++;
      }
      
      this.stats.processed++;
      return { processed: true, needsDelay: true }; // Delay needed after actual processing
      
    } catch (error) {
      if (retries < CONFIG.MAX_RETRIES) {
        console.log(`🔄 Retrying ${twitch_username} (${retries + 1}/${CONFIG.MAX_RETRIES})...`);
        await this.sleep(2000); // Extra delay before retry
        return await this.processUser(user, retries + 1);
      }
      
      this.progress.failed.push({ puuid: riot_puuid, reason: error.message });
      this.stats.failed++;
      this.stats.processed++;
      this.log(`FAILED: ${twitch_username} - ${error.message}`);
      return { processed: false, needsDelay: true }; // Delay after failed attempts too
    }
  }

  async run(options = {}) {
    this.isDryRun = options.dryRun || false;
    this.startFromUser = options.startFromUser || null;
    this.retryFailed = options.retryFailed || false;
    
    console.log('🚀 Starting manual peak rank seeding...');
    console.log(`📊 Mode: ${this.isDryRun ? 'DRY RUN' : 'LIVE UPDATE'}`);
    
    await this.connectDatabase();
    const allUsers = await this.getAllUsers();
    console.log(`📊 Found ${allUsers.length} total users in database`);
    
    // Filter out already-processed users AND optionally previously failed users
    const failedPuuids = this.progress.failed.map(f => f.puuid);
    const unprocessedUsers = allUsers.filter(user => {
      // Always skip completed users
      if (this.progress.completed.includes(user.riot_puuid)) {
        return false;
      }
      // Skip previously failed users unless --retry-failed is used
      if (!this.retryFailed && failedPuuids.includes(user.riot_puuid)) {
        return false;
      }
      return true;
    });
    
    const alreadyCompleted = this.progress.completed.length;
    const previouslyFailed = failedPuuids.length;
    const willRetryFailed = this.retryFailed ? previouslyFailed : 0;
    const skippedFailed = this.retryFailed ? 0 : previouslyFailed;
    
    console.log(`✅ Already completed: ${alreadyCompleted} users`);
    if (this.retryFailed && previouslyFailed > 0) {
      console.log(`🔄 Will retry previously failed: ${willRetryFailed} users`);
    } else if (previouslyFailed > 0) {
      console.log(`❌ Skipping previously failed: ${skippedFailed} users`);
    }
    console.log(`⏳ Remaining to process: ${unprocessedUsers.length} users`);
    
    if (unprocessedUsers.length === 0) {
      console.log('🎉 All users already processed! Nothing to do.');
      return;
    }
    
    // If retrying failed users, clear their failed status so they can be marked as successful if they work this time
    if (this.retryFailed && willRetryFailed > 0) {
      console.log(`🧹 Clearing failed status for ${willRetryFailed} users to allow fresh attempts`);
      this.progress.failed = [];
    }
    
    // Handle starting from specific user (now works on filtered list)
    let startIndex = 0;
    if (this.startFromUser) {
      startIndex = unprocessedUsers.findIndex(u => u.riot_puuid === this.startFromUser);
      if (startIndex === -1) {
        console.error(`❌ Start user ${this.startFromUser} not found in remaining users`);
        return;
      }
      console.log(`⏯️  Starting from user ${startIndex + 1}/${unprocessedUsers.length} in remaining list`);
    }

    for (let i = startIndex; i < unprocessedUsers.length; i++) {
      const user = unprocessedUsers[i];
      const overallProgress = alreadyCompleted + skippedFailed + i + 1;
      
      console.log(`\n[${overallProgress}/${allUsers.length}] Processing: ${user.twitch_username}`);
      
      const result = await this.processUser(user);
      
      // Save checkpoint after every user (CONFIG.BATCH_SIZE = 1)
      if (this.stats.processed % CONFIG.BATCH_SIZE === 0) {
        this.saveProgress();
        // Log stats every 10 users to avoid spam
        if (this.stats.processed % 10 === 0) {
          this.logStats();
        }
      }
      
      // Only delay after actual scraping attempts (not skipped users)
      if (result.needsDelay && i < unprocessedUsers.length - 1) {
        const delay = CONFIG.MIN_DELAY_MS + Math.random() * (CONFIG.MAX_DELAY_MS - CONFIG.MIN_DELAY_MS);
        console.log(`⏳ Waiting ${Math.round(delay)}ms...`);
        await this.sleep(delay);
      }
    }
    
    // Final save and summary
    this.saveProgress();
    this.printFinalSummary();
  }

  // Utility methods
  loadProgress() {
    try {
      if (fs.existsSync(CONFIG.CHECKPOINT_FILE)) {
        const data = JSON.parse(fs.readFileSync(CONFIG.CHECKPOINT_FILE, 'utf8'));
        console.log(`📋 Loaded checkpoint: ${data.completed.length} completed, ${data.failed.length} failed`);
        return data;
      }
    } catch (error) {
      console.warn('⚠️  Failed to load checkpoint, starting fresh');
    }
    
    return { completed: [], failed: [] };
  }

  // Cloudflare D1 REST API execution
  async executeD1Query(sql, params = []) {
    const endpoint = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/d1/database/${this.databaseId}/query`;
    
    const payload = {
      sql: sql,
      params: params
    };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.apiToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`D1 API error ${response.status}: ${errorText}`);
    }

    const result = await response.json();
    
    if (!result.success) {
      throw new Error(`D1 query failed: ${result.errors?.[0]?.message || 'Unknown error'}`);
    }

    // Return the rows from the first result (D1 API returns array of result objects)
    return result.result?.[0]?.results || [];
  }

  saveProgress() {
    try {
      const data = {
        ...this.progress,
        lastSaved: new Date().toISOString(),
        stats: this.stats
      };
      fs.writeFileSync(CONFIG.CHECKPOINT_FILE, JSON.stringify(data, null, 2));
      // Only log saves every 10 users to reduce console spam
      if (this.stats.processed % 10 === 0) {
        console.log(`💾 Progress saved: ${this.stats.successful} successful, ${this.stats.failed} failed`);
      }
    } catch (error) {
      console.error('❌ Failed to save progress:', error.message);
    }
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] ${message}\n`;
    fs.appendFileSync(CONFIG.LOG_FILE, logEntry);
  }

  logStats() {
    const elapsed = ((Date.now() - this.stats.startTime.getTime()) / 1000 / 60).toFixed(1);
    console.log(`\n📊 Progress: ${this.stats.processed} processed | ✅ ${this.stats.successful} success | ❌ ${this.stats.failed} failed | ⏱️ ${elapsed}min`);
  }

  printFinalSummary() {
    const elapsed = ((Date.now() - this.stats.startTime.getTime()) / 1000 / 60).toFixed(1);
    console.log('\n🎉 Manual peak rank seeding completed!');
    console.log(`📊 Final Stats:`);
    console.log(`   Total processed: ${this.stats.processed}`);
    console.log(`   ✅ Successful: ${this.stats.successful}`);
    console.log(`   ❌ Failed: ${this.stats.failed}`);
    console.log(`   ⏱️ Total time: ${elapsed} minutes`);
    console.log(`   📄 Progress saved to: ${CONFIG.CHECKPOINT_FILE}`);
    console.log(`   📝 Logs saved to: ${CONFIG.LOG_FILE}`);
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// CLI argument parsing
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};
  
  args.forEach(arg => {
    if (arg === '--dry-run') {
      options.dryRun = true;
    } else if (arg === '--retry-failed') {
      options.retryFailed = true;
    } else if (arg.startsWith('--start-from-user-id=')) {
      options.startFromUser = arg.split('=')[1];
    } else if (arg.startsWith('--batch-size=')) {
      CONFIG.BATCH_SIZE = parseInt(arg.split('=')[1]);
    }
  });
  
  return options;
}

// Main execution
async function main() {
  const options = parseArgs();
  const manager = new PeakSeedManager();
  
  try {
    await manager.run(options);
    process.exit(0);
  } catch (error) {
    console.error('💥 Fatal error:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { PeakSeedManager };
