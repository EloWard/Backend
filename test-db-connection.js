#!/usr/bin/env node

/**
 * Test Database Connection
 * 
 * Quick test to verify your database connection works before running the full script.
 * 
 * Usage:
 *   1. Create .env.local file with required variables
 *   2. node test-db-connection.js
 */

const fs = require('fs');
const path = require('path');

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

async function testConnection() {
  try {
    loadEnvLocal();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }

  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const databaseId = process.env.CLOUDFLARE_DATABASE_ID;
  const apiToken = process.env.CLOUDFLARE_API_TOKEN;
  
  if (!accountId || !databaseId || !apiToken) {
    console.error('❌ Required environment variables missing in .env.local:');
    console.log('💡 Create .env.local file with:');
    console.log('   CLOUDFLARE_ACCOUNT_ID=your_account_id');
    console.log('   CLOUDFLARE_DATABASE_ID=your_database_id');
    console.log('   CLOUDFLARE_API_TOKEN=your_api_token');
    process.exit(1);
  }

  console.log('🔌 Testing Cloudflare D1 connection...');
  console.log(`📍 Account: ${accountId.substring(0, 8)}...`);
  console.log(`📍 Database: ${databaseId.substring(0, 8)}...`);

  try {
    const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;
    
    // Test with simple query
    const testPayload = {
      sql: 'SELECT COUNT(*) as count FROM lol_ranks',
      params: []
    };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(testPayload)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`D1 API error ${response.status}: ${errorText}`);
    }

    const result = await response.json();
    
    if (!result.success) {
      throw new Error(`D1 query failed: ${result.errors?.[0]?.message || 'Unknown error'}`);
    }

    const userCount = result.result?.[0]?.results?.[0]?.count || 0;
    console.log(`✅ Connected to Cloudflare D1!`);
    console.log(`📊 Found ${userCount} users in lol_ranks table`);
    
    if (userCount === 0) {
      console.log('⚠️  Database appears empty - make sure you have user data before running peak seeding');
    }
    
  } catch (error) {
    console.error('❌ Cloudflare D1 connection failed:', error.message);
    console.log('\n💡 Common issues:');
    console.log('   - Wrong Account ID (check right sidebar in Cloudflare dashboard)');
    console.log('   - Wrong Database ID (check D1 dashboard → your database → Settings)');
    console.log('   - Invalid API Token (needs D1:Edit permissions)');
    console.log('   - API Token not created or expired');
    console.log('   - Network/firewall blocking api.cloudflare.com');
    process.exit(1);
  }
}

testConnection();
