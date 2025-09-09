# Manual Peak Rank Seeding Setup Guide

## Quick Setup

### 1. Create `.env.local` file

Create a file called `.env.local` in the Backend directory with these variables:

```bash
CLOUDFLARE_ACCOUNT_ID=your_account_id_here
CLOUDFLARE_DATABASE_ID=your_database_id_here
CLOUDFLARE_API_TOKEN=your_api_token_here
```

### 2. Test connection

```bash
node test-db-connection.js
```

### 3. Run the script

```bash
# Test run first
node manual-peak-seed.js --dry-run

# Live run  
node manual-peak-seed.js
```

## Environment Variables Required

You need these 3 variables in your `.env.local` file:

| Variable | Description | Example |
|----------|-------------|---------|
| `CLOUDFLARE_ACCOUNT_ID` | Your Cloudflare account ID | `1234567890abcdef1234567890abcdef` |
| `CLOUDFLARE_DATABASE_ID` | Your D1 database ID | `12345678-1234-1234-1234-123456789abc` |
| `CLOUDFLARE_API_TOKEN` | API token with D1:Edit permissions | `your-long-api-token-here` |

## Where to Find These Values

- **Account ID**: Right sidebar in any Cloudflare dashboard page
- **Database ID**: D1 dashboard → your database → Settings tab  
- **API Token**: My Profile → API Tokens → Create Token (D1:Edit permissions)

## Script Options

```bash
node manual-peak-seed.js [options]

Options:
  --dry-run                    Test run without database changes
  --start-from-user-id=ID      Resume from specific user
  --batch-size=50              Number of users per checkpoint
```

## Files Created During Run

- `peak-seed-progress.json` - Checkpoint data for resuming
- `peak-seed-log.txt` - Detailed execution logs


**Important**: Never commit `.env.local` to git! Add it to your `.gitignore` file.