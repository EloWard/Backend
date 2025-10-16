# EloWard Backend (Cloudflare Workers)

This folder contains all server-side components that power the EloWard ecosystem. It is a multi-worker architecture running on Cloudflare Workers with D1, KV, R2, Redis, and hybrid IRC bot integration.

See `architecture.txt` for a deeper overview of routes, flows, and database schema.

## Workers Overview

- **Riot Auth Worker** (`workers/riotauth/riotauth-worker.ts`)
  - Purpose: Completes Riot OAuth, fetches rank via Riot API, stores results, and seeds peak ranks via Op.gg scraping.
  - Key endpoints:
    - `POST /auth/complete` – Exchange code → PUUID → rank → store (via Ranks worker)
    - `GET /auth/redirect` – OAuth callback bridge (redirects to website)
    - `POST /riot/refreshrank` – Refresh rank by PUUID (no client tokens)
    - `DELETE /disconnect` – Remove rank data by PUUID
  - Features: Automatic peak rank seeding via Op.gg scraping, rank comparison logic

- **Twitch Auth Worker** (`workers/twitchauth/twitchauth-worker.ts`)
  - Purpose: Consolidated Twitch OAuth (code → user), registers/updates user profile.
  - Key endpoints:
    - `POST /twitch/auth` – Exchange code → user → register (via Users worker)
    - `GET /auth/twitch/redirect` – OAuth callback bridge
    - `GET /health` – Health check

- **Ranks Worker** (`workers/ranks/rank-worker.js`)
  - Purpose: D1-backed CRUD for League of Legends ranks with peak rank tracking and user options.
  - Key endpoints:
    - `GET /api/ranks/lol/{username}` – Fetch rank by Twitch username
    - `POST /api/ranks/lol` – Upsert rank (internal auth)
    - `POST /api/ranks/lol/by-puuid` – Lookup by PUUID
    - `DELETE /api/ranks/lol` – Delete by PUUID (internal auth)
    - `GET /api/options/{puuid}` – Get user display options (show_peak, animate_badge)
    - `PUT /api/options` – Update display options (Plus required)
  - Features: Scheduled rank refreshes, peak rank tracking, user customization options

- **Users Worker** (`workers/users/users-worker.js`)
  - Purpose: D1-backed user profile data, metrics, and viewer tracking.
  - Key endpoints:
    - `POST /user/register` – Upsert Twitch profile (internal auth)
    - `POST /user/lookup` – Lookup `channel_name` by `twitch_id`
    - `POST /user/riot-fallback` – Get consolidated user + rank data
    - `POST /dashboard/data_id` – Dashboard summary by Twitch ID (includes user email for Stripe)
    - `POST /metrics/db_read` – Increment read counter
    - `POST /metrics/successful_lookup` – Increment display counter
    - `POST /channelstatus/verify` – Check if a channel is active
    - `POST /channel/active/update_id` – Update `channel_active` by Twitch ID (internal auth)
    - `POST /view/qualify` – Track viewer qualification (5+ min watch time)
    - `GET /view/health` – Viewer tracking health stats
    - `GET /health` – Health check

- **CDN Worker** (`workers/cdn/cdn.js`)
  - Purpose: Serves badge assets from R2 with caching, strict path validation, and CORS.
  - Key endpoints:
    - `GET /{game}/{filename}` – Returns image from R2 with long-lived cache headers
  - Supported games: lol, chess, valorant, dota2, csgo, rocket, apex

- **Stripe Worker** (`workers/stripe/stripe-worker.js`)
  - Purpose: Stripe Checkout/Portal + webhooks to activate/deactivate subscriptions.
  - Key endpoints:
    - `POST /api/create-checkout-session` – Create Stripe checkout
    - `POST /api/create-portal-session` – Create customer portal
    - `POST /api/webhook` – Process Stripe webhooks (verified via Stripe library)
    - `POST /subscription/status` – Get subscription status
    - `POST /subscription/upsert` – Update subscription (internal auth)
    - `GET /health` – Health check with optional cleanup

- **EloWard Bot Worker** (`workers/elowardbot/bot-worker.ts`)
  - Purpose: Hybrid architecture - Cloudflare Workers (control plane) + AWS Lightsail IRC bot.
  - Key endpoints:
    - `GET /health` – Health check
    - `POST /bot/config_id` – Fetch config by Twitch ID
    - `POST /bot/config-update` – Update bot config (HMAC signed)
    - `POST /bot/config-get` – Get bot config (HMAC signed, used by IRC bot)
    - `POST /rank:get` – Get rank data for user (HMAC signed, used by IRC bot)
    - `GET /token` – Get bot token for IRC client
    - `GET /channels` – List ALL channels for IRC bot (always-on presence)
    - `POST /irc/channel/add` – Enable channel
    - `POST /irc/channel/remove` – Disable channel
    - `GET /oauth/start` – Start OAuth flow
    - `GET /oauth/callback` – Complete OAuth
    - `GET /oauth/done` – OAuth completion landing page
  - Features: HMAC-secured bot communication, Redis pub/sub for instant config updates, automatic token refresh

## Local Development

Each worker can be run with Wrangler in isolation. Bindings/secrets must be set locally.

Example:

```bash
# Run the ranks worker locally
wrangler dev --local --experimental-json-config
```

D1 schema files: `users.sql`, `subscriptions.sql`, `lol_ranks.sql`, `bot_channels.sql`, `channel_viewers_daily.sql`, `stripe_events.sql`.
Apply them to your local D1 database using Wrangler.

## Environment Variables (by worker)

- **Riot Auth**: `RIOT_CLIENT_ID`, `RIOT_CLIENT_SECRET`, `RIOT_API_KEY`, `RANK_WRITE_KEY`, `USERS_WRITE_KEY` + service bindings `RANK_WORKER`, `USERS_WORKER`, `TWITCH_AUTH_WORKER`
- **Twitch Auth**: `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, `USERS_WRITE_KEY` + service binding `USERS_WORKER`
- **Ranks**: `DB` (D1), `RANK_WRITE_KEY`
- **Users**: `DB` (D1), `USERS_WRITE_KEY`
- **CDN**: `ELOWARD_BADGES` (R2)
- **Stripe**: `secret_key`, `WEBHOOK_SECRET`, `MONTHLY_PRICE_ID`, `YEARLY_PRICE_ID`, `DB` (D1)
- **Bot**: `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, `WORKER_PUBLIC_URL`, `BOT_KV`, `RANK_WORKER`, `BOT_WRITE_KEY`, `DB` (D1), `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`, `HMAC_SECRET`

See each worker file header for binding specifics.

## Peak Rank Seeding

The system includes automatic peak rank seeding via Op.gg scraping:

- **Manual Script**: `peak-rank/manual-peak-seed.js` - Batch process all users
- **Op.gg Scraper**: `peak-rank/test-opgg-scraper.js` - Extract ranks from op.gg pages
- **Migration**: `peak-rank/fix_peak_ranks_migration.sql` - Database migration for peak ranks

## Security

- Write operations require service-specific auth headers (e.g., `USERS_WRITE_KEY`, `RANK_WRITE_KEY`, `BOT_WRITE_KEY`).
- HMAC-signed requests for bot communication with timestamp validation
- Tokens are not returned to clients from auth workers.
- Database workers never accept direct public writes.
- Redis pub/sub for secure instant config propagation

## License

Apache 2.0 + Commons Clause. See `LICENSE`.