# EloWard Backend (Cloudflare Workers)

This folder contains all server-side components that power the EloWard ecosystem. It is a multi-worker architecture running on Cloudflare Workers with D1, KV, R2, and Durable Objects.

See `architecture.txt` for a deeper overview of routes, flows, and database schema.

## Workers Overview

- Riot Auth Worker (`workers/riotauth/riotauth-worker.ts`)
  - Purpose: Completes Riot OAuth (single call), fetches rank via Riot API, and stores results.
  - Key endpoints:
    - `POST /auth/complete` – Exchange code → PUUID → rank → store (via Ranks worker)
    - `GET /auth/redirect` – OAuth callback bridge (redirects to website)
    - `POST /riot/refreshrank` – Refresh rank by PUUID (no client tokens)
    - `DELETE /disconnect` – Remove rank data by PUUID

- Twitch Auth Worker (`workers/twitchauth/twitchauth-worker.ts`)
  - Purpose: Consolidated Twitch OAuth (code → user), registers/updates user profile.
  - Key endpoints:
    - `POST /twitch/auth` – Exchange code → user → register (via Users worker)
    - `GET /auth/twitch/redirect` – OAuth callback bridge

- Ranks Worker (`workers/ranks/rank-worker.js`)
  - Purpose: D1-backed CRUD for League of Legends ranks.
  - Key endpoints:
    - `GET /api/ranks/lol/:username` – Fetch rank by Twitch username
    - `POST /api/ranks/lol` – Upsert rank (internal auth)
    - `POST /api/ranks/lol/by-puuid` – Lookup by PUUID
    - `DELETE /api/ranks/lol` – Delete by PUUID (internal auth)

- Users Worker (`workers/users/users-worker.js`)
  - Purpose: D1-backed user profile data and metrics.
  - Key endpoints:
    - `POST /user/register` – Upsert Twitch profile (internal auth)
    - `POST /user/lookup` – Lookup `channel_name` by `twitch_id`
    - `POST /dashboard/data_id` – Dashboard summary by Twitch ID (preferred)
    - `POST /dashboard/data` – Dashboard summary by login (legacy)
    - `POST /metrics/db_read` – Increment read counter
    - `POST /metrics/successful_lookup` – Increment display counter
    - `POST /channelstatus/verify` – Check if a channel is active
    - `POST /channel/active/update_id` – Update `channel_active` by Twitch ID (preferred; internal auth)
    - `POST /channel/active/update` – Update `channel_active` by login (legacy; internal auth)

- CDN Worker (`workers/cdn/cdn.js`)
  - Purpose: Serves badge assets from R2 with caching, strict path validation, and CORS.
  - Key endpoints:
    - `GET /{game}/{filename}` – Returns image from R2 with long-lived cache headers

- Stripe Worker (`workers/stripe/stripe-worker.js`)
  - Purpose: Stripe Checkout/Portal + webhooks to activate/deactivate subscriptions.
  - Key endpoints:
    - `POST /api/create-portal-session`
    - `POST /api/webhook` – Raw body verified via Stripe library

- EloWard Bot Worker (`workers/elowardbot/bot-worker.ts`)
  - Purpose: Moderation bot that interacts with chat based on user ranks via IRC-only ingestion.
  - Key endpoints:
    - `GET /health`
    - `POST /bot/config_id` – Fetch config by Twitch ID
    - `POST /bot/enable_internal` / `POST /bot/disable_internal` / `POST /bot/config_internal`
    - `POST /irc/start` / `POST /irc/reload` – Bootstrap and reload IRC shards
  - Durable Objects: `BotManager` (orchestration), `IrcShard` (cooldowns), `IrcClientShard` (IRC WebSocket client)

## Local Development

Each worker can be run with Wrangler in isolation. Bindings/secrets must be set locally.

Example:

```bash
# Run the ranks worker locally
wrangler dev --local --experimental-json-config
```

D1 schema files: `users.sql`, `subscriptions.sql`, `lol_ranks.sql`, `bot_channels.sql`.
Apply them to your local D1 database using Wrangler.

## Environment Variables (by worker)

- Riot Auth: `RIOT_CLIENT_ID`, `RIOT_CLIENT_SECRET`, `RIOT_API_KEY`, `RANK_WRITE_KEY`, `USERS_WRITE_KEY` + service bindings `RANK_WORKER`, `USERS_WORKER`, `TWITCH_AUTH_WORKER`
- Twitch Auth: `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, `USERS_WRITE_KEY` + service binding `USERS_WORKER`
- Ranks: `DB` (D1), `RANK_WRITE_KEY`
- Users: `DB` (D1), `USERS_WRITE_KEY`
- CDN: `ELOWARD_BADGES` (R2)
- Stripe: `secret_key`, `WEBHOOK_SECRET`, `DB` (D1)
- Bot: `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, `WORKER_PUBLIC_URL`, `EVENTSUB_SECRET`, `BOT_KV`, `RANK_WORKER`, `BOT_WRITE_KEY`, `DB` (D1) + Durable Objects `BOT_MANAGER`, `IRC_SHARD`

See each worker file header for binding specifics.

## Security

- Write operations require service-specific auth headers (e.g., `USERS_WRITE_KEY`, `RANK_WRITE_KEY`, `BOT_WRITE_KEY`).
- Tokens are not returned to clients from auth workers.
- Database workers never accept direct public writes.

## License

Apache 2.0 + Commons Clause. See `LICENSE`.
