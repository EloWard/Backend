# Security Policy (Backend)

The EloWard backend is designed with a security-first architecture. This document summarizes what data is handled, where it lives, and the controls we apply.

## Data We Handle

- D1 Database (see `users.sql`, `subscriptions.sql`, `lol_ranks.sql`, `bot_channels.sql`, `channel_viewers_daily.sql`, `stripe_events.sql`):
  - `users`: Twitch profile data and app metrics (no OAuth tokens stored)
  - `subscriptions`: Stripe metadata and activation flags (no card data)
  - `lol_ranks`: League of Legends rank data keyed by Riot PUUID with peak rank tracking
  - `twitch_bot_users`: Bot configuration per channel (minimal PII)
  - `channel_viewers_daily`: Viewer qualification tracking (PUUID + channel + date)
  - `stripe_events`: Webhook event idempotency tracking
- R2 (via CDN worker): Rank badge images only
- KV (Bot worker): EloWardBot tokens (moderation scopes) and minimal state
- Redis (Bot worker): Instant config propagation via pub/sub (no persistent data)

## Token Handling

- Riot/Twitch tokens are exchanged server-side and not returned to clients except when strictly necessary for redirects.
- Workers never persist user Riot/Twitch access tokens in D1.
- Bot tokens (for EloWardBot) are stored in KV and refreshed server-side with automatic refresh.
- Stripe webhooks are verified using the official library and your `WEBHOOK_SECRET`.
- HMAC-secured bot communication uses shared secrets with timestamp validation (±60s window).

## Internal Authentication

- Write endpoints (e.g., rank and user upserts) require `X-Internal-Auth` with a secret (`*_WRITE_KEY` / `INTERNAL_WRITE_KEY`).
- Database workers only accept writes from authorized auth workers via service bindings + internal header.
- HMAC-signed requests for bot communication with timestamp validation (±60s window).
- Redis pub/sub messages are authenticated via API tokens.

## Network Security

- HTTPS-only endpoints (Cloudflare Workers).
- CORS is restricted to production origins where appropriate; otherwise minimal and explicit.
- No secrets or tokens are logged. Logs include IDs/status codes only.
- Redis connections use TLS with API token authentication.
- HMAC request validation prevents replay attacks with timestamp windows.

## Least Privilege

- Service bindings are scoped to only what each worker needs (e.g., RiotAuth worker can call Ranks/Users workers; Ranks has direct D1 only).
- API keys (RIOT_API_KEY, Stripe secret) are held in environment variables, not in source.
- Redis access is limited to pub/sub operations only.
- HMAC secrets are worker-specific and not shared across services.
- Viewer tracking data is minimal (PUUID + channel + date only).

## Vulnerability Reporting

If you discover a vulnerability:
- Do not open a public issue.
- Email: unleashai.inquiries@gmail.com
- Include steps to reproduce and impact assessment.
- Please allow reasonable time for a fix before public disclosure.

## Files to Audit First

- `workers/riotauth/riotauth-worker.ts` (Riot OAuth, rank fetch + store, Op.gg scraping)
- `workers/twitchauth/twitchauth-worker.ts` (Twitch OAuth, user register)
- `workers/ranks/rank-worker.js` (D1 rank storage + reads, user options)
- `workers/users/users-worker.js` (D1 user storage + metrics, viewer tracking)
- `workers/stripe/stripe-worker.js` (Stripe webhook verification)
- `workers/elowardbot/bot-worker.ts` (HMAC validation, Redis pub/sub, KV, token management)
- `workers/cdn/cdn.js` (R2 asset serving with path validation)

## Privacy

- We store minimal PII (Twitch profile info) to operate the service.
- No passwords are ever collected; OAuth flows use provider infrastructure.
- Users can disconnect Riot linkage which removes rank data by PUUID.
- Viewer tracking data is anonymized (PUUID only, no usernames).
- Peak rank seeding uses public Op.gg data only.
- Redis pub/sub messages contain no PII, only configuration updates.
