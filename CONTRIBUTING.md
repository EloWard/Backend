# Contributing to EloWard Backend

We welcome contributions! This backend powers EloWard's Cloudflare Workers. Please follow these guidelines.

## License

Contributions are licensed under Apache 2.0 + Commons Clause, the same as the project. By submitting a PR, you certify that you have the right to license your contribution under these terms.

## Development Setup

- Cloudflare Workers, D1, R2, KV, and Durable Objects are used.
- Install tooling:
  - Node.js LTS
  - Wrangler: `npm i -g wrangler`
- Each worker has its own entry file under `workers/`.
- Secrets and bindings are configured via Wrangler/Terraform and are NOT stored in this repo.

## Running Locally

- Use `wrangler dev` within each worker directory or a unified monorepo config.
- Mock secrets via `--var`/`--local` only. Do not commit secrets.
- D1 schema files are in the repo root (e.g., `users.sql`, `subscriptions.sql`, `lol_ranks.sql`, `bot_channels.sql`).

## Coding Standards

- Prefer TypeScript for new workers; keep JS workers consistent and small.
- Small, composable functions; clear names; early returns; meaningful error messages.
- Avoid logging secrets or tokens. Log IDs and statuses only.
- Keep CORS consistent and minimal.

## Testing

- Add unit-level tests where practical; otherwise, thorough manual verification with `wrangler dev`.
- Include curl examples in PR descriptions for new endpoints.

## Pull Requests

- One logical change per PR.
- Include: purpose, high-level design, security considerations, and testing steps.
- Update documentation (`README.md` or `architecture.txt`) when APIs change.

## Security

- Never log access/refresh tokens, API keys, or secrets.
- Validate inputs and enforce internal auth for write operations.
- Follow the least privilege principle for service bindings and DB access.

Thanks for helping improve EloWard! 🎮
