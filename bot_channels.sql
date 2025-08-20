-- Create table for EloWardBot per-channel configuration (minimal PII)
CREATE TABLE IF NOT EXISTS twitch_bot_users (
  twitch_id TEXT PRIMARY KEY,
  channel_name TEXT,                           -- optional, display only
  bot_enabled INTEGER DEFAULT 0,
  timeout_seconds INTEGER DEFAULT 30,
  reason_template TEXT DEFAULT "⏱️ {seconds}s timeout: link your EloWard rank at {site}",
  ignore_roles TEXT DEFAULT "broadcaster,moderator,vip",
  cooldown_seconds INTEGER DEFAULT 60,
  enforcement_mode TEXT DEFAULT 'has_rank',    -- 'has_rank' | 'min_rank'
  min_rank_tier TEXT,                          -- e.g., 'GOLD', 'DIAMOND', 'MASTER'
  min_rank_division INTEGER,                   -- 1..4 (I..IV), null for Master+
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraint on channel_name to avoid duplicates per username
-- If a prior unique index name exists, drop it for clarity
DROP INDEX IF EXISTS uq_bot_users_channel_name;
CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_users_channel_name ON twitch_bot_users(channel_name);

