-- Create table for EloWardBot per-channel configuration (minimal PII)
DROP TABLE IF EXISTS twitch_bot_users;
CREATE TABLE IF NOT EXISTS twitch_bot_users (
  twitch_id TEXT PRIMARY KEY,
  channel_name TEXT,                           -- optional, display only
  bot_enabled INTEGER DEFAULT 0,
  timeout_seconds INTEGER DEFAULT 30,
  reason_template TEXT DEFAULT "not enough elo to speak. type !eloward",
  ignore_roles TEXT DEFAULT "broadcaster,moderator,vip",
  enforcement_mode TEXT DEFAULT 'has_rank',    -- 'has_rank' | 'min_rank'
  min_rank_tier TEXT,                          -- e.g., 'GOLD', 'DIAMOND', 'MASTER'
  min_rank_division INTEGER,                   -- 1..4 (I..IV), null for Master+
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_users_channel_name ON twitch_bot_users(channel_name);

