-- Create table for EloWardBot per-channel configuration
CREATE TABLE IF NOT EXISTS twitch_bot_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_login TEXT UNIQUE NOT NULL,          -- lowercase Twitch login
  twitch_id TEXT,
  bot_enabled BOOLEAN DEFAULT 0,
  timeout_seconds INTEGER DEFAULT 30,
  reason_template TEXT DEFAULT "⏱️ {seconds}s timeout: link your EloWard rank at {site}",
  ignore_roles TEXT DEFAULT "broadcaster,moderator,vip",
  cooldown_seconds INTEGER DEFAULT 60,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Helpful index for lookups by login
CREATE INDEX IF NOT EXISTS idx_bot_channels_login ON twitch_bot_channels(channel_login);

