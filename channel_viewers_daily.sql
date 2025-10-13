-- Channel Viewers Daily Tracking Table
-- Tracks unique viewers per channel per day with 07:00 UTC window reset
-- A viewer qualifies after 5 minutes (300 seconds) of video playback

DROP TABLE IF EXISTS `channel_viewers_daily`;

CREATE TABLE IF NOT EXISTS `channel_viewers_daily` (
  -- stat_date represents the START of the 24-hour window in UTC
  -- e.g., '2025-01-12' means window [Jan 12 07:00 UTC ... Jan 13 06:59:59 UTC]
  `stat_date`     TEXT    NOT NULL,  -- 'YYYY-MM-DD' format (window start date)
  `channel_login` TEXT    NOT NULL,  -- Twitch channel login (lowercase)
  `riot_puuid`    TEXT    NOT NULL,  -- Riot PUUID of the viewer
  `created_at`    INTEGER NOT NULL DEFAULT (unixepoch('now')),  -- Unix timestamp when row was created
  
  PRIMARY KEY (stat_date, channel_login, riot_puuid)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_cvd_channel_date ON `channel_viewers_daily`(channel_login, stat_date);
CREATE INDEX IF NOT EXISTS idx_cvd_puuid_date   ON `channel_viewers_daily`(riot_puuid, stat_date);
CREATE INDEX IF NOT EXISTS idx_cvd_created_at   ON `channel_viewers_daily`(created_at);

-- Example queries:
-- Daily viewers for a channel:
-- SELECT COUNT(DISTINCT riot_puuid) FROM channel_viewers_daily WHERE channel_login = ? AND stat_date = ?

-- Channels a viewer watched today:
-- SELECT DISTINCT channel_login FROM channel_viewers_daily WHERE riot_puuid = ? AND stat_date = ?

-- Total unique viewers across all channels for a day:
-- SELECT COUNT(DISTINCT riot_puuid) FROM channel_viewers_daily WHERE stat_date = ?

-- Cleanup old data (e.g., keep only last 90 days):
-- DELETE FROM channel_viewers_daily WHERE created_at < unixepoch('now') - (90 * 24 * 60 * 60)