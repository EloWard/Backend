-- Channel Statistics Cache Table
-- Stores computed all-time statistics per channel
-- Updated daily by stats-worker.js cron job at 07:10 UTC
--
-- This table is the source of truth for the leaderboard and channel stats pages
-- One row per channel that has ever had EloWard viewers

DROP TABLE IF EXISTS `channel_stats_cache`;

CREATE TABLE IF NOT EXISTS `channel_stats_cache` (
  -- Primary identifier
  `channel_twitch_id`         TEXT PRIMARY KEY,  -- Twitch channel login (lowercase)
  `channel_display_name`      TEXT,              -- Display name with proper capitalization

  -- All-time aggregate metrics
  `total_unique_viewers`      INTEGER NOT NULL DEFAULT 0,  -- Count of unique riot_puuids across all time
  `avg_rank_score`            REAL,                        -- Average numeric rank score (0-2000 scale)
  `avg_rank_tier`             TEXT,                        -- Human-readable tier (e.g., "PLATINUM")
  `avg_rank_division`         TEXT,                        -- Human-readable division (e.g., "II", null for Master+)
  `avg_lp`                    INTEGER,                     -- Average LP across all viewers (0-100)
  `median_rank_score`         REAL,                        -- Median numeric rank score (0-2000 scale)
  `median_rank_tier`          TEXT,                        -- Median rank tier
  `median_rank_division`      TEXT,                        -- Median rank division
  `median_lp`                 INTEGER,                     -- Median LP

  -- Top viewers metadata
  `top_viewers_json`          TEXT,                        -- JSON array of top 10 viewers: [{twitch_username, rank_tier, rank_division, score}]

  -- Metadata
  `last_updated`              INTEGER NOT NULL,            -- Unix timestamp of last computation
  `last_computed_stat_date`   TEXT,                        -- Last stat_date (YYYY-MM-DD) that was processed

  -- Leaderboard eligibility
  `is_eligible`               INTEGER DEFAULT 0 NOT NULL   -- 1 if >= 10 viewers (eligible for public leaderboard)
);

-- Index for leaderboard queries (ORDER BY avg_rank_score DESC)
-- Filtered index only includes eligible channels (optimization)
CREATE INDEX IF NOT EXISTS `idx_stats_avg_score`
  ON `channel_stats_cache`(`avg_rank_score` DESC)
  WHERE `is_eligible` = 1;

-- Index for sorting by viewer count
CREATE INDEX IF NOT EXISTS `idx_stats_viewers`
  ON `channel_stats_cache`(`total_unique_viewers` DESC);

-- Index for finding stale entries (if needed for cleanup)
CREATE INDEX IF NOT EXISTS `idx_stats_last_updated`
  ON `channel_stats_cache`(`last_updated`);
