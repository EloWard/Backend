-- Channel Daily Snapshots Table
-- Stores daily metrics for temporal trend charts
-- One row per channel per day (only created if channel had viewers that day)
-- Updated every 3 hours by stats-worker.js cron job (uses ON CONFLICT to update same day's row)

DROP TABLE IF EXISTS `channel_daily_snapshots`;

CREATE TABLE IF NOT EXISTS `channel_daily_snapshots` (
  -- Composite primary key (one snapshot per channel per day)
  `stat_date`               TEXT NOT NULL,  -- Window start date in YYYY-MM-DD format
  `channel_twitch_id`       TEXT NOT NULL,  -- Twitch channel login (lowercase)

  -- Daily metrics (viewers who qualified on THIS specific day only)
  `daily_viewer_count`      INTEGER NOT NULL,  -- Count of unique viewers on this specific day
  `daily_avg_rank_score`    REAL,              -- Average rank score of viewers on this day
  `daily_avg_lp`            INTEGER,           -- Average LP of viewers on this day
  `daily_median_rank_score` REAL,              -- Median rank score of viewers on this day
  `daily_median_lp`         INTEGER,           -- Median LP of viewers on this day

  -- All-time metrics snapshot (as of this date)
  -- These represent the channel's all-time stats as of this specific day
  `alltime_avg_rank_score`  REAL,              -- All-time average rank score as of this date
  `alltime_avg_lp`          INTEGER,           -- All-time average LP as of this date
  `alltime_median_rank_score` REAL,            -- All-time median rank score as of this date
  `alltime_median_lp`       INTEGER,           -- All-time median LP as of this date
  `alltime_viewer_count`    INTEGER,           -- Total unique viewers (lifetime) as of this date

  -- Metadata
  `created_at`              INTEGER NOT NULL DEFAULT (unixepoch('now')),

  PRIMARY KEY (`stat_date`, `channel_twitch_id`)
);

-- Index for trend queries (get last N days for a specific channel)
-- Sorted DESC so most recent data is first
CREATE INDEX IF NOT EXISTS `idx_daily_channel`
  ON `channel_daily_snapshots`(`channel_twitch_id`, `stat_date` DESC);

-- Index for date-based queries (all channels on a specific day)
CREATE INDEX IF NOT EXISTS `idx_daily_date`
  ON `channel_daily_snapshots`(`stat_date`);

-- Index for cleanup queries (finding old snapshots)
CREATE INDEX IF NOT EXISTS `idx_daily_created`
  ON `channel_daily_snapshots`(`created_at`);
