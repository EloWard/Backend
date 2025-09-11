-- Create table for storing League of Legends rank data
DROP TABLE IF EXISTS lol_ranks;
CREATE TABLE lol_ranks (
  riot_puuid TEXT PRIMARY KEY,        -- PUUID as primary key ensures uniqueness
  twitch_username TEXT NOT NULL,      -- Current Twitch username (can change)
  riot_id TEXT,                       -- Storing gameName#tagLine
  rank_tier TEXT NOT NULL,
  rank_division TEXT,
  lp INTEGER DEFAULT 0,
  region TEXT,                        -- Riot API region (e.g., na1, euw1, kr)
  last_updated INTEGER NOT NULL,
  plus_active BOOLEAN DEFAULT 0,      -- Premium subscription status for badge display (added via migration)
  peak_rank_tier TEXT,               -- Peak rank tier achieved
  peak_rank_division TEXT,           -- Peak rank division achieved
  peak_lp INTEGER,                   -- Peak LP achieved
  show_peak BOOLEAN DEFAULT 0,       -- Whether to display peak rank instead of current rank
  animate_badge BOOLEAN DEFAULT 0    -- Whether to show animated badge effects
);

-- Add index for efficient Twitch username lookups (most common search)
CREATE INDEX IF NOT EXISTS idx_lol_ranks_twitch_username ON lol_ranks(twitch_username);
-- Add index for riot_id lookups if needed
CREATE INDEX IF NOT EXISTS idx_lol_ranks_riot_id ON lol_ranks(riot_id); 