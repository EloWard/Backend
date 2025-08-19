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
  last_updated INTEGER NOT NULL
);

-- Add index for efficient Twitch username lookups (most common search)
CREATE INDEX IF NOT EXISTS idx_lol_ranks_twitch_username ON lol_ranks(twitch_username);
-- Add index for riot_id lookups if needed
CREATE INDEX IF NOT EXISTS idx_lol_ranks_riot_id ON lol_ranks(riot_id); 