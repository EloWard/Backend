-- Drop the existing table if it exists
DROP TABLE IF EXISTS `users`;

-- Create the users table storing complete Twitch user data
CREATE TABLE IF NOT EXISTS `users` (
  -- Core Twitch user data (ordered as received from Twitch API)
  `twitch_id` TEXT PRIMARY KEY,         -- Twitch user ID (unique identifier)
  `channel_name` TEXT NOT NULL,         -- Twitch login/username (stored in lowercase)
  `display_name` TEXT,                  -- Twitch display name
  `type` TEXT,                          -- User type (e.g., "", "admin", "global_mod", "staff")
  `broadcaster_type` TEXT,              -- Broadcaster type (e.g., "", "affiliate", "partner")
  `description` TEXT,                   -- User bio/description
  `profile_image_url` TEXT,             -- Profile image URL
  `offline_image_url` TEXT,             -- Offline banner image URL
  `view_count` INTEGER DEFAULT 0,       -- Total channel view count
  `email` TEXT,                         -- Twitch email (nullable)
  `twitch_created_at` TEXT,             -- When Twitch account was created (ISO string)
  
  -- EloWard app-specific data
  `db_reads` INTEGER DEFAULT 0,         -- Number of rank lookups performed
  `successful_lookups` INTEGER DEFAULT 0, -- Number of successful rank displays
  `channel_active` BOOLEAN DEFAULT 1,   -- Whether EloWard is active for this channel
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When user joined EloWard
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Last update timestamp
);

-- Add indexes for faster lookups
DROP INDEX IF EXISTS idx_users_channel_name;
DROP INDEX IF EXISTS idx_users_twitch_id;
DROP INDEX IF EXISTS idx_users_stripe_customer_id;
DROP INDEX IF EXISTS idx_users_stripe_subscription_id;
DROP INDEX IF EXISTS idx_users_channel_active;
DROP INDEX IF EXISTS idx_users_email;

-- Create indexes (twitch_id is already indexed as PRIMARY KEY)
CREATE INDEX IF NOT EXISTS idx_users_channel_name ON `users` (`channel_name`);
CREATE INDEX IF NOT EXISTS idx_users_channel_active ON `users` (`channel_active`);
CREATE INDEX IF NOT EXISTS idx_users_email ON `users` (`email`);
CREATE INDEX IF NOT EXISTS idx_users_broadcaster_type ON `users` (`broadcaster_type`); 

-- Note: D1 handles updated_at via application-side logic in workers
-- Triggers are not needed as workers update the timestamp explicitly

-- Note: channel_name uniqueness is not enforced to allow multiple accounts
-- with same username if they disconnect/reconnect. twitch_id is the unique identifier. 