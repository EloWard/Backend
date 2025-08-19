-- Drop the existing table if it exists
DROP TABLE IF EXISTS `subscriptions`;

-- Create the subscriptions table for payment/subscription management
CREATE TABLE IF NOT EXISTS `subscriptions` (
  `twitch_id` TEXT PRIMARY KEY,  -- Primary key and reference to users.twitch_id
  `stripe_customer_id` TEXT UNIQUE, -- Stripe Customer ID should be unique
  `stripe_subscription_id` TEXT UNIQUE, -- Stripe Subscription ID should be unique
  `subscription_end_date` TIMESTAMP, -- When subscription expires
  `plus_active` BOOLEAN DEFAULT 0, -- Whether plus features are active
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  -- Foreign key constraint to link with users table
  FOREIGN KEY (`twitch_id`) REFERENCES `users` (`twitch_id`) ON DELETE CASCADE
);

-- Add indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_subscriptions_stripe_customer_id ON `subscriptions` (`stripe_customer_id`); 
CREATE INDEX IF NOT EXISTS idx_subscriptions_stripe_subscription_id ON `subscriptions` (`stripe_subscription_id`); 
CREATE INDEX IF NOT EXISTS idx_subscriptions_plus_active ON `subscriptions` (`plus_active`);
CREATE INDEX IF NOT EXISTS idx_subscriptions_end_date ON `subscriptions` (`subscription_end_date`);
