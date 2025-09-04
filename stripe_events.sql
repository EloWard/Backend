-- Create table for Stripe event idempotency tracking
-- This prevents duplicate processing of Stripe webhook events

DROP TABLE IF EXISTS stripe_events;
CREATE TABLE stripe_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stripe_event_id TEXT UNIQUE NOT NULL,   -- Stripe's event ID for idempotency
  event_type TEXT NOT NULL,               -- Event type (e.g., invoice.paid, subscription.updated)
  processed_at INTEGER NOT NULL,          -- Timestamp when event was processed
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  
  -- Indexes for performance
  UNIQUE(stripe_event_id)
);

-- Index for cleanup queries (remove old events periodically)
CREATE INDEX idx_stripe_events_processed_at ON stripe_events(processed_at);
CREATE INDEX idx_stripe_events_type ON stripe_events(event_type);
