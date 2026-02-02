-- Migration: Add client_order_id column to trades table (nullable, not unique)
ALTER TABLE trades
ADD COLUMN client_order_id VARCHAR(255);
-- No NOT NULL or UNIQUE constraint 