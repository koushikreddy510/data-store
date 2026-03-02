-- Add market_cap, listing_date, sector columns to symbols table
ALTER TABLE symbols ADD COLUMN IF NOT EXISTS market_cap DOUBLE PRECISION DEFAULT 0;
ALTER TABLE symbols ADD COLUMN IF NOT EXISTS listing_date DATE;
ALTER TABLE symbols ADD COLUMN IF NOT EXISTS sector TEXT DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_symbols_market_cap ON symbols(market_cap);
CREATE INDEX IF NOT EXISTS idx_symbols_listing_date ON symbols(listing_date);
