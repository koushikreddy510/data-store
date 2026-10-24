CREATE TABLE IF NOT EXISTS ohlcv_1d (
  tradingsymbol TEXT NOT NULL,
  date DATE NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (tradingsymbol, date)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_1d_date ON ohlcv_1d(date);
-- the primary key (tradingsymbol, date) already serves lookups by symbol; a second index on tradingsymbol only slows writes
DROP INDEX IF EXISTS idx_ohlcv_1d_symbol;