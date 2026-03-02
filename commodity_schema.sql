-- Commodity futures symbols (MCX)
CREATE TABLE IF NOT EXISTS commodity_symbols (
  tradingsymbol TEXT PRIMARY KEY,        -- e.g. MCX:GOLDM25JUNFUT
  fyers_token TEXT,
  name TEXT,                              -- e.g. GOLD, SILVER, CRUDEOIL
  underlying TEXT,                        -- normalized: GOLD, SILVER, COPPER, ALUMINIUM, CRUDEOIL, NATURALGAS
  expiry_date DATE,
  lot_size INTEGER,
  updated_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_commodity_symbols_underlying ON commodity_symbols(underlying);

-- Multi-timeframe OHLCV for commodities
-- 1D timeframe
CREATE TABLE IF NOT EXISTS commodity_ohlcv_1d (
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
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_1d_date ON commodity_ohlcv_1d(date);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_1d_symbol ON commodity_ohlcv_1d(tradingsymbol);

-- 4H timeframe
CREATE TABLE IF NOT EXISTS commodity_ohlcv_4h (
  tradingsymbol TEXT NOT NULL,
  datetime TIMESTAMP NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (tradingsymbol, datetime)
);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_4h_datetime ON commodity_ohlcv_4h(datetime);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_4h_symbol ON commodity_ohlcv_4h(tradingsymbol);

-- 2H timeframe
CREATE TABLE IF NOT EXISTS commodity_ohlcv_2h (
  tradingsymbol TEXT NOT NULL,
  datetime TIMESTAMP NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (tradingsymbol, datetime)
);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_2h_datetime ON commodity_ohlcv_2h(datetime);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_2h_symbol ON commodity_ohlcv_2h(tradingsymbol);

-- 1H timeframe
CREATE TABLE IF NOT EXISTS commodity_ohlcv_1h (
  tradingsymbol TEXT NOT NULL,
  datetime TIMESTAMP NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (tradingsymbol, datetime)
);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_1h_datetime ON commodity_ohlcv_1h(datetime);
CREATE INDEX IF NOT EXISTS idx_commodity_ohlcv_1h_symbol ON commodity_ohlcv_1h(tradingsymbol);
