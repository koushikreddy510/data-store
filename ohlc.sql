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
CREATE INDEX IF NOT EXISTS idx_ohlcv_1d_symbol ON ohlcv_1d(tradingsymbol);