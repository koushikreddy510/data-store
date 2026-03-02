CREATE TABLE symbols (
  tradingsymbol TEXT PRIMARY KEY,
  fyers_token TEXT,
  name TEXT,
  isin TEXT,
  updated_at TIMESTAMP DEFAULT now()
);