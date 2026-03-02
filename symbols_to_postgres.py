import pandas as pd
import requests
import certifi
from io import StringIO
import psycopg

# --------------------
# Config
# --------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

FYERS_SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"

# --------------------
# Download CSV
# --------------------
print("Downloading FYERS symbol master...")

resp = requests.get(FYERS_SYMBOL_MASTER_URL, verify=certifi.where(), timeout=30)
resp.raise_for_status()

df = pd.read_csv(StringIO(resp.text), header=None)

# Columns we care about:
# 0 -> fyers_token
# 1 -> name
# 5 -> isin
# 9 -> tradingsymbol (e.g., NSE:SBIN-EQ)

symbols_df = df[[0, 1, 5, 9]].copy()
symbols_df.columns = ["fyers_token", "name", "isin", "tradingsymbol"]

# Keep only NSE equity (-EQ) if you want
symbols_df = symbols_df[symbols_df["tradingsymbol"].str.endswith("-EQ")]

# Drop rows with missing tradingsymbol just in case
symbols_df = symbols_df.dropna(subset=["tradingsymbol"])

print(f"Total symbols to upsert: {len(symbols_df)}")
print(symbols_df.head())

# --------------------
# Insert / Upsert into Postgres
# --------------------
conn = psycopg.connect(**DB_CONFIG)
conn.autocommit = False

upsert_sql = """
INSERT INTO symbols (tradingsymbol, fyers_token, name, isin, updated_at)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (tradingsymbol)
DO UPDATE SET
  fyers_token = EXCLUDED.fyers_token,
  name = EXCLUDED.name,
  isin = EXCLUDED.isin,
  updated_at = now();
"""

with conn.cursor() as cur:
    for row in symbols_df.itertuples(index=False):
        cur.execute(
            upsert_sql,
            (row.tradingsymbol, str(row.fyers_token), row.name, row.isin),
        )

conn.commit()
conn.close()

print("✅ Symbols synced to Postgres successfully.")