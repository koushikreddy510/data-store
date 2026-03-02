"""
Fetches MCX commodity futures symbols from FYERS symbol master
and upserts them into the commodity_symbols table.

Targets: GOLD, SILVER, COPPER, ALUMINIUM, CRUDEOIL, NATURALGAS
"""

import pandas as pd
import requests
import certifi
from io import StringIO
import psycopg
import re
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

FYERS_MCX_URL = "https://public.fyers.in/sym_details/MCX_COM.csv"

TARGET_COMMODITIES = {
    "GOLD": "GOLD",
    "GOLDM": "GOLD",
    "GOLDPETAL": "GOLD",
    "SILVER": "SILVER",
    "SILVERM": "SILVER",
    "SILVERMIC": "SILVER",
    "COPPER": "COPPER",
    "COPPERM": "COPPER",
    "ALUMINIUM": "ALUMINIUM",
    "ALUMINI": "ALUMINIUM",
    "CRUDEOIL": "CRUDEOIL",
    "CRUDEOILM": "CRUDEOIL",
    "NATURALGAS": "NATURALGAS",
    "NATGAS": "NATURALGAS",
    "NATGASMINI": "NATURALGAS",
}

print("Downloading FYERS MCX symbol master...")
resp = requests.get(FYERS_MCX_URL, verify=certifi.where(), timeout=30)
resp.raise_for_status()

df = pd.read_csv(StringIO(resp.text), header=None)

# FYERS MCX CSV columns:
# 0 -> fyers_token (numeric)
# 1 -> name/description
# 5 -> some identifier
# 9 -> tradingsymbol (e.g. MCX:GOLDM25JUNFUT)
# 11 -> lot_size
# 14 -> expiry (epoch or date)

symbols_df = df[[0, 1, 9]].copy()
symbols_df.columns = ["fyers_token", "name", "tradingsymbol"]

# Keep only FUT symbols
symbols_df = symbols_df[symbols_df["tradingsymbol"].str.contains("FUT", na=False)]
symbols_df = symbols_df.dropna(subset=["tradingsymbol"])

def extract_underlying(tradingsymbol):
    """Extract the commodity name from tradingsymbol like MCX:GOLDM25JUNFUT"""
    sym = tradingsymbol.split(":")[-1] if ":" in tradingsymbol else tradingsymbol
    match = re.match(r'^([A-Z]+?)(\d)', sym)
    if match:
        return match.group(1)
    return sym.rstrip("FUT")

def extract_expiry(tradingsymbol):
    """Try to extract expiry date from symbol name like GOLDM25JUNFUT -> 2025-06"""
    sym = tradingsymbol.split(":")[-1] if ":" in tradingsymbol else tradingsymbol
    match = re.search(r'(\d{2})([A-Z]{3})FUT$', sym)
    if match:
        year = 2000 + int(match.group(1))
        month_str = match.group(2)
        months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                  "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        month = months.get(month_str, 1)
        try:
            return datetime(year, month, 28).date()
        except ValueError:
            return datetime(year, month, 27).date()
    return None

symbols_df["raw_underlying"] = symbols_df["tradingsymbol"].apply(extract_underlying)
symbols_df["underlying"] = symbols_df["raw_underlying"].map(TARGET_COMMODITIES)
symbols_df = symbols_df.dropna(subset=["underlying"])
symbols_df["expiry_date"] = symbols_df["tradingsymbol"].apply(extract_expiry)

print(f"Found {len(symbols_df)} MCX commodity futures symbols")
print(symbols_df[["tradingsymbol", "underlying", "expiry_date"]].head(20))

conn = psycopg.connect(**DB_CONFIG)
conn.autocommit = False

upsert_sql = """
INSERT INTO commodity_symbols (tradingsymbol, fyers_token, name, underlying, expiry_date, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (tradingsymbol)
DO UPDATE SET
  fyers_token = EXCLUDED.fyers_token,
  name = EXCLUDED.name,
  underlying = EXCLUDED.underlying,
  expiry_date = EXCLUDED.expiry_date,
  updated_at = now();
"""

with conn.cursor() as cur:
    for row in symbols_df.itertuples(index=False):
        cur.execute(
            upsert_sql,
            (row.tradingsymbol, str(row.fyers_token), row.name, row.underlying, row.expiry_date),
        )

conn.commit()
conn.close()

print("Commodity symbols synced to Postgres successfully.")
