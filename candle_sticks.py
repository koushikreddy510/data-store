import time
from datetime import datetime, timedelta
import psycopg
from fyers_apiv3 import fyersModel

# --------------------
# CONFIG
# --------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

# FYERS credentials
# You should already have an access token generated via FYERS auth flow
FYERS_CLIENT_ID = "YOUR_CLIENT_ID_HERE"
FYERS_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE"  # format: "appid:token" or just token depending on SDK

# How many days of history to keep
HISTORY_DAYS = 370  # ~1 year + buffer

# Rate limiting (be nice to the API)
SLEEP_BETWEEN_CALLS = 0.3  # seconds

# --------------------
# FYERS CLIENT
# --------------------
fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=None,
)

# --------------------
# DB CONNECTION
# --------------------
conn = psycopg.connect(**DB_CONFIG)
conn.autocommit = False

# --------------------
# HELPERS
# --------------------
def get_symbols():
    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM symbols ORDER BY tradingsymbol;")
        rows = cur.fetchall()
        return [r[0] for r in rows]


UPSERT_SQL = """
INSERT INTO ohlcv_1d
(tradingsymbol, date, open, high, low, close, volume, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (tradingsymbol, date)
DO UPDATE SET
  open = EXCLUDED.open,
  high = EXCLUDED.high,
  low = EXCLUDED.low,
  close = EXCLUDED.close,
  volume = EXCLUDED.volume,
  updated_at = now();
"""

def upsert_candles(symbol, candles):
    """
    candles: list of [timestamp, open, high, low, close, volume]
    """
    with conn.cursor() as cur:
        for c in candles:
            ts, o, h, l, cl, v = c
            # FYERS timestamp is usually in seconds
            dt = datetime.fromtimestamp(ts).date()

            cur.execute(
                UPSERT_SQL,
                (symbol, dt, o, h, l, cl, int(v)),
            )

# --------------------
# MAIN JOB
# --------------------
def main():
    symbols = get_symbols()
    print(f"Found {len(symbols)} symbols to process")

    today = datetime.now().date()
    range_from = (today - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    range_to = today.strftime("%Y-%m-%d")

    success = 0
    failed = 0

    for i, symbol in enumerate(symbols, start=1):
        print(f"[{i}/{len(symbols)}] Fetching {symbol} ...")

        try:
            data = {
                "symbol": symbol,
                "resolution": "1D",
                "date_format": "1",
                "range_from": range_from,
                "range_to": range_to,
                "cont_flag": "1",
            }

            resp = fyers.history(data=data)

            if not resp or resp.get("s") != "ok":
                print(f"  ❌ Failed for {symbol}: {resp}")
                failed += 1
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            candles = resp.get("candles", [])
            if not candles:
                print(f"  ⚠️ No candles for {symbol}")
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            upsert_candles(symbol, candles)
            conn.commit()
            success += 1
            print(f"  ✅ Stored {len(candles)} candles")

        except Exception as e:
            conn.rollback()
            failed += 1
            print(f"  ❌ Exception for {symbol}: {e}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Retention cleanup: keep only ~1 year
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ohlcv_1d WHERE date < CURRENT_DATE - INTERVAL '370 days';"
        )
        deleted = cur.rowcount
    conn.commit()

    print("--------------------------------------------------")
    print(f"Done. Success: {success}, Failed: {failed}, Old rows deleted: {deleted}")

if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()