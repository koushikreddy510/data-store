"""
Fetches OHLCV data for MCX commodity futures from FYERS API
across multiple timeframes (1D, 4H, 2H, 1H) and stores in PostgreSQL.

Usage:
  python commodity_ohlcv_job.py [--timeframe 1D|4H|2H|1H|ALL]

Defaults to ALL timeframes if not specified.
"""

import time
import sys
import os
import argparse
from datetime import datetime, timedelta
import psycopg
from fyers_apiv3 import fyersModel

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

FYERS_CLIENT_ID = "03VEQP97U0-100"
TOKEN_FILE = "fyers_access_token.txt"
HISTORY_DAYS = 365
SLEEP_BETWEEN_CALLS = 0.3

TIMEFRAME_CONFIG = {
    "1D": {
        "resolution": "1D",
        "table": "commodity_ohlcv_1d",
        "datetime_col": "date",
        "datetime_type": "date",
        "retention_days": 370,
    },
    "4H": {
        "resolution": "240",
        "table": "commodity_ohlcv_4h",
        "datetime_col": "datetime",
        "datetime_type": "datetime",
        "retention_days": 90,
    },
    "2H": {
        "resolution": "120",
        "table": "commodity_ohlcv_2h",
        "datetime_col": "datetime",
        "datetime_type": "datetime",
        "retention_days": 60,
    },
    "1H": {
        "resolution": "60",
        "table": "commodity_ohlcv_1h",
        "datetime_col": "datetime",
        "datetime_type": "datetime",
        "retention_days": 60,
    },
}

if not os.path.exists(TOKEN_FILE):
    print(f"Token file not found: {TOKEN_FILE}")
    print("Run generate_token.py first.")
    sys.exit(1)

with open(TOKEN_FILE, "r") as f:
    FYERS_ACCESS_TOKEN = f.read().strip()

if not FYERS_ACCESS_TOKEN:
    print("Token file is empty. Regenerate the token.")
    sys.exit(1)

fyers = fyersModel.FyersModel(
    client_id=FYERS_CLIENT_ID,
    token=FYERS_ACCESS_TOKEN,
    log_path=None,
)

print("Checking FYERS token validity...")
try:
    prof = fyers.get_profile()
    if not prof or prof.get("s") != "ok":
        print("Token seems invalid or expired.")
        print("Response:", prof)
        sys.exit(1)
except Exception as e:
    print("Failed to validate token:", e)
    sys.exit(1)

print("Token is valid. Proceeding...")

conn = psycopg.connect(**DB_CONFIG)
conn.autocommit = False


def get_commodity_symbols():
    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM commodity_symbols ORDER BY tradingsymbol;")
        return [r[0] for r in cur.fetchall()]


def build_upsert_sql(table, datetime_col):
    return f"""
    INSERT INTO {table}
    (tradingsymbol, {datetime_col}, open, high, low, close, volume, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
    ON CONFLICT (tradingsymbol, {datetime_col})
    DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      updated_at = now();
    """


def upsert_candles(symbol, candles, table, datetime_col, datetime_type):
    sql = build_upsert_sql(table, datetime_col)
    with conn.cursor() as cur:
        for c in candles:
            ts, o, h, l, cl, v = c
            if datetime_type == "date":
                dt = datetime.fromtimestamp(ts).date()
            else:
                dt = datetime.fromtimestamp(ts)
            cur.execute(sql, (symbol, dt, o, h, l, cl, int(v)))


def fetch_timeframe(symbols, tf_key, tf_config):
    print(f"\n{'='*60}")
    print(f"Fetching {tf_key} data for {len(symbols)} commodity symbols")
    print(f"{'='*60}")

    today = datetime.now().date()
    history_days = min(HISTORY_DAYS, tf_config["retention_days"])
    range_from = (today - timedelta(days=history_days)).strftime("%Y-%m-%d")
    range_to = today.strftime("%Y-%m-%d")

    success = 0
    failed = 0

    for i, symbol in enumerate(symbols, start=1):
        print(f"  [{i}/{len(symbols)}] {tf_key} {symbol} ...", end=" ")

        try:
            data = {
                "symbol": symbol,
                "resolution": tf_config["resolution"],
                "date_format": "1",
                "range_from": range_from,
                "range_to": range_to,
                "cont_flag": "1",
            }

            resp = fyers.history(data=data)

            if not resp or resp.get("s") != "ok":
                print(f"FAILED: {resp}")
                failed += 1
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            candles = resp.get("candles", [])
            if not candles:
                print("No candles")
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            upsert_candles(
                symbol, candles,
                tf_config["table"],
                tf_config["datetime_col"],
                tf_config["datetime_type"],
            )
            conn.commit()
            success += 1
            print(f"OK ({len(candles)} candles)")

        except Exception as e:
            conn.rollback()
            failed += 1
            print(f"ERROR: {e}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Retention cleanup
    retention = tf_config["retention_days"]
    datetime_col = tf_config["datetime_col"]
    table = tf_config["table"]
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {table} WHERE {datetime_col} < CURRENT_DATE - INTERVAL '{retention} days';"
        )
        deleted = cur.rowcount
    conn.commit()

    print(f"  {tf_key} done. Success: {success}, Failed: {failed}, Old rows deleted: {deleted}")
    return success, failed


def main():
    parser = argparse.ArgumentParser(description="Fetch commodity OHLCV data")
    parser.add_argument("--timeframe", default="ALL",
                        choices=["1D", "4H", "2H", "1H", "ALL"],
                        help="Timeframe to fetch (default: ALL)")
    args = parser.parse_args()

    symbols = get_commodity_symbols()
    if not symbols:
        print("No commodity symbols found. Run commodity_symbols_to_postgres.py first.")
        return

    print(f"Found {len(symbols)} commodity symbols")

    timeframes = list(TIMEFRAME_CONFIG.keys()) if args.timeframe == "ALL" else [args.timeframe]

    total_success = 0
    total_failed = 0

    for tf in timeframes:
        s, f = fetch_timeframe(symbols, tf, TIMEFRAME_CONFIG[tf])
        total_success += s
        total_failed += f

    print(f"\n{'='*60}")
    print(f"All done. Total success: {total_success}, Total failed: {total_failed}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
