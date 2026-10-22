"""
Download 1D OHLCV for all NSE equity symbols via Yahoo Finance and upsert
into the ohlcv_1d table.

yfinance is cloud-friendly (no auth, no IP restrictions) and maps NSE symbols
trivially: "NSE:SBIN-EQ" -> "SBIN.NS"

Optional env:
  DATABASE_URL   - postgres DSN (falls back to PG* vars via db_config.py)
  HISTORY_DAYS   - days of history to fetch (default 365)
  BATCH_SIZE     - symbols per yfinance download call (default 100)
"""
import logging
import os
import sys
from datetime import datetime, timedelta

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

import pandas as pd
from db_config import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "365"))
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "100"))


def _to_yf_ticker(tradingsymbol: str) -> str | None:
    """Convert 'NSE:SBIN-EQ' -> 'SBIN.NS'; skip non-NSE or special segments."""
    if tradingsymbol.startswith("NSE:"):
        base = tradingsymbol[4:]          # strip "NSE:"
        # Strip standard equity suffix (-EQ, -BE, -SM, -N1, -N2 …)
        for suffix in ("-EQ", "-BE", "-SM", "-BL", "-IL", "-N1", "-N2", "-N3", "-N4"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        return f"{base}.NS"
    if tradingsymbol.startswith("BSE:"):
        base = tradingsymbol[4:]
        for suffix in ("-A", "-B", "-E", "-Z", "-T"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        return f"{base}.BO"
    return None


UPSERT_SQL = """
INSERT INTO ohlcv_1d (tradingsymbol, date, open, high, low, close, volume, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (tradingsymbol, date)
DO UPDATE SET
  open       = EXCLUDED.open,
  high       = EXCLUDED.high,
  low        = EXCLUDED.low,
  close      = EXCLUDED.close,
  volume     = EXCLUDED.volume,
  updated_at = now();
"""


def _upsert_batch(conn, rows: list[tuple]) -> int:
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


def main():
    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM symbols ORDER BY tradingsymbol")
        all_symbols = [r[0] for r in cur.fetchall()]

    log.info("Total symbols in DB: %d", len(all_symbols))

    # Build ticker -> tradingsymbol map, skip unmappable ones
    ticker_map: dict[str, str] = {}
    for sym in all_symbols:
        t = _to_yf_ticker(sym)
        if t:
            ticker_map[t] = sym

    tickers = list(ticker_map)
    log.info("Fetching %d tickers from Yahoo Finance in batches of %d", len(tickers), BATCH_SIZE)

    today      = datetime.now().date()
    start_date = (today - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    end_date   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    total_rows = success = failed = 0

    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        log.info("Batch %d / %d  (%d symbols)", batch_num, -(-len(tickers) // BATCH_SIZE), len(batch))

        try:
            df = yf.download(
                batch,
                start=start_date,
                end=end_date,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            log.error("yfinance download failed for batch %d: %s", batch_num, e)
            failed += len(batch)
            continue

        if df.empty:
            log.warning("Batch %d returned no data", batch_num)
            failed += len(batch)
            continue

        rows: list[tuple] = []
        for ticker in batch:
            tradingsymbol = ticker_map[ticker]
            try:
                # Multi-ticker download nests columns under ticker; single ticker is flat
                if len(batch) == 1:
                    sym_df = df
                else:
                    sym_df = df[ticker] if ticker in df.columns.get_level_values(0) else pd.DataFrame()

                if sym_df.empty:
                    continue

                for dt_idx, row in sym_df.iterrows():
                    dt = dt_idx.date() if hasattr(dt_idx, "date") else dt_idx
                    o = row.get("Open") or row.get("open")
                    h = row.get("High") or row.get("high")
                    l = row.get("Low")  or row.get("low")
                    c = row.get("Close") or row.get("close")
                    v = row.get("Volume") or row.get("volume") or 0
                    if any(pd.isna(x) for x in (o, h, l, c)):
                        continue
                    rows.append((tradingsymbol, dt, float(o), float(h), float(l), float(c), int(v)))
                success += 1
            except Exception as e:
                log.warning("Parse failed for %s: %s", ticker, e)
                failed += 1

        if rows:
            inserted = _upsert_batch(conn, rows)
            total_rows += inserted

    # Retention: ~4 years, so backtests have a year of warm-up (52-week high, 200-day average) before a 3-year window
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ohlcv_1d WHERE date < CURRENT_DATE - INTERVAL '1500 days'")
        deleted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"Done. Symbols ok={success} failed={failed}, Rows upserted={total_rows}, Old rows deleted={deleted}")


if __name__ == "__main__":
    main()
