"""
Populate symbols.sector from Yahoo Finance (yfinance).

Yahoo Finance uses standard 11-sector taxonomy (Financial Services, Technology, …).
We store the Yahoo sector in symbols.sector. The terminal's sector_groups.py
already handles these names via a self-mapping added alongside this script.

Usage:
  python enrich_sectors_yfinance.py [--workers N] [--sleep S] [--force]

  --force  overwrite even if sector already set (default: skip already-set)
"""
import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

from db_config import get_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
_lock = threading.Lock()


def _to_yf(sym: str) -> str | None:
    if sym.startswith("NSE:"):
        base = sym[4:]
        for sfx in ("-EQ", "-BE", "-SM", "-BL", "-IL"):
            if base.endswith(sfx):
                base = base[: -len(sfx)]
                break
        return f"{base}.NS"
    return None


def _fetch_one(tradingsymbol: str, sleep: float) -> tuple[str, str | None, str]:
    yf_sym = _to_yf(tradingsymbol)
    if not yf_sym:
        return tradingsymbol, None, "skip"
    try:
        info = yf.Ticker(yf_sym).info or {}
        sector = info.get("sector") or info.get("sectorDisp") or None
        if sleep > 0:
            time.sleep(sleep)
        return tradingsymbol, sector, "ok"
    except Exception as e:
        return tradingsymbol, None, f"error:{e}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--sleep", type=float, default=0.3)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true", help="Overwrite existing sector values")
    args = ap.parse_args()

    conn = get_conn()
    with conn.cursor() as cur:
        if args.force:
            cur.execute("SELECT tradingsymbol FROM symbols WHERE tradingsymbol LIKE 'NSE:%-EQ' ORDER BY tradingsymbol")
        else:
            cur.execute("SELECT tradingsymbol FROM symbols WHERE tradingsymbol LIKE 'NSE:%-EQ' AND COALESCE(TRIM(sector), '') = '' ORDER BY tradingsymbol")
        symbols = [r[0] for r in cur.fetchall()]
    conn.close()

    if args.limit:
        symbols = symbols[: args.limit]

    log.info("Enriching sector for %d symbols (force=%s, workers=%d)", len(symbols), args.force, args.workers)

    done = ok = no_data = errors = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_fetch_one, sym, args.sleep): sym for sym in symbols}
        batch: list[tuple[str, str]] = []
        for future in as_completed(futures):
            sym, sector, status = future.result()
            done += 1
            if status == "ok" and sector:
                ok += 1
                batch.append((sector, sym))
            elif sector is None:
                no_data += 1
            else:
                errors += 1

            # Flush in batches of 50
            if len(batch) >= 50 or (done == len(symbols) and batch):
                conn = get_conn()
                with conn.cursor() as cur:
                    cur.executemany("UPDATE symbols SET sector = %s WHERE tradingsymbol = %s", batch)
                conn.commit()
                conn.close()
                batch = []

            if done % 100 == 0 or done == len(symbols):
                with _lock:
                    print(f"[{done}/{len(symbols)}] ok={ok} no_data={no_data} errors={errors}", flush=True)

    print(f"\nDone. ok={ok} no_data={no_data} errors={errors}")


if __name__ == "__main__":
    main()
