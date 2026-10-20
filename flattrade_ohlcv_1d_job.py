"""
Fetch 1D OHLCV candles for all NSE equities from Flattrade's TPSeries API
and upsert into the ohlcv_1d table.

Requires env (set in deploy/.env, forwarded by docker-compose):
  FLATTRADE_USER_ID  - e.g. FT029182
  FLATTRADE_TOKEN    - today's session token (updated daily by deploy.sh flattrade-login)

Optional env:
  DATABASE_URL       - postgres DSN (falls back to PG* vars via db_config.py)
  HISTORY_DAYS       - days of history to fetch (default 365)
"""
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests as http

from db_config import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://piconnect.flattrade.in/PiConnectAPI"
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "365"))
SLEEP_BETWEEN_CALLS = 0.2   # seconds; be polite to the API

USER_ID = os.getenv("FLATTRADE_USER_ID", "")
TOKEN = os.getenv("FLATTRADE_TOKEN", "")

if not USER_ID or not TOKEN:
    print("FLATTRADE_USER_ID and FLATTRADE_TOKEN must be set. Run deploy.sh flattrade-login first.")
    sys.exit(1)


# ---------- Flattrade REST helper ----------------------------------------

def _ft_post(endpoint: str, payload: dict):
    body = {"uid": USER_ID, **payload}
    # Send as raw string — Flattrade's REST API expects un-encoded JSON in jData;
    # passing a dict to requests would URL-encode { } " which the server rejects.
    raw = f"jData={json.dumps(body, separators=(',', ':'))}&jKey={TOKEN}"
    resp = http.post(f"{BASE_URL}/{endpoint}", data=raw.encode(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and str(data.get("stat", "")).lower() == "not_ok":
        raise RuntimeError(f"{endpoint}: {data.get('emsg') or 'Not_Ok'}")
    return data


# ---------- Token resolution (cached in symbols.flattrade_token) ----------

def _resolve_token(tradingsymbol: str, conn) -> tuple[str, str] | tuple[None, None]:
    """Return (exchange, broker_token) for a tradingsymbol like 'NSE:SBIN-EQ', caching in DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT flattrade_token FROM symbols WHERE tradingsymbol = %s", (tradingsymbol,))
        row = cur.fetchone()
        if row and row[0]:
            # Cached: derive exchange from symbol prefix
            exch = tradingsymbol.split(":")[0] if ":" in tradingsymbol else "NSE"
            return exch, row[0]
    # Strip exchange prefix: "NSE:SBIN-EQ" -> exch="NSE", tsym="SBIN-EQ"
    if ":" in tradingsymbol:
        exch, tsym = tradingsymbol.split(":", 1)
    else:
        exch, tsym = "NSE", tradingsymbol
    try:
        data = _ft_post("SearchScrip", {"stext": tsym, "exch": exch})
        matches = [m for m in (data.get("values") or []) if m.get("tsym") == tsym]
        if not matches:
            return None, None
        tok = str(matches[0]["token"])
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE symbols SET flattrade_token = %s WHERE tradingsymbol = %s",
                (tok, tradingsymbol),
            )
        conn.commit()
        return exch, tok
    except Exception as e:
        log.warning("SearchScrip failed for %s: %s", tradingsymbol, e)
        return None, None


# ---------- Candle parsing ------------------------------------------------

def _parse_date(time_str: str) -> date | None:
    """Flattrade bar timestamps: 'DD-MM-YYYY HH:MM:SS' or 'DD/MM/YYYY HH:MM:SS'."""
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(time_str.strip(), fmt).date()
        except ValueError:
            continue
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


def _fetch_and_upsert(tradingsymbol: str, exch: str, ft_token: str, conn, today: date) -> int:
    start_ts = int(datetime.combine(today - timedelta(days=HISTORY_DAYS), datetime.min.time()).timestamp())
    end_ts = int(datetime.combine(today + timedelta(days=1), datetime.min.time()).timestamp())

    # Daily bars: one request covers the full history window
    try:
        rows = _ft_post("TPSeries", {
            "exch": exch,
            "token": ft_token,
            "st": str(start_ts),
            "et": str(end_ts),
            "intrv": "D",
        })
    except Exception as e:
        log.warning("TPSeries failed for %s: %s", tradingsymbol, e)
        return 0

    if isinstance(rows, dict):
        log.warning("TPSeries error for %s: %s", tradingsymbol, rows.get("emsg") or rows)
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for r in (rows or []):
            if str(r.get("stat", "Ok")).lower() != "ok":
                continue
            dt = _parse_date(r.get("time", ""))
            if dt is None:
                continue
            try:
                o = float(r["into"])
                h = float(r["inth"])
                l = float(r["intl"])
                c = float(r["intc"])
                v = int(float(r.get("intv") or 0))
                cur.execute(UPSERT_SQL, (tradingsymbol, dt, o, h, l, c, v))
                inserted += 1
            except (KeyError, ValueError):
                pass
    conn.commit()
    return inserted


# ---------- Main ----------------------------------------------------------

def main():
    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM symbols ORDER BY tradingsymbol")
        symbols = [r[0] for r in cur.fetchall()]

    log.info("Processing %d symbols", len(symbols))
    today = datetime.now().date()
    success = failed = skipped = 0

    for i, sym in enumerate(symbols, 1):
        exch, ft_token = _resolve_token(sym, conn)
        if not ft_token:
            skipped += 1
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        try:
            n = _fetch_and_upsert(sym, exch, ft_token, conn, today)
            success += 1
            if i % 100 == 0:
                log.info("[%d/%d] last=%s rows=%d  ok=%d skip=%d fail=%d",
                         i, len(symbols), sym, n, success, skipped, failed)
        except Exception as e:
            conn.rollback()
            failed += 1
            log.error("[%d/%d] %s failed: %s", i, len(symbols), sym, e)

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Retention: keep just over a year of history
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ohlcv_1d WHERE date < CURRENT_DATE - INTERVAL '400 days'")
        deleted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"Done. Success: {success}, Failed: {failed}, Skipped: {skipped}, Old rows deleted: {deleted}")


if __name__ == "__main__":
    main()
