"""
Enriches the symbols table with market cap, listing date, and sector
data from NSE India's public API.

Usage:
  python enrich_market_cap.py

Fetches data for each ISIN from NSE and updates the symbols table.
Rate-limited to avoid getting blocked.
"""

import time
import requests
import psycopg

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_BASE_URL = "https://www.nseindia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

SLEEP_BETWEEN = 0.5


def get_nse_session():
    """Create a session with NSE cookies."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get(NSE_BASE_URL, timeout=10)
    return session


def extract_symbol_name(tradingsymbol: str) -> str:
    """NSE:SBIN-EQ -> SBIN"""
    sym = tradingsymbol
    if ":" in sym:
        sym = sym.split(":")[1]
    if sym.endswith("-EQ"):
        sym = sym[:-3]
    return sym


def fetch_nse_data(session: requests.Session, nse_symbol: str) -> dict:
    """Fetch quote data from NSE for a symbol."""
    try:
        resp = session.get(
            NSE_QUOTE_URL,
            params={"symbol": nse_symbol},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 401:
            session.get(NSE_BASE_URL, timeout=10)
            resp = session.get(NSE_QUOTE_URL, params={"symbol": nse_symbol}, timeout=10)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        print(f"  Error fetching {nse_symbol}: {e}")
    return None


def main():
    conn = psycopg.connect(**DB_CONFIG)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol, isin FROM symbols ORDER BY tradingsymbol;")
        symbols = cur.fetchall()

    print(f"Found {len(symbols)} symbols to enrich")

    session = get_nse_session()

    update_sql = """
    UPDATE symbols
       SET market_cap = %s,
           listing_date = %s,
           sector = %s,
           updated_at = now()
     WHERE tradingsymbol = %s;
    """

    success = 0
    failed = 0

    for i, (tradingsymbol, isin) in enumerate(symbols, start=1):
        nse_sym = extract_symbol_name(tradingsymbol)
        print(f"[{i}/{len(symbols)}] {nse_sym} ...", end=" ")

        data = fetch_nse_data(session, nse_sym)

        if not data:
            print("SKIP (no data)")
            failed += 1
            time.sleep(SLEEP_BETWEEN)
            continue

        try:
            info = data.get("info", {})
            metadata = data.get("metadata", {})
            security_info = data.get("securityInfo", {})

            market_cap_raw = security_info.get("issuedSize", 0)
            last_price = data.get("priceInfo", {}).get("lastPrice", 0)
            market_cap = float(market_cap_raw or 0) * float(last_price or 0)

            listing_date_str = metadata.get("listingDate")
            listing_date = None
            if listing_date_str:
                try:
                    from datetime import datetime
                    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                        try:
                            listing_date = datetime.strptime(listing_date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass

            sector = metadata.get("industry", "") or info.get("industry", "") or ""

            with conn.cursor() as cur:
                cur.execute(update_sql, (market_cap, listing_date, sector, tradingsymbol))
            conn.commit()

            mcap_cr = market_cap / 1e7
            print(f"OK (MCap: {mcap_cr:,.0f} Cr, Listed: {listing_date}, Sector: {sector[:30]})")
            success += 1

        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            failed += 1

        time.sleep(SLEEP_BETWEEN)

        if i % 100 == 0:
            print(f"  --- Refreshing NSE session ---")
            session = get_nse_session()
            time.sleep(2)

    conn.close()
    print(f"\nDone. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()
