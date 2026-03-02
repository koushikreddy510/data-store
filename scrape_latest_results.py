"""
Scrapes latest financial result announcements from NSE corporate announcements API.
Stores them in the result_announcements table and optionally triggers
financial data scraping for those symbols from screener.in.

Usage:
  python scrape_latest_results.py                    # last 7 days
  python scrape_latest_results.py --days 14          # last 14 days
  python scrape_latest_results.py --scrape-financials # also scrape financials for found symbols
"""

import argparse
import time
import re
from datetime import datetime, timedelta
import requests
import psycopg

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}

NSE_BASE = "https://www.nseindia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html",
    "Accept-Language": "en-US,en;q=0.9",
}

RESULT_KEYWORDS = [
    "financial result", "quarterly", "unaudited", "audited",
    "half year", "annual result", "profit after tax",
]

QUARTER_PATTERNS = [
    (r"quarter\s+ended?\s+(\w+\s+\d{1,2},?\s+\d{4})", None),
    (r"period\s+ended?\s+(\w+\s+\d{4})", None),
    (r"(Q[1-4]\s*FY\s*\d{2,4})", None),
    (r"(September|December|March|June)\s+\d{4}", None),
]


def get_nse_session():
    """Create a session with NSE cookies."""
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(NSE_BASE, timeout=15)
    return s


def fetch_announcements(session, from_date, to_date):
    """Fetch corporate announcements from NSE API."""
    fmt = "%d-%m-%Y"
    url = (
        f"{NSE_BASE}/api/corporate-announcements"
        f"?index=equities"
        f"&from_date={from_date.strftime(fmt)}"
        f"&to_date={to_date.strftime(fmt)}"
    )
    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        print(f"NSE API returned {resp.status_code}")
        return []
    return resp.json()


def is_financial_result(announcement):
    """Check if an announcement is about financial results."""
    desc = announcement.get("desc", "").lower()
    text = announcement.get("attchmntText", "").lower()
    combined = f"{desc} {text}"

    if "outcome of board meeting" in desc:
        return any(kw in combined for kw in RESULT_KEYWORDS)

    if "financial result" in desc:
        return True

    return False


def extract_quarter(text):
    """Try to extract the quarter/period from the announcement text."""
    for pattern, _ in QUARTER_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return ""


def parse_announcement_date(dt_str):
    """Parse NSE date format like '28-Feb-2026 19:05:09'."""
    try:
        return datetime.strptime(dt_str, "%d-%b-%Y %H:%M:%S").date()
    except (ValueError, TypeError):
        return None


def upsert_announcement(conn, symbol, company_name, result_date, quarter):
    """Insert or update a result announcement."""
    sql = """
    INSERT INTO result_announcements (nse_symbol, company_name, result_date, quarter, source, scraped_at)
    VALUES (%s, %s, %s, %s, 'nse', now())
    ON CONFLICT (nse_symbol, result_date, quarter)
    DO UPDATE SET company_name = EXCLUDED.company_name, scraped_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, company_name, result_date, quarter))
    conn.commit()


def get_our_symbols(conn):
    """Get the set of NSE symbols we track."""
    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM symbols;")
        rows = cur.fetchall()
    symbols = set()
    for r in rows:
        sym = r[0]
        if ":" in sym:
            sym = sym.split(":")[1]
        if sym.endswith("-EQ"):
            sym = sym[:-3]
        symbols.add(sym)
    return symbols


def main():
    parser = argparse.ArgumentParser(description="Scrape latest result announcements from NSE")
    parser.add_argument("--days", type=int, default=7, help="Look back N days (default 7)")
    parser.add_argument("--scrape-financials", action="store_true",
                        help="Also scrape financial details from screener.in for found symbols")
    args = parser.parse_args()

    conn = psycopg.connect(**DB_CONFIG)
    our_symbols = get_our_symbols(conn)
    print(f"Tracking {len(our_symbols)} symbols in our database")

    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=args.days)
    print(f"Fetching NSE announcements from {from_date} to {to_date}")

    session = get_nse_session()
    announcements = fetch_announcements(session, from_date, to_date)
    print(f"Total announcements: {len(announcements)}")

    result_announcements = [a for a in announcements if is_financial_result(a)]
    print(f"Financial result announcements: {len(result_announcements)}")

    seen = set()
    saved = 0
    our_count = 0
    symbols_to_scrape = []

    for ann in result_announcements:
        symbol = ann.get("symbol", "")
        company = ann.get("sm_name", "")
        date_str = ann.get("an_dt", "")
        text = ann.get("attchmntText", "")

        result_date = parse_announcement_date(date_str)
        if not result_date:
            continue

        quarter = extract_quarter(text)
        key = (symbol, str(result_date), quarter)
        if key in seen:
            continue
        seen.add(key)

        is_ours = symbol in our_symbols
        tag = "★" if is_ours else " "
        print(f"  {tag} {symbol:20s} {result_date} {quarter[:30]:30s} {company[:40]}")

        upsert_announcement(conn, symbol, company, result_date, quarter)
        saved += 1

        if is_ours:
            our_count += 1
            if symbol not in [s for s, _ in symbols_to_scrape]:
                symbols_to_scrape.append((symbol, company))

    print(f"\nSaved {saved} announcements ({our_count} are in our stock list)")

    if args.scrape_financials and symbols_to_scrape:
        print(f"\nScraping financial details for {len(symbols_to_scrape)} symbols...")
        from scrape_financials import scrape_symbol, upsert_financials
        fin_session = requests.Session()
        fin_session.headers.update(HEADERS)

        for sym, company in symbols_to_scrape:
            tradingsymbol = f"NSE:{sym}-EQ"
            print(f"  Scraping {sym}...", end=" ", flush=True)
            try:
                data = scrape_symbol(fin_session, sym)
                if data:
                    count = upsert_financials(conn, tradingsymbol, sym, data)
                    print(f"OK ({count} rows)")
                else:
                    print("SKIP (no data)")
            except Exception as e:
                conn.rollback()
                print(f"ERROR: {e}")
            time.sleep(1.0)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
