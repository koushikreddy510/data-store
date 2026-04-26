"""
Scrapes quarterly and annual financial results from screener.in
and stores them in the stock_financials table.

Usage:
  python scrape_financials.py                  # scrape all symbols
  python scrape_financials.py --limit 50       # scrape first 50
  python scrape_financials.py --symbol RELIANCE # scrape one symbol

Separate from OHLCV data — uses its own table.
"""

import re
import time
import argparse
import os
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import psycopg

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market",
    "user": "market_user",
    "password": "market_pass",
}


def _get_conn():
    try:
        from db_config import get_conn
        return get_conn()
    except ImportError:
        return psycopg.connect(**DB_CONFIG)


SCREENER_BASE = "https://www.screener.in/company"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in/",
    "sec-ch-ua": '"Brave";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

SLEEP_BETWEEN = 1.0


def parse_number(text: str):
    """Parse a number from screener.in table cell, handling commas and %."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("%", "")
    if text in ("", "-", "—", "N/A"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_period_date(period_str: str):
    """Convert 'Mar 2025' or 'Jun 2024' to a date."""
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    parts = period_str.strip().split()
    if len(parts) == 2:
        month_str, year_str = parts
        month = month_map.get(month_str)
        if month and year_str.isdigit():
            import calendar
            year = int(year_str)
            last_day = calendar.monthrange(year, month)[1]
            return datetime(year, month, last_day).date()
    return None


def extract_table_data(soup, section_id):
    """Extract quarterly or annual results table from screener.in HTML."""
    section = soup.find("section", id=section_id)
    if not section:
        return []

    table = section.find("table")
    if not table:
        return []

    headers = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            text = th.get_text(strip=True)
            if text:
                headers.append(text)

    if len(headers) < 2:
        return []

    periods = headers[1:]
    rows_data = {}

    tbody = table.find("tbody")
    if not tbody:
        return []

    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row_label = cells[0].get_text(strip=True)
        values = [parse_number(c.get_text(strip=True)) for c in cells[1:]]
        rows_data[row_label] = values

    results = []
    for i, period in enumerate(periods):
        entry = {"period": period}
        for label, values in rows_data.items():
            if i < len(values):
                entry[label] = values[i]
        results.append(entry)

    return results


def map_row_labels(entry: dict) -> dict:
    """Map screener.in row labels to our DB columns.
    Handles labels with '+' suffix (e.g. 'Sales+', 'Net Profit+')."""
    label_map = {
        "sales": "revenue",
        "revenue": "revenue",
        "total revenue": "revenue",
        "expenses": "expenses",
        "total expenses": "expenses",
        "operating profit": "operating_profit",
        "opm %": "opm_pct",
        "opm": "opm_pct",
        "other income": "other_income",
        "interest": "interest",
        "finance cost": "interest",
        "depreciation": "depreciation",
        "profit before tax": "profit_before_tax",
        "pbt": "profit_before_tax",
        "tax %": "tax",
        "tax": "tax",
        "net profit": "net_profit",
        "pat": "net_profit",
        "eps in rs": "eps",
        "eps": "eps",
        "eps (rs)": "eps",
    }

    mapped = {"period": entry.get("period", "")}
    for label, value in entry.items():
        if label == "period":
            continue
        # Strip trailing '+' and whitespace, then lowercase for matching
        clean_label = label.rstrip("+").strip().lower()
        db_col = label_map.get(clean_label)
        if db_col and value is not None:
            mapped[db_col] = value

    return mapped


def extract_ratios(soup) -> dict:
    """Extract PE, PB, Market Cap, ROCE, ROE, Dividend Yield from the top section."""
    ratios = {}
    ratio_list = soup.find("ul", id="top-ratios")
    if not ratio_list:
        ratio_list = soup.find("div", class_="company-ratios")

    if ratio_list:
        for li in ratio_list.find_all("li"):
            name_el = li.find("span", class_="name")
            val_el = li.find("span", class_="number") or li.find("span", class_="value")
            if name_el and val_el:
                name = name_el.get_text(strip=True)
                val = parse_number(val_el.get_text(strip=True))
                if "Market Cap" in name:
                    ratios["market_cap_cr"] = val
                elif "P/E" in name or "Stock P/E" in name:
                    ratios["pe_ratio"] = val
                elif "P/B" in name or "Book Value" in name:
                    ratios["pb_ratio"] = val
                elif "Dividend" in name:
                    ratios["dividend_yield"] = val
                elif "ROCE" in name:
                    ratios["roce_pct"] = val
                elif "ROE" in name:
                    ratios["roe_pct"] = val

    # Also try the top-ratios table format
    for span in soup.find_all("span", class_="name"):
        text = span.get_text(strip=True)
        sibling = span.find_next_sibling("span")
        if not sibling:
            parent = span.parent
            if parent:
                sibling = parent.find("span", class_="number") or parent.find("span", class_="nowrap")
        if sibling:
            val = parse_number(sibling.get_text(strip=True))
            if val is not None:
                if "Market Cap" in text and "market_cap_cr" not in ratios:
                    ratios["market_cap_cr"] = val
                elif "Stock P/E" in text and "pe_ratio" not in ratios:
                    ratios["pe_ratio"] = val
                elif "Book Value" in text and "pb_ratio" not in ratios:
                    # This is book value, not P/B — we'll compute P/B if needed
                    pass
                elif "Dividend Yield" in text and "dividend_yield" not in ratios:
                    ratios["dividend_yield"] = val
                elif "ROCE" in text and "roce_pct" not in ratios:
                    ratios["roce_pct"] = val
                elif "ROE" in text and "roe_pct" not in ratios:
                    ratios["roe_pct"] = val

    return ratios


def scrape_symbol(session: requests.Session, nse_symbol: str) -> dict:
    """Scrape financial data for a single symbol from screener.in."""
    url = f"{SCREENER_BASE}/{nse_symbol}/consolidated/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 429:
            return {"rate_limited": True, "status_code": 429, "quarterly": [], "annual": []}
        if resp.status_code == 404:
            url = f"{SCREENER_BASE}/{nse_symbol}/"
            resp = session.get(url, timeout=15)
        if resp.status_code == 429:
            return {"rate_limited": True, "status_code": 429, "quarterly": [], "annual": []}
        if resp.status_code != 200:
            return None
    except Exception as e:
        print(f"  Network error: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    company_name = ""
    h1 = soup.find("h1")
    if h1:
        company_name = h1.get_text(strip=True)

    quarterly = extract_table_data(soup, "quarters")
    annual = extract_table_data(soup, "profit-loss")
    lower = resp.text[:5000].lower()
    page_title = soup.find("title").get_text(" ", strip=True).lower() if soup.find("title") else ""
    if (
        not quarterly
        and not annual
        and not company_name
        and ("login" in page_title or "register" in page_title or "create account" in lower)
    ):
        return {"login_required": True, "quarterly": [], "annual": []}

    ratios = extract_ratios(soup)

    return {
        "company_name": company_name,
        "quarterly": [map_row_labels(e) for e in quarterly],
        "annual": [map_row_labels(e) for e in annual],
        "ratios": ratios,
    }


def upsert_financials(conn, tradingsymbol: str, nse_symbol: str, data: dict):
    """Insert or update financial results into the DB."""
    company_name = data.get("company_name", "")
    ratios = data.get("ratios", {})

    sql = """
    INSERT INTO stock_financials (
        tradingsymbol, nse_symbol, company_name, result_type, period, period_end_date,
        revenue, expenses, operating_profit, opm_pct, other_income, interest,
        depreciation, profit_before_tax, tax, net_profit, npm_pct, eps,
        pe_ratio, pb_ratio, market_cap_cr, dividend_yield, roce_pct, roe_pct,
        revenue_growth_pct, profit_growth_pct, source, scraped_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, now()
    )
    ON CONFLICT (tradingsymbol, result_type, period)
    DO UPDATE SET
        revenue = EXCLUDED.revenue,
        expenses = EXCLUDED.expenses,
        operating_profit = EXCLUDED.operating_profit,
        opm_pct = EXCLUDED.opm_pct,
        other_income = EXCLUDED.other_income,
        interest = EXCLUDED.interest,
        depreciation = EXCLUDED.depreciation,
        profit_before_tax = EXCLUDED.profit_before_tax,
        tax = EXCLUDED.tax,
        net_profit = EXCLUDED.net_profit,
        npm_pct = EXCLUDED.npm_pct,
        eps = EXCLUDED.eps,
        pe_ratio = EXCLUDED.pe_ratio,
        pb_ratio = EXCLUDED.pb_ratio,
        market_cap_cr = EXCLUDED.market_cap_cr,
        dividend_yield = EXCLUDED.dividend_yield,
        roce_pct = EXCLUDED.roce_pct,
        roe_pct = EXCLUDED.roe_pct,
        revenue_growth_pct = EXCLUDED.revenue_growth_pct,
        profit_growth_pct = EXCLUDED.profit_growth_pct,
        source = EXCLUDED.source,
        scraped_at = now();
    """

    count = 0
    for result_type, entries in [("quarterly", data.get("quarterly", [])), ("annual", data.get("annual", []))]:
        prev_revenue = None
        prev_profit = None

        for entry in reversed(entries):
            period = entry.get("period", "")
            if not period:
                continue

            period_date = parse_period_date(period)
            revenue = entry.get("revenue")
            net_profit = entry.get("net_profit")
            npm = None
            if revenue and net_profit and revenue != 0:
                npm = round((net_profit / revenue) * 100, 2)

            rev_growth = None
            profit_growth = None
            if prev_revenue and revenue and prev_revenue != 0:
                rev_growth = round(((revenue - prev_revenue) / abs(prev_revenue)) * 100, 2)
            if prev_profit and net_profit and prev_profit != 0:
                profit_growth = round(((net_profit - prev_profit) / abs(prev_profit)) * 100, 2)

            with conn.cursor() as cur:
                cur.execute(sql, (
                    tradingsymbol, nse_symbol, company_name, result_type, period, period_date,
                    revenue, entry.get("expenses"), entry.get("operating_profit"),
                    entry.get("opm_pct"), entry.get("other_income"), entry.get("interest"),
                    entry.get("depreciation"), entry.get("profit_before_tax"),
                    entry.get("tax"), net_profit, npm, entry.get("eps"),
                    ratios.get("pe_ratio"), ratios.get("pb_ratio"),
                    ratios.get("market_cap_cr"), ratios.get("dividend_yield"),
                    ratios.get("roce_pct"), ratios.get("roe_pct"),
                    rev_growth, profit_growth, "screener.in",
                ))
            count += 1
            prev_revenue = revenue
            prev_profit = net_profit

    conn.commit()
    return count


def extract_nse_symbol(tradingsymbol: str) -> str:
    """NSE:SBIN-EQ -> SBIN"""
    sym = tradingsymbol
    if ":" in sym:
        sym = sym.split(":")[1]
    if sym.endswith("-EQ"):
        sym = sym[:-3]
    return sym


def main():
    parser = argparse.ArgumentParser(description="Scrape financials from screener.in")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of symbols (0 = all)")
    parser.add_argument("--symbol", type=str, default="", help="Scrape a single NSE symbol")
    parser.add_argument("--sleep", type=float, default=SLEEP_BETWEEN, help="Sleep between requests")
    args = parser.parse_args()

    conn = _get_conn()
    conn.autocommit = False

    if args.symbol:
        symbols = [(f"NSE:{args.symbol}-EQ", None)]
    else:
        with conn.cursor() as cur:
            cur.execute("SELECT tradingsymbol, isin FROM symbols ORDER BY tradingsymbol;")
            symbols = cur.fetchall()

    if args.limit > 0:
        symbols = symbols[:args.limit]

    print(f"Scraping financials for {len(symbols)} symbols from screener.in")

    session = requests.Session()
    session.trust_env = False
    session.headers.update(HEADERS)
    cookie_str = os.getenv("SCREENER_SESSION_COOKIE", "").strip()
    if cookie_str:
        session.headers["Cookie"] = cookie_str

    success = 0
    failed = 0
    skipped = 0

    for i, row in enumerate(symbols, start=1):
        tradingsymbol = row[0]
        nse_sym = extract_nse_symbol(tradingsymbol)
        print(f"[{i}/{len(symbols)}] {nse_sym} ...", end=" ", flush=True)

        try:
            data = scrape_symbol(session, nse_sym)
            if not data:
                print("SKIP (no data)")
                skipped += 1
                time.sleep(args.sleep)
                continue

            q_count = len(data.get("quarterly", []))
            a_count = len(data.get("annual", []))

            if q_count == 0 and a_count == 0:
                print("SKIP (no results tables)")
                skipped += 1
                time.sleep(args.sleep)
                continue

            count = upsert_financials(conn, tradingsymbol, nse_sym, data)
            ratios = data.get("ratios", {})
            pe = ratios.get("pe_ratio", "-")
            mcap = ratios.get("market_cap_cr", "-")
            print(f"OK ({count} rows, Q:{q_count} A:{a_count}, PE:{pe}, MCap:{mcap})")
            success += 1

        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            failed += 1

        time.sleep(args.sleep)

        if i % 50 == 0:
            print(f"  --- Progress: {success} OK, {failed} ERR, {skipped} SKIP ---")

    conn.close()
    print(f"\nDone. Success: {success}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
