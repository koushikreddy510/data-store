"""
Fetch quarterly and annual financial statements for all NSE symbols via yfinance
and upsert into the stock_financials table.

yfinance works from cloud IPs (no auth required) and provides 4 quarters + 4 years
of income-statement data for NSE tickers as <SYM>.NS.

Usage:
  python yfinance_financials_job.py [--limit N] [--workers N] [--sleep S]

Env:
  DATABASE_URL - postgres DSN (falls back to PG* vars via db_config.py)
"""
import argparse
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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
_print_lock = threading.Lock()


def _safe_print(*args):
    with _print_lock:
        print(*args, flush=True)


def _to_yf_ticker(tradingsymbol: str) -> str | None:
    if tradingsymbol.startswith("NSE:"):
        base = tradingsymbol[4:]
        for suffix in ("-EQ", "-BE", "-SM", "-BL", "-IL", "-N1", "-N2", "-N3", "-N4"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        return f"{base}.NS"
    return None


def _row(series: pd.Series, *keys) -> float | None:
    for k in keys:
        if k in series.index:
            v = series[k]
            if v is not None and not pd.isna(v):
                return float(v)
    return None


UPSERT_SQL = """
INSERT INTO stock_financials (
    tradingsymbol, nse_symbol, company_name, result_type, period, period_end_date,
    revenue, expenses, operating_profit, opm_pct, interest, depreciation,
    net_profit, npm_pct, eps,
    pe_ratio, pb_ratio, market_cap_cr, dividend_yield, roe_pct,
    source, scraped_at
)
VALUES (
    %(tradingsymbol)s, %(nse_symbol)s, %(company_name)s, %(result_type)s,
    %(period)s, %(period_end_date)s,
    %(revenue)s, %(expenses)s, %(operating_profit)s, %(opm_pct)s,
    %(interest)s, %(depreciation)s, %(net_profit)s, %(npm_pct)s, %(eps)s,
    %(pe_ratio)s, %(pb_ratio)s, %(market_cap_cr)s, %(dividend_yield)s, %(roe_pct)s,
    'yfinance', now()
)
ON CONFLICT (tradingsymbol, result_type, period)
DO UPDATE SET
    revenue         = EXCLUDED.revenue,
    expenses        = EXCLUDED.expenses,
    operating_profit= EXCLUDED.operating_profit,
    opm_pct         = EXCLUDED.opm_pct,
    interest        = EXCLUDED.interest,
    depreciation    = EXCLUDED.depreciation,
    net_profit      = EXCLUDED.net_profit,
    npm_pct         = EXCLUDED.npm_pct,
    eps             = EXCLUDED.eps,
    pe_ratio        = EXCLUDED.pe_ratio,
    pb_ratio        = EXCLUDED.pb_ratio,
    market_cap_cr   = EXCLUDED.market_cap_cr,
    dividend_yield  = EXCLUDED.dividend_yield,
    roe_pct         = EXCLUDED.roe_pct,
    source          = 'yfinance',
    scraped_at      = now();
"""


def _extract_rows(tradingsymbol: str, yf_sym: str, stmt: pd.DataFrame, result_type: str, info: dict) -> list[dict]:
    rows = []
    if stmt is None or stmt.empty:
        return rows

    pe  = info.get("trailingPE")
    pb  = info.get("priceToBook")
    mcap_raw = info.get("marketCap")
    mcap_cr  = float(mcap_raw) / 1e7 if mcap_raw else None   # INR → Crores
    div_yield = info.get("dividendYield")
    roe       = info.get("returnOnEquity")
    name      = info.get("longName") or info.get("shortName") or ""

    for col in stmt.columns:
        s = stmt[col]
        period_dt = col.date() if hasattr(col, "date") else col
        if result_type == "quarterly":
            period_label = period_dt.strftime("%b %Y")   # "Jun 2024"
        else:
            period_label = f"FY{period_dt.year}"

        def _cr(v): return round(v / 1e7, 2) if v is not None else None  # INR → Crores

        revenue     = _cr(_row(s, "Total Revenue"))
        op_income   = _cr(_row(s, "Operating Income", "EBIT"))
        net_income  = _cr(_row(s, "Net Income", "Net Income Common Stockholders"))
        expenses    = _cr(_row(s, "Total Expenses"))
        interest    = _cr(_row(s, "Interest Expense", "Interest Expense Non Operating"))
        depreciation= _cr(_row(s, "Reconciled Depreciation"))
        eps         = _row(s, "Diluted EPS", "Basic EPS")  # per-share, keep as INR

        opm_pct = (op_income / revenue * 100) if revenue and op_income else None
        npm_pct = (net_income / revenue * 100) if revenue and net_income else None

        # interest is stored as negative in yfinance for expense
        if interest and interest < 0:
            interest = abs(interest)

        rows.append({
            "tradingsymbol":   tradingsymbol,
            "nse_symbol":      yf_sym.replace(".NS", ""),
            "company_name":    name,
            "result_type":     result_type,
            "period":          period_label,
            "period_end_date": str(period_dt),
            "revenue":         revenue,
            "expenses":        expenses,
            "operating_profit":op_income,
            "opm_pct":         opm_pct,
            "interest":        interest,
            "depreciation":    depreciation,
            "net_profit":      net_income,
            "npm_pct":         npm_pct,
            "eps":             eps,
            "pe_ratio":        float(pe) if pe and not pd.isna(pe) else None,
            "pb_ratio":        float(pb) if pb and not pd.isna(pb) else None,
            "market_cap_cr":   mcap_cr,
            "dividend_yield":  float(div_yield * 100) if div_yield and not pd.isna(div_yield) else None,
            "roe_pct":         float(roe * 100) if roe and not pd.isna(roe) else None,
        })
    return rows


def _process_one(tradingsymbol: str, sleep: float) -> tuple[str, int, str]:
    yf_sym = _to_yf_ticker(tradingsymbol)
    if not yf_sym:
        return tradingsymbol, 0, "skip"

    try:
        t = yf.Ticker(yf_sym)
        info = {}
        try:
            info = t.info or {}
        except Exception:
            pass

        rows = []
        for stmt, rtype in [
            (getattr(t, "quarterly_income_stmt", None), "quarterly"),
            (getattr(t, "income_stmt", None), "annual"),
        ]:
            rows.extend(_extract_rows(tradingsymbol, yf_sym, stmt, rtype, info))

        if not rows:
            return tradingsymbol, 0, "no_data"

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(UPSERT_SQL, row)
            conn.commit()
        finally:
            conn.close()

        if sleep > 0:
            time.sleep(sleep)
        return tradingsymbol, len(rows), "ok"

    except Exception as e:
        return tradingsymbol, 0, f"error:{e}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",   type=int,   default=0,   help="Max symbols (0=all)")
    ap.add_argument("--workers", type=int,   default=3,   help="Parallel workers")
    ap.add_argument("--sleep",   type=float, default=0.5, help="Sleep between requests per worker")
    args = ap.parse_args()

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT tradingsymbol FROM symbols WHERE tradingsymbol LIKE 'NSE:%' ORDER BY tradingsymbol")
        symbols = [r[0] for r in cur.fetchall()]
    conn.close()

    if args.limit:
        symbols = symbols[: args.limit]

    log.info("Processing %d symbols with %d workers", len(symbols), args.workers)
    total_rows = ok = no_data = errors = 0
    done = 0
    start = datetime.now()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_process_one, sym, args.sleep): sym for sym in symbols}
        for future in as_completed(futures):
            sym, count, status = future.result()
            done += 1
            total_rows += count
            if status == "ok":
                ok += 1
            elif status == "no_data":
                no_data += 1
            elif status.startswith("error"):
                errors += 1
            if done % 50 == 0 or done == len(symbols):
                elapsed = (datetime.now() - start).seconds
                _safe_print(f"[{done}/{len(symbols)}] rows={total_rows} ok={ok} no_data={no_data} errors={errors} elapsed={elapsed}s")

    print(f"\nDone. ok={ok} no_data={no_data} errors={errors} total_rows={total_rows}")


if __name__ == "__main__":
    main()
