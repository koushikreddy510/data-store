"""
Minimal HTTP runner for data-store scripts.
Enables scheduler to trigger jobs via HTTP instead of subprocess.
Set DATABASE_URL, FYERS token via env. Scripts should support DATABASE_URL.
"""
import os
import subprocess
import sys

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Data-Store Runner", version="1.0")
DATA_STORE_DIR = os.path.dirname(os.path.abspath(__file__))


def _run_script(name: str, *args) -> dict:
    """Run a Python script from data-store. Return {ok, returncode, stderr}."""
    script = os.path.join(DATA_STORE_DIR, name)
    if not os.path.isfile(script):
        return {"ok": False, "error": f"Script not found: {name}"}
    cmd = [sys.executable, script] + list(args)
    try:
        r = subprocess.run(cmd, cwd=DATA_STORE_DIR, capture_output=True, text=True, timeout=3600, env=os.environ)
        return {
            "ok": r.returncode == 0,
            "returncode": r.returncode,
            "stdout": (r.stdout or "")[:500],
            "stderr": (r.stderr or "")[:500],
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "Timeout (3600s)"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/health")
def health():
    return {"status": "ok", "data_store_dir": DATA_STORE_DIR}


@app.post("/run/ohlcv-stocks")
def run_ohlcv_stocks():
    return _run_script("fyers_ohlcv_1d_job.py")


@app.post("/run/commodity-ohlcv")
def run_commodity_ohlcv(timeframe: str = "ALL"):
    if timeframe.upper() == "ALL":
        return _run_script("commodity_ohlcv_job.py")
    return _run_script("commodity_ohlcv_job.py", "--timeframe", timeframe)


@app.post("/run/enrich-market-cap")
def run_enrich_market_cap():
    return _run_script("enrich_market_cap.py")


@app.post("/run/scrape-latest-results")
def run_scrape_latest_results(days: int = 1):
    return _run_script("scrape_latest_results.py", "--days", str(days))


@app.post("/run/scrape-financials")
def run_scrape_financials(limit: int = 200, sleep: float = 1.5):
    return _run_script("scrape_financials.py", "--limit", str(limit), "--sleep", str(sleep))


@app.post("/run/symbols-to-postgres")
def run_symbols():
    return _run_script("symbols_to_postgres.py")

