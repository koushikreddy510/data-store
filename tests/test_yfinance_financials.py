"""
Tests for yfinance_financials_job.py

Key invariant: yfinance returns monetary values in absolute INR (e.g. 1,41,70,89,00,000).
Every monetary column (revenue, expenses, operating_profit, interest,
depreciation, net_profit) must be stored in Crores (divide by 1e7 = 10,000,000).
EPS is per-share INR and must NOT be divided.
"""
import math
import sys
import os

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yfinance_financials_job import _extract_rows, _to_yf_ticker


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_stmt(row_dict: dict) -> pd.DataFrame:
    """Build a single-column DataFrame that looks like yfinance quarterly_income_stmt."""
    import datetime
    col = datetime.date(2026, 6, 30)
    s = pd.Series(row_dict, name=col)
    return pd.DataFrame({col: s})


# ── _to_yf_ticker ─────────────────────────────────────────────────────────────

class TestToYfTicker:
    def test_eq_suffix_becomes_dot_ns(self):
        assert _to_yf_ticker("NSE:SBIN-EQ") == "SBIN.NS"

    def test_be_suffix_stripped(self):
        assert _to_yf_ticker("NSE:E2E-BE") == "E2E.NS"

    def test_sm_suffix_stripped(self):
        assert _to_yf_ticker("NSE:SOMESTOCK-SM") == "SOMESTOCK.NS"

    def test_non_nse_returns_none(self):
        assert _to_yf_ticker("BSE:500325") is None

    def test_unknown_suffix_kept(self):
        # If no known suffix matches, the whole base (with suffix) becomes the ticker
        assert _to_yf_ticker("NSE:FOO-XX") == "FOO-XX.NS"


# ── _extract_rows: unit conversion ───────────────────────────────────────────

ONE_CR = 10_000_000  # 1 crore in rupees


class TestMonetaryConversionToCrores:
    """Monetary columns must arrive in Crores, not raw rupees."""

    def _rows(self, revenue_inr, net_income_inr, eps_inr=25.0):
        stmt = _make_stmt({
            "Total Revenue": revenue_inr,
            "Net Income": net_income_inr,
            "Operating Income": revenue_inr * 0.2,  # 20% OPM
            "Total Expenses": revenue_inr * 0.8,
            "Diluted EPS": eps_inr,
        })
        info = {"marketCap": 100 * ONE_CR, "longName": "Test Co"}
        return _extract_rows("NSE:TEST-EQ", "TEST.NS", stmt, "quarterly", info)

    def test_revenue_divided_by_1e7(self):
        rows = self._rows(revenue_inr=23165 * ONE_CR, net_income_inr=6081 * ONE_CR)
        assert rows[0]["revenue"] == pytest.approx(23165, rel=1e-4)

    def test_net_profit_divided_by_1e7(self):
        rows = self._rows(revenue_inr=23165 * ONE_CR, net_income_inr=6081 * ONE_CR)
        assert rows[0]["net_profit"] == pytest.approx(6081, rel=1e-4)

    def test_operating_profit_divided_by_1e7(self):
        rows = self._rows(revenue_inr=10000 * ONE_CR, net_income_inr=1000 * ONE_CR)
        assert rows[0]["operating_profit"] == pytest.approx(2000, rel=1e-4)

    def test_eps_is_NOT_divided(self):
        """EPS is per-share in INR — must stay as-is."""
        rows = self._rows(revenue_inr=10000 * ONE_CR, net_income_inr=500 * ONE_CR, eps_inr=48.5)
        assert rows[0]["eps"] == pytest.approx(48.5, rel=1e-4)

    def test_market_cap_divided_by_1e7(self):
        stmt = _make_stmt({"Total Revenue": 1000 * ONE_CR, "Net Income": 100 * ONE_CR})
        info = {"marketCap": 50000 * ONE_CR, "longName": "Big Corp"}
        rows = _extract_rows("NSE:BIG-EQ", "BIG.NS", stmt, "quarterly", info)
        assert rows[0]["market_cap_cr"] == pytest.approx(50000, rel=1e-4)

    def test_small_company_values_still_correct(self):
        """20 Cr revenue company (like GAJA from screenshot) — sanity check."""
        rows = self._rows(revenue_inr=20 * ONE_CR, net_income_inr=4 * ONE_CR)
        assert rows[0]["revenue"] == pytest.approx(20, rel=1e-4)
        assert rows[0]["net_profit"] == pytest.approx(4, rel=1e-4)

    def test_large_company_values_still_correct(self):
        """HindustanZinc-scale: 13,747 Cr revenue."""
        rows = self._rows(revenue_inr=13747 * ONE_CR, net_income_inr=5469 * ONE_CR)
        assert rows[0]["revenue"] == pytest.approx(13747, rel=1e-4)
        assert rows[0]["net_profit"] == pytest.approx(5469, rel=1e-4)

    def test_values_not_in_raw_rupees(self):
        """Guard: ensure values are NOT the raw INR figures (would be ~1e11 for big cos)."""
        rows = self._rows(revenue_inr=23165 * ONE_CR, net_income_inr=6081 * ONE_CR)
        assert rows[0]["revenue"] < 1_000_000, "revenue looks like raw rupees, not Crores"
        assert rows[0]["net_profit"] < 1_000_000, "net_profit looks like raw rupees, not Crores"


# ── _extract_rows: OPM/NPM percentages computed correctly ────────────────────

class TestPctCalculations:
    def test_opm_pct_computed_from_already_converted_values(self):
        stmt = _make_stmt({
            "Total Revenue": 10000 * ONE_CR,
            "Operating Income": 1500 * ONE_CR,  # 15% OPM
            "Net Income": 800 * ONE_CR,
        })
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["opm_pct"] == pytest.approx(15.0, rel=1e-3)

    def test_npm_pct_computed_from_already_converted_values(self):
        stmt = _make_stmt({
            "Total Revenue": 10000 * ONE_CR,
            "Net Income": 800 * ONE_CR,  # 8% NPM
        })
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["npm_pct"] == pytest.approx(8.0, rel=1e-3)


# ── _extract_rows: missing / None values ─────────────────────────────────────

class TestMissingValues:
    def test_missing_revenue_yields_none(self):
        stmt = _make_stmt({"Net Income": 500 * ONE_CR})
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["revenue"] is None

    def test_missing_net_income_yields_none(self):
        stmt = _make_stmt({"Total Revenue": 1000 * ONE_CR})
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["net_profit"] is None

    def test_opm_none_when_revenue_missing(self):
        stmt = _make_stmt({"Operating Income": 200 * ONE_CR})
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["opm_pct"] is None

    def test_empty_stmt_returns_no_rows(self):
        rows = _extract_rows("NSE:X-EQ", "X.NS", pd.DataFrame(), "quarterly", {})
        assert rows == []

    def test_interest_expense_negative_becomes_positive(self):
        """yfinance often stores interest expense as a negative number."""
        stmt = _make_stmt({
            "Total Revenue": 1000 * ONE_CR,
            "Interest Expense": -50 * ONE_CR,
        })
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["interest"] == pytest.approx(50, rel=1e-4)


# ── _extract_rows: period label ───────────────────────────────────────────────

class TestPeriodLabel:
    def test_quarterly_period_formatted_as_mon_year(self):
        import datetime
        col = datetime.date(2026, 6, 30)
        stmt = pd.DataFrame({col: pd.Series({"Total Revenue": 1000 * ONE_CR})})
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "quarterly", {})
        assert rows[0]["period"] == "Jun 2026"

    def test_annual_period_formatted_as_fy(self):
        import datetime
        col = datetime.date(2026, 3, 31)
        stmt = pd.DataFrame({col: pd.Series({"Total Revenue": 1000 * ONE_CR})})
        rows = _extract_rows("NSE:X-EQ", "X.NS", stmt, "annual", {})
        assert rows[0]["period"] == "FY2026"
