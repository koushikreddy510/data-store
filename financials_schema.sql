-- Separate table for stock financial results (quarterly/annual)
-- Scraped from screener.in — NOT merged with existing OHLCV or symbols tables

CREATE TABLE IF NOT EXISTS stock_financials (
    id SERIAL PRIMARY KEY,
    tradingsymbol TEXT NOT NULL,
    nse_symbol TEXT NOT NULL,
    company_name TEXT DEFAULT '',
    result_type TEXT NOT NULL CHECK (result_type IN ('quarterly', 'annual')),
    period TEXT NOT NULL,            -- e.g. 'Mar 2025', 'Jun 2024', 'FY2025'
    period_end_date DATE,

    -- Income Statement
    revenue DOUBLE PRECISION,        -- Total Revenue / Sales
    expenses DOUBLE PRECISION,       -- Total Expenses
    operating_profit DOUBLE PRECISION,
    opm_pct DOUBLE PRECISION,        -- Operating Profit Margin %
    other_income DOUBLE PRECISION,
    interest DOUBLE PRECISION,       -- Interest / Finance Cost
    depreciation DOUBLE PRECISION,
    profit_before_tax DOUBLE PRECISION,
    tax DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    npm_pct DOUBLE PRECISION,        -- Net Profit Margin %
    eps DOUBLE PRECISION,            -- Earnings Per Share

    -- Valuation Ratios (latest snapshot)
    pe_ratio DOUBLE PRECISION,
    pb_ratio DOUBLE PRECISION,
    market_cap_cr DOUBLE PRECISION,
    dividend_yield DOUBLE PRECISION,
    roce_pct DOUBLE PRECISION,       -- Return on Capital Employed
    roe_pct DOUBLE PRECISION,        -- Return on Equity

    -- Growth (YoY)
    revenue_growth_pct DOUBLE PRECISION,
    profit_growth_pct DOUBLE PRECISION,

    source TEXT DEFAULT 'screener.in',
    scraped_at TIMESTAMP DEFAULT now(),

    UNIQUE(tradingsymbol, result_type, period)
);

CREATE INDEX IF NOT EXISTS idx_financials_symbol ON stock_financials(tradingsymbol);
CREATE INDEX IF NOT EXISTS idx_financials_nse ON stock_financials(nse_symbol);
CREATE INDEX IF NOT EXISTS idx_financials_period ON stock_financials(period_end_date);
CREATE INDEX IF NOT EXISTS idx_financials_type ON stock_financials(result_type);
