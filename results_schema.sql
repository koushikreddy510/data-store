CREATE TABLE IF NOT EXISTS result_announcements (
    id SERIAL PRIMARY KEY,
    nse_symbol TEXT NOT NULL,
    company_name TEXT,
    result_date DATE,
    quarter TEXT,
    source TEXT,
    scraped_at TIMESTAMP DEFAULT now(),
    UNIQUE (nse_symbol, result_date, quarter)
);
