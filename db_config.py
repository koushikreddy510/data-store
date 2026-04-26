"""
DB config for data-store scripts. Uses DATABASE_URL if set, else env vars or defaults.
For Docker: set DATABASE_URL=postgresql://user:pass@host:5432/market
"""
import os
import psycopg


def get_conn():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg.connect(url)
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "market"),
        user=os.getenv("PGUSER", "market_user"),
        password=os.getenv("PGPASSWORD", "market_pass"),
    )
