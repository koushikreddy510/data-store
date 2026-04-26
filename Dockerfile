FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir beautifulsoup4 fastapi uvicorn[standard]
COPY . .

# Scripts use DB_CONFIG; override via DATABASE_URL or PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE
# For DATABASE_URL, scripts need to be updated to use it. See deploy README.
ENV PGDATABASE=market
EXPOSE 8003

CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "8003"]
