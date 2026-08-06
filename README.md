# Crypto Data Pipeline

An end-to-end data engineering pipeline that fetches cryptocurrency market data from the CoinGecko API, stores it in SQL Server using a dimension/fact model, and prepares it for analysis and reporting.

Built as a portfolio project to demonstrate data engineering practices: API integration, data modeling, pipeline observability, and (eventually) automation and reporting.

---

## Project Status

🟡 **In Progress**

| Stage | Status |
|---|---|
| API research & testing | ✅ Done |
| Project setup (venv, structure, git) | ✅ Done |
| Database schema (dimension/fact model) | ✅ Done |
| Fetch script (API → SQL Server) | ✅ Done |
| Error handling & retry logic | ⬜ Not started |
| Logging (structured) | ⬜ Not started |
| Automation / scheduling | ⬜ Not started |
| Power BI dashboard | ⬜ Not started |
| Docker containerization | ⬜ Not started |

---

## Architecture

```
CoinGecko API
      │
      ▼
Python fetch script (requests)
      │
      ▼
SQL Server
 ├── PipelineRunLog     (tracks each pipeline run: status, rows inserted, errors)
 ├── DimCoin             (slowly-changing dimension: coin id, symbol, name)
 └── FactCoinMarketData  (append-only fact table: price, market cap, volume, etc. per run)
      │
      ▼
Power BI Dashboard (planned)
```

**Design decisions:**
- **Dimension/fact split** — coin metadata (name, symbol) rarely changes and is stored separately from market metrics (price, volume), which change on every fetch. This avoids redundant data and reflects standard data warehousing practice.
- **Append-only fact table** — each pipeline run inserts new rows rather than updating existing ones, intentionally building a historical time series for trend analysis.
- **Run logging** — every pipeline execution is logged with a status (`Running`/`Success`/`Failed`), row count, and error message if applicable, enabling traceability when something breaks.

---

## Tech Stack

- **Python** — `requests`, `pandas`, `SQLAlchemy`, `pyodbc`, `python-dotenv`
- **SQL Server** — data storage
- **Power BI** — reporting layer (planned)
- **Git/GitHub** — version control
- **Docker** — containerization (planned)

---

## Data Source

[CoinGecko API](https://www.coingecko.com/en/api) (Demo/free tier) — `coins/markets` endpoint, providing current price, market cap, volume, and 24h statistics for the top cryptocurrencies by market cap.

---

## Project Structure

```
crypto-data-pipeline/
├── src/
│   └── fetch_data.py       # Main pipeline script
├── sql/
│   ├── 01_create_tables.sql
│   └── 02_indexes.sql
├── .env                     # API key & connection string (not committed)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Setup & Usage

1. Clone the repo and create a virtual environment:
   ```
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the project root:
   ```
   COINGECKO_API_KEY=your_api_key_here
   SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=YOUR_SERVER;Database=CryptoPipelineDB;Trusted_Connection=yes;TrustServerCertificate=yes;
   ```

3. Run the SQL scripts in `sql/` against your SQL Server instance to create the schema.

4. Run the pipeline:
   ```
   python src/fetch_data.py
   ```

---

## What This Project Demonstrates

- Consuming a REST API and handling JSON responses in Python
- Designing a dimension/fact data model instead of a single flat table
- Parameterized SQL queries (SQL injection prevention)
- Pipeline observability through run logging
- Environment-based configuration (secrets kept out of source control)

## Planned Next Steps

- Add retry logic for transient API failures (`tenacity`)
- Add structured logging
- Automate scheduled runs
- Build a Power BI dashboard on top of the fact table
- Containerize the pipeline with Docker

---

## Notes

This project intentionally uses a simplified, local setup (local SQL Server, scheduled task instead of Airflow) appropriate for a portfolio project. In a production environment, this would typically run on a cloud data warehouse (e.g. Snowflake, BigQuery, Azure Synapse) with orchestration via Airflow and secrets managed through a dedicated secrets manager.