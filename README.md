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
| Error handling & retry logic | ✅ Done |
| Logging (structured) | ✅ Done |
| Automation / scheduling | ✅ Done |
| Power BI dashboard | ⬜ Not started |
| Docker containerization | ⬜ Not started |

---

## Architecture

```
CoinGecko API
      │
      ▼
Python fetch script (requests) — orchestrated by Prefect (@flow / @task)
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
- **Orchestration with Prefect** — the pipeline is wrapped in a `@flow`/`@task` structure and scheduled via `flow.serve()` against a dedicated Prefect server, giving a UI for run history, manual triggering, and a foundation for future alerting. Tasks that receive a live database connection are marked `cache_policy=NO_CACHE`, since Prefect cannot (and should not) cache results involving stateful resources like open connections.
- **Retry logic built manually, not via decorator** — retry is implemented using `tenacity.Retrying(...)` constructed fresh inside the task rather than the `@retry` decorator, because Prefect serializes flow/task code to run it in a subprocess, and a decorator-attached retry object holds unpicklable internal state (thread locks). Building the retryer at call time avoids this.
- **Explicit logger configuration instead of `logging.basicConfig()`** — Prefect configures its own root logger early, which causes `basicConfig()` to silently no-op. The pipeline's logger is configured directly with its own handlers instead.

---

## Tech Stack

- **Python** — `requests`, `pandas`, `SQLAlchemy`, `pyodbc`, `python-dotenv`, `tenacity`, `prefect`
- **SQL Server** — data storage
- **Prefect** — workflow orchestration and scheduling
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

4. Run the pipeline (requires two terminals — Prefect needs a dedicated server running for scheduling to work, not just the ephemeral one it spins up automatically):

   **Terminal 1 — start the Prefect server:**
   ```
   prefect server start
   ```
   Leave this running. The UI is available at `http://127.0.0.1:4200`.

   **Terminal 2 — run the pipeline:**
   ```
   cd src
   $env:PREFECT_API_URL = "http://127.0.0.1:4200/api"
   python fetch_data.py
   ```
   This registers the deployment and serves the flow, polling for its daily 08:00 cron schedule. You can also trigger a run immediately from the Prefect UI ("Run" button on the deployment page) instead of waiting for the schedule.

---

## What This Project Demonstrates

- Consuming a REST API and handling JSON responses in Python
- Designing a dimension/fact data model instead of a single flat table
- Parameterized SQL queries (SQL injection prevention)
- Pipeline observability through run logging
- Environment-based configuration (secrets kept out of source control)
- Retry logic with exponential backoff for transient failures (`tenacity`), distinguishing retryable errors (network issues, 5xx) from permanent ones (4xx) that shouldn't be retried
- Structured logging to file and console with UTF-8 support
- Workflow orchestration with Prefect (`@flow`/`@task`), including scheduled and manually-triggered runs via a self-hosted server and UI
- Debugging cross-process serialization issues (unpicklable objects like open DB connections and stateful retry handlers) that arise when an orchestrator runs code in a subprocess

## Planned Next Steps

- Build a Power BI dashboard on top of the fact table
- Containerize the pipeline with Docker

---

## Notes

This project intentionally uses a simplified, local setup (local SQL Server, scheduled task instead of Airflow) appropriate for a portfolio project. In a production environment, this would typically run on a cloud data warehouse (e.g. Snowflake, BigQuery, Azure Synapse) with orchestration via Airflow and secrets managed through a dedicated secrets manager.
