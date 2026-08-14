import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from datetime import datetime
from dotenv import load_dotenv
import logging
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception_type
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from pathlib import Path

load_dotenv()

LOG_FILE = Path(__file__).parent / "pipeline.log"

logger = logging.getLogger("crypto_pipeline")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.propagate = False  # sprečava duplo ispisivanje kroz Prefect-ov root logger

API_KEY = os.getenv("COINGECKO_API_KEY")
CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING")
API_URL = "https://api.coingecko.com/api/v3/coins/markets"

def get_engine():
    """Pravi nov Engine objekat. Namerno lenja inicijalizacija - 
    izbegava probleme sa serijalizacijom kad Prefect pokreće flow u novom procesu."""
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": CONNECTION_STRING})
    return create_engine(connection_url)

class ServerError(Exception):
    """Baca se za 5xx server greske - one se ponavljaju, za razliku od 4xx gresaka."""
    pass

@task(name="start-pipeline-run", cache_policy=NO_CACHE)
def start_pipeline_run(conn):
    """Upisuje novi red u PipelineRunLog i vraća BatchId za praćenje."""
    result = conn.execute(
        text("INSERT INTO PipelineRunLog (Status) OUTPUT INSERTED.BatchId VALUES ('Running')")
    )
    batch_id = result.scalar()
    conn.commit()
    return batch_id

@task(name="finish-pipeline-run", cache_policy=NO_CACHE)
def finish_pipeline_run(conn, batch_id, status, rows_inserted=None, error_message=None):
    """Ažurira log na kraju - uspeh ili neuspeh."""
    conn.execute(
        text("""
            UPDATE PipelineRunLog 
            SET FinishedAt = :finished_at, Status = :status, 
                RowsInserted = :rows, ErrorMessage = :error
            WHERE BatchId = :batch_id
        """),
        {
            "finished_at": datetime.now(),
            "status": status,
            "rows": rows_inserted,
            "error": error_message,
            "batch_id": batch_id
        }
    )
    conn.commit()

@task(name="fetch-market-data", retries=0)
def fetch_market_data():
    """Poziva CoinGecko API i vraća listu coin-ova. Retry logika se pravi 'sveža' 
    pri svakom pozivu (ne kao trajni dekorator) - izbegava probleme sa serijalizacijom 
    kad Prefect šalje task u novi proces."""
    headers = {"x-cg-demo-api-key": API_KEY}
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}

    def _do_request():
        logger.info("Pozivam CoinGecko API...")
        response = requests.get(API_URL, headers=headers, params=params, timeout=10)
        if 500 <= response.status_code < 600:
            raise ServerError(f"Server greška {response.status_code}")
        response.raise_for_status()
        return response.json()

    retryer = Retrying(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, ServerError)),
        reraise=True
    )
    result = retryer(_do_request)
    logger.info(f"API odgovorio uspešno, dobijeno {len(result)} coin-ova.")
    return result

@task(name="upsert-dim-coin", cache_policy=NO_CACHE)
def upsert_dim_coin(conn, coins):
    """Ubacuje nove coin-ove u DimCoin, ignoriše one koji već postoje."""
    for coin in coins:
        conn.execute(
            text("""
                IF NOT EXISTS (SELECT 1 FROM DimCoin WHERE CoinId = :coin_id)
                INSERT INTO DimCoin (CoinId, Symbol, Name, ImageUrl)
                VALUES (:coin_id, :symbol, :name, :image)
            """),
            {
                "coin_id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "image": coin["image"]
            }
        )
    conn.commit()

@task(name="insert-fact-data", cache_policy=NO_CACHE)
def insert_fact_data(conn, coins, batch_id):
    """Upisuje metrike u FactCoinMarketData, vraća broj upisanih redova."""
    for coin in coins:
        conn.execute(
            text("""
                INSERT INTO FactCoinMarketData 
                (CoinId, CurrentPrice, MarketCap, MarketCapRank, TotalVolume,
                 High24h, Low24h, PriceChange24h, PriceChangePercentage24h,
                 CirculatingSupply, Ath, AthDate, Atl, AtlDate, ApiLastUpdated, BatchId)
                VALUES 
                (:coin_id, :price, :mcap, :rank, :volume,
                 :high, :low, :change, :change_pct,
                 :supply, :ath, :ath_date, :atl, :atl_date, :updated, :batch_id)
            """),
            {
                "coin_id": coin["id"],
                "price": coin["current_price"],
                "mcap": coin["market_cap"],
                "rank": coin["market_cap_rank"],
                "volume": coin["total_volume"],
                "high": coin["high_24h"],
                "low": coin["low_24h"],
                "change": coin["price_change_24h"],
                "change_pct": coin["price_change_percentage_24h"],
                "supply": coin["circulating_supply"],
                "ath": coin["ath"],
                "ath_date": coin["ath_date"],
                "atl": coin["atl"],
                "atl_date": coin["atl_date"],
                "updated": coin["last_updated"],
                "batch_id": batch_id
            }
        )
    conn.commit()
    return len(coins)

@flow(name="crypto-data-pipeline", log_prints=True)
def run_pipeline():
    """Glavna orkestracija - poziva sve funkcije redom, hvata greške."""
    engine = get_engine()
    with engine.connect() as conn:
        batch_id = start_pipeline_run(conn)
        logger.info(f"Pipeline pokrenut. Batch ID: {batch_id}")
        try:
            coins = fetch_market_data()
            upsert_dim_coin(conn, coins)
            rows = insert_fact_data(conn, coins, batch_id)
            finish_pipeline_run(conn, batch_id, "Success", rows_inserted=rows)
            logger.info(f"Pipeline uspešan. Batch {batch_id}, upisano {rows} redova.")
        except Exception as e:
            finish_pipeline_run(conn, batch_id, "Failed", error_message=str(e))
            logger.error(f"Pipeline pao. Batch {batch_id}, greška: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    run_pipeline.serve(
        name="daily-crypto-fetch",
        cron="0 8 * * *",  # svaki dan u 8:00 ujutru
    )