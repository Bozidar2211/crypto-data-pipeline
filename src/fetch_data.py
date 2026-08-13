import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler()  # i dalje ispisuje u konzolu, ne samo u fajl
    ]
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("COINGECKO_API_KEY")
CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING")
API_URL = "https://api.coingecko.com/api/v3/coins/markets"

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONNECTION_STRING}")

class ServerError(Exception):
    """Baca se za 5xx server greske - one se ponavljaju, za razliku od 4xx gresaka."""
    pass

def start_pipeline_run(conn):
    """Upisuje novi red u PipelineRunLog i vraća BatchId za praćenje."""
    result = conn.execute(
        text("INSERT INTO PipelineRunLog (Status) OUTPUT INSERTED.BatchId VALUES ('Running')")
    )
    batch_id = result.scalar()
    conn.commit()
    return batch_id


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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, ServerError)),
    reraise=True
)
def fetch_market_data():
    logger.info("Pozivam CoinGecko API...")
    headers = {"x-cg-demo-api-key": API_KEY}
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}
    response = requests.get(API_URL, headers=headers, params=params, timeout=10)

    if 500 <= response.status_code < 600:
        raise ServerError(f"Server greška {response.status_code}")
    response.raise_for_status()  # baca za 4xx, ali tenacity to više ne hvata jer nije u retry listi

    logger.info(f"API odgovorio uspešno, dobijeno {len(response.json())} coin-ova.")
    return response.json()


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


def run_pipeline():
    """Glavna orkestracija - poziva sve funkcije redom, hvata greške."""
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
    run_pipeline()