# ingest/batch_tiingo_tickers.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
# Small reference dataset (one row per watchlist symbol) — full refresh each run.
from databricks.sdk.runtime import dbutils, spark
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

TIINGO_TOKEN = dbutils.secrets.get("beacon", "tiingo_token")
TABLE = "beacon.bronze.tickers_raw"

WATCHLIST = {
    "SPY": "ETF",
    "QQQ": "ETF",
    "DIA": "ETF",
    "IWM": "ETF",
    "XLF": "ETF",
    "XLE": "ETF",
    "XLK": "ETF",
    "XLU": "ETF",
    "TLT": "ETF",
}

HEADERS = {"Content-Type": "application/json", "Authorization": f"Token {TIINGO_TOKEN}"}


def fetch(symbol: str, asset_type: str):
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    o = r.json()
    return (
        o["ticker"],
        o.get("name"),
        asset_type,
        o.get("exchangeCode"),
        o.get("startDate", "")[:10] or None,
        o.get("endDate", "")[:10] or None,
        o.get("description"),
    )


def run():
    rows = [fetch(s, t) for s, t in WATCHLIST.items()]
    schema = StructType(
        [
            StructField("symbol", StringType()),
            StructField("name", StringType()),
            StructField("asset_type", StringType()),
            StructField("exchange_code", StringType()),
            StructField("start_date", StringType()),
            StructField("end_date", StringType()),
            StructField("description", StringType()),
        ]
    )
    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE)
    print(f"Loaded {df.count()} tickers.")


run()
