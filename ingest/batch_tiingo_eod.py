# ingest/batch_tiingo_eod.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType,
)

TIINGO_TOKEN = dbutils.secrets.get("beacon", "tiingo_token")
TABLE = "beacon.bronze.markets_raw"

# Broad-market ETF watchlist: index + sector coverage to pair against macro indicators.
WATCHLIST = {
    "SPY": "ETF",  # S&P 500
    "QQQ": "ETF",  # Nasdaq 100
    "DIA": "ETF",  # Dow Jones Industrial Average
    "IWM": "ETF",  # Russell 2000 (small-cap)
    "XLF": "ETF",  # Financials sector
    "XLE": "ETF",  # Energy sector
    "XLK": "ETF",  # Technology sector
    "XLU": "ETF",  # Utilities sector
    "TLT": "ETF",  # 20+ Year Treasury bond
}

HEADERS = {"Content-Type": "application/json", "Authorization": f"Token {TIINGO_TOKEN}"}


def latest_dates() -> dict:
    """Return {symbol: max obs_date} already loaded, for incremental pulls."""
    if not spark.catalog.tableExists(TABLE):
        return {}
    rows = (
        spark.table(TABLE)
        .groupBy("symbol")
        .agg(F.max("obs_date").alias("mx"))
        .collect()
    )
    return {r["symbol"]: str(r["mx"]) for r in rows}


def fetch(symbol: str, start: str | None):
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params = {}
    if start:
        params["startDate"] = start
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return [
        (
            symbol,
            o["date"][:10],
            o["open"], o["high"], o["low"], o["close"], o["volume"],
            o["adjOpen"], o["adjHigh"], o["adjLow"], o["adjClose"], o["adjVolume"],
            o["divCash"], o["splitFactor"],
        )
        for o in r.json()
    ]


def run():
    have = latest_dates()
    rows = [row for s in WATCHLIST for row in fetch(s, have.get(s))]
    if not rows:
        print("No new bars.")
        return
    schema = StructType(
        [
            StructField("symbol", StringType()),
            StructField("obs_date", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", LongType()),
            StructField("adj_open", DoubleType()),
            StructField("adj_high", DoubleType()),
            StructField("adj_low", DoubleType()),
            StructField("adj_close", DoubleType()),
            StructField("adj_volume", LongType()),
            StructField("div_cash", DoubleType()),
            StructField("split_factor", DoubleType()),
        ]
    )
    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("obs_date", F.to_date("obs_date"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.createOrReplaceTempView("incoming")
    if spark.catalog.tableExists(TABLE):
        spark.sql(
            f"""
            MERGE INTO {TABLE} t
            USING incoming s
            ON t.symbol = s.symbol AND t.obs_date = s.obs_date
            WHEN NOT MATCHED THEN INSERT *
        """
        )
    else:
        df.write.format("delta").saveAsTable(TABLE)
    print(f"Loaded {df.count()} rows.")


run()
