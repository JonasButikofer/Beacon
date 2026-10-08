# ingest/batch_tiingo_eod.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
from databricks.sdk.runtime import dbutils, spark
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType,
)

TIINGO_TOKEN = dbutils.secrets.get("beacon", "tiingo_token")
TABLE = "beacon.bronze.markets_raw"
# Tiingo's /prices endpoint returns only the single latest day when startDate is
# omitted — so first-time pulls (no watermark yet) must pass an explicit far-past
# date to get full history, not just "today".
BACKFILL_START = "1990-01-01"

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


def fetch(symbol: str, start: str):
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params = {"startDate": start}
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


def fetch_symbol(symbol: str, watermark: str | None):
    """Incremental pull, widened to full history after a split or dividend.

    Tiingo recomputes adj_* for every past date when a corporate action lands,
    so an incremental pull alone would leave the stored history on the old basis.
    Only rows after the watermark count as new — the watermark date itself is
    re-fetched every run and was already checked when it first arrived.
    """
    rows = fetch(symbol, watermark or BACKFILL_START)
    if watermark and any(
        r[1] > watermark and (r[12] or r[13] != 1.0)  # div_cash, split_factor
        for r in rows
    ):
        print(f"{symbol}: corporate action since {watermark}; re-pulling full history.")
        rows = fetch(symbol, BACKFILL_START)
    return rows


def run():
    have = latest_dates()
    rows = [row for s in WATCHLIST for row in fetch_symbol(s, have.get(s))]
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
        # Matched rows are rewritten only when Tiingo's adjusted values moved
        # (i.e. after a corporate-action re-pull), so _ingested_at stays meaningful.
        m = spark.sql(
            f"""
            MERGE INTO {TABLE} t
            USING incoming s
            ON t.symbol = s.symbol AND t.obs_date = s.obs_date
            WHEN MATCHED AND NOT (
                t.adj_open <=> s.adj_open AND t.adj_high <=> s.adj_high
                AND t.adj_low <=> s.adj_low AND t.adj_close <=> s.adj_close
                AND t.adj_volume <=> s.adj_volume
            ) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        ).collect()[0]
        print(f"Inserted {m['num_inserted_rows']} rows, updated {m['num_updated_rows']}.")
    else:
        df.write.format("delta").saveAsTable(TABLE)
        print(f"Loaded {df.count()} rows.")


run()
