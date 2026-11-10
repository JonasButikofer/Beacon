# ingest/batch_tiingo_eod.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
#
# Two modes, one beacon_batch task each:
#   --symbols watchlist (default)  the 9 ETFs below -> beacon.bronze.markets_raw
#   --symbols companies            the stocks in the sector ETFs' latest holdings
#                                  snapshot (etf_holdings_raw) -> beacon.bronze.company_prices_raw
#
# Company mode spends at most COMPANY_CALLS_PER_RUN Tiingo requests per run.
# Symbols never loaded go first, then the most out-of-date, so a capped daily
# run works through the list in rotation and every run's history stays gap-free
# (each pull starts at that symbol's own watermark).
import argparse

from databricks.sdk.runtime import dbutils, spark
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType,
)

parser = argparse.ArgumentParser()
parser.add_argument("--symbols", choices=["watchlist", "companies"], default="watchlist")
args, _ = parser.parse_known_args()
COMPANIES = args.symbols == "companies"

TIINGO_TOKEN = dbutils.secrets.get("beacon", "tiingo_token")
TABLE = "beacon.bronze.company_prices_raw" if COMPANIES else "beacon.bronze.markets_raw"
HOLDINGS_TABLE = "beacon.bronze.etf_holdings_raw"
UNIVERSE_TABLE = "beacon.bronze.tickers_universe"
# Tiingo's /prices endpoint returns only the single latest day when startDate is
# omitted — so first-time pulls (no watermark yet) must pass an explicit far-past
# date to get full history, not just "today". Companies start later to keep each
# pull small; 2015 still gives every 6-month window years of history.
BACKFILL_START = "2015-01-01" if COMPANIES else "1990-01-01"

# Tiingo's free plan allows 50 requests an hour. The watchlist and ticker
# metadata tasks run alongside this one in beacon_batch and use ~18 of them
# (more on a day with dividend re-pulls), so companies get the rest minus a
# margin. At ~210 companies that refreshes each one about every 7 trading days.
# On a paid plan, set this to 0 (no cap) and every company is current daily.
COMPANY_CALLS_PER_RUN = 30
calls_made = 0

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


def company_symbols() -> list[str]:
    """Stocks in each sector ETF's latest holdings snapshot that Tiingo lists.

    The universe join drops cash, money market and index-future lines, which
    would otherwise 404 and, never getting a watermark, jump the queue every run.
    """
    if not spark.catalog.tableExists(HOLDINGS_TABLE):
        return []
    rows = spark.sql(
        f"""
        WITH latest AS (
            SELECT etf_symbol, max(as_of_date) AS as_of_date
            FROM {HOLDINGS_TABLE}
            GROUP BY etf_symbol
        ),
        stocks AS (
            SELECT DISTINCT upper(ticker) AS symbol
            FROM {UNIVERSE_TABLE}
            WHERE asset_type = 'Stock'
        )
        SELECT DISTINCT h.symbol
        FROM {HOLDINGS_TABLE} AS h
        INNER JOIN latest AS l
            ON l.etf_symbol = h.etf_symbol AND l.as_of_date = h.as_of_date
        INNER JOIN stocks AS s
            ON s.symbol = h.symbol
        """
    ).collect()
    return sorted(r["symbol"] for r in rows)


def fetch(symbol: str, start: str):
    global calls_made
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params = {"startDate": start}
    calls_made += 1
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


def fetch_companies(have: dict) -> list:
    """Pull companies within the call budget, never-loaded first, then oldest.

    A 429 (hourly limit hit early, e.g. by a manual re-run) or a 404 (ticker
    Tiingo doesn't serve) skips ahead instead of failing the task, so the rows
    already fetched still get written.
    """
    symbols = company_symbols()
    if not symbols:
        print(f"No company symbols yet; run the holdings ingest ({HOLDINGS_TABLE}) first.")
        return []
    queue = sorted(symbols, key=lambda s: (s in have, have.get(s, "")))
    rows, done = [], 0
    for s in queue:
        if COMPANY_CALLS_PER_RUN and calls_made >= COMPANY_CALLS_PER_RUN:
            break
        try:
            rows.extend(fetch_symbol(s, have.get(s)))
            done += 1
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                print(f"{s}: not found on Tiingo; skipped.")
                continue
            if status == 429:
                print("Tiingo rate limit reached; stopping early.")
                break
            raise
    behind = len(queue) - done
    print(f"Fetched {done} of {len(queue)} companies in {calls_made} calls; {behind} wait for a later run.")
    return rows


def run():
    have = latest_dates()
    if COMPANIES:
        rows = fetch_companies(have)
    else:
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
