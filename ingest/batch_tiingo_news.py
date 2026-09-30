# ingest/batch_tiingo_news.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
#
# Tiingo's free/Starter key includes the News endpoint (50 req/hr, 1,000/day),
# but the free tier has NO historical archive — only real-time/forward articles
# are returned, so the first run will not backfill past news. If the key isn't
# entitled at all, the API returns 403 — see the explicit check in run() below
# rather than letting that fail silently as "no new articles."
from databricks.sdk.runtime import dbutils, spark
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

TIINGO_TOKEN = dbutils.secrets.get("beacon", "tiingo_token")
TABLE = "beacon.bronze.news_raw"
TICKERS = ["spy", "qqq", "dia", "iwm", "xlf", "xle", "xlk", "xlu", "tlt"]

HEADERS = {"Content-Type": "application/json", "Authorization": f"Token {TIINGO_TOKEN}"}


def latest_published() -> str | None:
    """Return the max published_date already loaded, for incremental pulls."""
    if not spark.catalog.tableExists(TABLE):
        return None
    mx = spark.table(TABLE).agg(F.max("published_date")).collect()[0][0]
    return str(mx) if mx else None


def fetch(start: str | None):
    url = "https://api.tiingo.com/tiingo/news"
    params = {"tickers": ",".join(TICKERS), "limit": 1000, "sortBy": "publishedDate"}
    if start:
        params["startDate"] = start
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if r.status_code == 403:
        raise RuntimeError(
            "Tiingo News API returned 403 — this key's plan may not include "
            "News access. Check entitlement at tiingo.com/account/api/token "
            "before assuming there's simply nothing new."
        )
    r.raise_for_status()
    return [
        (
            a["id"],
            a.get("title"),
            a.get("description"),
            a.get("url"),
            a.get("source"),
            a.get("publishedDate"),
            a.get("crawlDate"),
            a.get("tickers", []),
            a.get("tags", []),
        )
        for a in r.json()
    ]


def run():
    rows = fetch(latest_published())
    if not rows:
        print("No new articles.")
        return
    schema = StructType(
        [
            StructField("article_id", StringType()),
            StructField("title", StringType()),
            StructField("description", StringType()),
            StructField("url", StringType()),
            StructField("source", StringType()),
            StructField("published_date", StringType()),
            StructField("crawl_date", StringType()),
            StructField("tickers", ArrayType(StringType())),
            StructField("tags", ArrayType(StringType())),
        ]
    )
    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("published_date", F.to_timestamp("published_date"))
        .withColumn("crawl_date", F.to_timestamp("crawl_date"))
        .withColumn("_ingested_at", F.current_timestamp())
        .dropDuplicates(["article_id"])
    )
    df.createOrReplaceTempView("incoming")
    if spark.catalog.tableExists(TABLE):
        spark.sql(
            f"""
            MERGE INTO {TABLE} t
            USING incoming s
            ON t.article_id = s.article_id
            WHEN NOT MATCHED THEN INSERT *
        """
        )
    else:
        df.write.format("delta").saveAsTable(TABLE)
    print(f"Loaded {df.count()} articles.")


run()
