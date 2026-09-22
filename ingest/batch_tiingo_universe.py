# ingest/batch_tiingo_universe.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
#
# Full Tiingo-supported-symbol universe (~100k+ tickers), pulled from the static
# bulk CSV rather than per-symbol calls (no per-ticker rate-limit cost). Thin
# columns only (no name/description) — join against tickers_raw in dbt for the
# richer metadata on the symbols actually tracked.
import csv
import io
import zipfile

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

UNIVERSE_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
TABLE = "beacon.bronze.tickers_universe"


def fetch_rows():
    r = requests.get(UNIVERSE_URL, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
        with z.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            return [
                (
                    row["ticker"],
                    row["exchange"] or None,
                    row["assetType"] or None,
                    row["priceCurrency"] or None,
                    row["startDate"] or None,
                    row["endDate"] or None,
                )
                for row in reader
            ]


def run():
    rows = fetch_rows()
    schema = StructType(
        [
            StructField("ticker", StringType()),
            StructField("exchange", StringType()),
            StructField("asset_type", StringType()),
            StructField("price_currency", StringType()),
            StructField("start_date", StringType()),
            StructField("end_date", StringType()),
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
    print(f"Loaded {df.count()} tickers into universe.")


run()
