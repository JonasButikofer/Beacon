# ingest/batch_fred.py
# Databricks notebook / job task — `spark` and `dbutils` are pre-injected.
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

FRED_KEY = dbutils.secrets.get("beacon", "fred_key")
SERIES = ["CPIAUCSL", "UNRATE", "FEDFUNDS", "DGS10", "DGS2", "T10Y2Y", "GDP", "UMCSENT"]
TABLE = "beacon.bronze.macro_raw"


def latest_dates() -> dict:
    """Return {series_id: max obs_date} already loaded, for incremental pulls."""
    if not spark.catalog.tableExists(TABLE):
        return {}
    rows = (
        spark.table(TABLE)
        .groupBy("series_id")
        .agg(F.max("obs_date").alias("mx"))
        .collect()
    )
    return {r["series_id"]: str(r["mx"]) for r in rows}


def fetch(series_id: str, start: str | None):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json"}
    if start:
        params["observation_start"] = start
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return [
        (series_id, o["date"], o["value"])
        for o in r.json()["observations"]
        if o["value"] != "."
    ]


def run():
    have = latest_dates()
    rows = [row for s in SERIES for row in fetch(s, have.get(s))]
    if not rows:
        print("No new observations.")
        return
    schema = StructType(
        [
            StructField("series_id", StringType()),
            StructField("obs_date", StringType()),
            StructField("value", StringType()),
        ]
    )
    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("obs_date", F.to_date("obs_date"))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.createOrReplaceTempView("incoming")
    if spark.catalog.tableExists(TABLE):
        spark.sql(
            f"""
            MERGE INTO {TABLE} t
            USING incoming s
            ON t.series_id = s.series_id AND t.obs_date = s.obs_date
            WHEN NOT MATCHED THEN INSERT *
        """
        )
    else:
        df.write.format("delta").saveAsTable(TABLE)
    print(f"Loaded {df.count()} rows.")


run()
