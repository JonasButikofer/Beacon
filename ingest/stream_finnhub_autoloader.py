# ingest/stream_finnhub_autoloader.py
# Databricks notebook / job task — `spark` is pre-injected.
#
# Structured Streaming job: Auto Loader incrementally ingests JSON files landed
# by stream_finnhub_consumer.py into Bronze. Uses trigger(availableNow=True) —
# processes whatever's currently in the landing volume, then stops, which is
# right for testing and for running as a scheduled batch-style task. Swap to
# .trigger(processingTime="30 seconds") and run it as its own always-on Job
# task if it needs to keep pace with the consumer continuously (per spec M2).
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType,
)

LANDING_PATH = "/Volumes/beacon/bronze/quote_landing"
CHECKPOINT_PATH = "/Volumes/beacon/bronze/checkpoints/quotes_raw"
TABLE = "beacon.bronze.quotes_raw"

SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("trade_time", LongType()),
        StructField("conditions", ArrayType(StringType())),
    ]
)

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(SCHEMA)
    .load(LANDING_PATH)
    .withColumn("trade_ts", F.to_timestamp(F.col("trade_time") / 1000))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TABLE)
)

query.awaitTermination()
