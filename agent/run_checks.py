# agent/run_checks.py
# Job task 1 of beacon_quality: run every rule-based check in checks.py and
# append the results to beacon.ops.dq_results. Never fails on bad data — the
# diagnose task reads the results and decides whether the job fails.
#
# Args: --run-id {{job.run_id}}  --bundle-root ${workspace.file_path}  --target ${bundle.target}
import argparse
import sys

from databricks.sdk.runtime import spark

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--bundle-root", required=True)
parser.add_argument("--target", default="dev")
args, _ = parser.parse_known_args()
sys.path.insert(0, f"{args.bundle_root}/agent")

import config as C  # noqa: E402

C.TARGET = args.target
import checks  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StructType, StructField, StringType  # noqa: E402

# Dates in the checks (current_date, to_date(trade_ts)) mean US market time.
spark.conf.set("spark.sql.session.timeZone", "America/New_York")


def ensure_tables():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.OPS_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {C.RESULTS_TABLE} (
            run_id STRING, check_ts TIMESTAMP, check_name STRING, table_name STRING,
            severity STRING, status STRING, message STRING, observed STRING
        ) USING DELTA
    """)


def run():
    ensure_tables()
    results = checks.run_all()

    schema = StructType([
        StructField(c, StringType())
        for c in ["run_id", "check_name", "table_name", "severity", "status", "message", "observed"]
    ])
    df = (
        spark.createDataFrame([checks.to_row(r, args.run_id) for r in results], schema)
        .withColumn("check_ts", F.current_timestamp())
        .select("run_id", "check_ts", "check_name", "table_name", "severity", "status", "message", "observed")
    )
    # A retried or repaired task replaces this run's rows instead of duplicating them.
    spark.sql(f"DELETE FROM {C.RESULTS_TABLE} WHERE run_id = '{args.run_id}'")
    df.write.mode("append").saveAsTable(C.RESULTS_TABLE)

    for r in results:
        print(f"[{r.status.upper():5}] {r.severity:5} {r.check_name}: {r.message}")
    bad = [r for r in results if r.status != "pass"]
    print(f"\n{len(results) - len(bad)}/{len(results)} checks passed.")


run()
