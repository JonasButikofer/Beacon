# ingest/batch_etf_holdings.py
# Databricks notebook / job task — `spark` is pre-injected.
#
# Holdings of the sector SPDR ETFs: which companies each fund owns and at what
# weight, from the daily .xlsx State Street publishes (free, no key). One row
# per line of the file, including cash, money market and index-future lines,
# so weights still sum to ~100%. Those lines have a null or non-stock symbol;
# stg_etf_holdings marks real stocks with is_equity.
#
# Each run adds one snapshot per ETF, keyed by the file's "As of" date.
# Re-running for the same date replaces that snapshot (replaceWhere), so the
# table never holds two versions of one day.
from databricks.sdk.runtime import spark
import io
from datetime import datetime

import openpyxl
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType,
)

TABLE = "beacon.bronze.etf_holdings_raw"
URL = "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{etf}.xlsx"

# The sector ETFs to drill into. IWM (~2,000 small caps) and TLT (bonds) are
# left out; QQQ and DIA aren't State Street funds.
ETFS = ["XLK", "XLF", "XLE", "XLU"]

# Columns of the holdings table, in file order. Checked on every run so a
# layout change fails loudly instead of loading shifted columns.
EXPECTED_COLUMNS = [
    "Name", "Ticker", "Identifier", "SEDOL", "Weight", "Sector", "Shares Held", "Local Currency",
]

SCHEMA = StructType(
    [
        StructField("etf_symbol", StringType()),
        StructField("as_of_date", DateType()),
        StructField("position", IntegerType()),
        StructField("holding_name", StringType()),
        StructField("ticker", StringType()),
        StructField("symbol", StringType()),
        StructField("identifier", StringType()),
        StructField("sedol", StringType()),
        StructField("weight_pct", DoubleType()),
        StructField("shares_held", DoubleType()),
        StructField("local_currency", StringType()),
    ]
)


def to_symbol(ticker: str | None) -> str | None:
    """State Street ticker -> Tiingo symbol. Share classes use '-' in Tiingo
    (BRK.B -> BRK-B). Cash lines have '-' as the ticker."""
    if ticker is None or ticker.strip() in ("", "-"):
        return None
    return ticker.strip().upper().replace(".", "-")


def text(v) -> str | None:
    return None if v is None else str(v).strip() or None


def parse(etf: str, content: bytes):
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    rows = list(wb.worksheets[0].iter_rows(values_only=True))

    # Header block: "Fund Name:", "Ticker Symbol:", "Holdings: As of 04-Nov-2026".
    meta = {text(r[0]): r[1] for r in rows[:4] if r and r[0]}
    if text(meta.get("Ticker Symbol:")) != etf:
        raise ValueError(f"{etf}: file is for {meta.get('Ticker Symbol:')!r}")
    as_of = datetime.strptime(
        text(meta["Holdings:"]).replace("As of", "").strip(), "%d-%b-%Y"
    ).date()

    header_at = next(i for i, r in enumerate(rows) if r and text(r[0]) == "Name")
    columns = [text(c) for c in rows[header_at][: len(EXPECTED_COLUMNS)]]
    if columns != EXPECTED_COLUMNS:
        raise ValueError(f"{etf}: unexpected columns {columns}")

    out = []
    for position, r in enumerate(rows[header_at + 1:], start=1):
        # The table ends at the first blank row; disclaimers follow it.
        if not r or r[0] is None:
            break
        name, ticker, identifier, sedol, weight, _sector, shares, currency = r[:8]
        out.append(
            (
                etf,
                as_of,
                position,
                text(name),
                text(ticker),
                to_symbol(text(ticker)),
                text(identifier),
                text(sedol),
                float(weight) if weight is not None else None,
                float(shares) if shares is not None else None,
                text(currency),
            )
        )
    if not out:
        raise ValueError(f"{etf}: no holdings rows found")
    return as_of, out


def fetch(etf: str) -> bytes:
    r = requests.get(URL.format(etf=etf.lower()), timeout=60)
    r.raise_for_status()
    if "spreadsheetml" not in r.headers.get("Content-Type", ""):
        raise ValueError(f"{etf}: expected an .xlsx, got {r.headers.get('Content-Type')}")
    return r.content


def write(etf: str, as_of, rows):
    df = spark.createDataFrame(rows, SCHEMA).withColumn("_ingested_at", F.current_timestamp())
    if not spark.catalog.tableExists(TABLE):
        df.write.format("delta").saveAsTable(TABLE)
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"etf_symbol = '{etf}' AND as_of_date = '{as_of}'")
            .saveAsTable(TABLE)
        )
    print(f"{etf}: {len(rows)} lines as of {as_of}.")


def run():
    # One ETF failing (e.g. a layout change) doesn't stop the others from loading.
    errors = []
    for etf in ETFS:
        try:
            as_of, rows = parse(etf, fetch(etf))
            write(etf, as_of, rows)
        except Exception as e:  # noqa: BLE001 — collected and re-raised below
            print(f"{etf}: FAILED: {e}")
            errors.append(f"{etf}: {e}")
    if errors:
        raise RuntimeError("Holdings ingest failed for: " + "; ".join(errors))


run()
