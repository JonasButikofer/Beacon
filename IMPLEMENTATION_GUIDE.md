# Beacon — Implementation Guide (Private, Step-by-Step)

> **For Jonas only.** The build manual for `PROJECT_SPEC.md`. Follow it top to bottom.
> Every section has: **Goal → Steps (copy-paste) → Verify → Troubleshoot.**
> Assumes macOS/Linux shell (or WSL on Windows) and Python 3.11.

---

## Table of contents

- [0. Prerequisites & tooling](#0-prerequisites--tooling)
- [1. M0 — Environment & foundation](#1-m0--environment--foundation-6h)
- [2. M1 — Batch ingestion with PySpark](#2-m1--batch-ingestion-with-pyspark-10h)
- [3. M2 — Streaming ingestion](#3-m2--streaming-ingestion-12h)
- [4. M3 — dbt-databricks modeling](#4-m3--dbt-databricks-modeling-12h)
- [5. M4 — Orchestration with Databricks Workflows](#5-m4--orchestration-with-databricks-workflows-4h)
- [6. M5 — Data quality, CI & observability](#6-m5--data-quality-ci--observability-6h)
- [7. M6 — MCP server + custom analytics agent](#7-m6--mcp-server--custom-analytics-agent-10h)
- [8. M7 — Serving, docs & demo](#8-m7--serving-docs--demo-6h)
- [9. M8 — Buffer / polish / teardown](#9-m8--buffer--polish--teardown-4h)
- [10. Cost control](#10-cost-control)
- [11. Master troubleshooting index](#11-master-troubleshooting-index)
- [12. Sources](#12-sources)

---

## 0. Prerequisites & tooling

**Goal:** a clean local machine that can talk to Databricks, run Docker, and run Python.

### 0.1 Install local tools

```bash
# Python 3.11 + pipx for CLI tools
python3 --version                      # expect 3.11.x
python3 -m pip install --user pipx && python3 -m pipx ensurepath

# Databricks CLI (v0.2xx, the new one)
brew tap databricks/tap && brew install databricks   # macOS
# or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version

# Docker Desktop (for the MCP server + agent)
docker --version && docker compose version

# uv (fast Python env manager) — optional but recommended
curl -LsSf https://astral.sh/uv/install.sh | sh

# git
git --version
```

### 0.2 Decide your Databricks path (READ — this drives everything)

| | Free Edition | 14-day Premium Trial |
|---|---|---|
| Cost | **$0 forever** | Free 14 days, then bills **your cloud** |
| Compute | Serverless only | Full (job clusters, etc.) |
| Concurrency | **5 job tasks**, 1 SQL warehouse (2X-Small) | Full |
| Personal Access Tokens | Generally available (verify Day 1) | Yes |
| Workspace REST / Jobs API | **Yes** (workspace-level) | Yes |
| Account console / account-level APIs | **Blocked** | Yes |
| Managed MCP / Genie | Restricted | Yes |

**Plan:** build the whole graded pipeline (M0–M7) on Free Edition. Orchestration now runs on **native Databricks Workflows**, which lives inside the workspace and needs no external scheduler and no account-level API — so the earlier "does the PAT work?" question no longer gates orchestration. A PAT is still convenient for **dbt token auth** and the **MCP SQL connector** (M3, M6); if PATs are ever disabled, fall back to OAuth (U2M) for the CLI/dbt. Reserve the paid trial only if you decide to demo the **managed-MCP** path in M6. See §1.6.

**Verify:** `databricks --version` prints ≥ 0.210, `docker ps` runs without error.

---

## 1. M0 — Environment & foundation (6h)

**Goal:** a governed, version-controlled, secret-managed skeleton you can build into.

### 1.1 Create the Databricks workspace

1. Go to **databricks.com/learn/free-edition** → sign up with Google/Microsoft (no card).
2. Open the workspace. Note your **workspace URL** (e.g. `https://dbc-xxxx.cloud.databricks.com`) — this is your `DATABRICKS_HOST`.
3. Start a **serverless** notebook to confirm compute works: create a notebook, run `print(spark.version)`.

**Verify:** notebook prints a Spark version (e.g. `3.5.x`).

### 1.2 Authenticate the CLI

```bash
databricks configure --host https://dbc-xxxx.cloud.databricks.com
# It will open a browser for OAuth (U2M). Confirm:
databricks current-user me
```

**Troubleshoot:** if OAuth fails on Free Edition, use a PAT (see 1.6) with `databricks configure --token`.

### 1.3 Create Unity Catalog structure

Run in a Databricks SQL editor or notebook (`%sql`):

```sql
CREATE CATALOG IF NOT EXISTS beacon;
CREATE SCHEMA IF NOT EXISTS beacon.bronze;
CREATE SCHEMA IF NOT EXISTS beacon.silver;
CREATE SCHEMA IF NOT EXISTS beacon.gold;

-- Volumes for streaming landing + checkpoints (managed volumes)
CREATE VOLUME IF NOT EXISTS beacon.bronze.quote_landing;
CREATE VOLUME IF NOT EXISTS beacon.bronze.checkpoints;
```

**Verify:** `SHOW SCHEMAS IN beacon;` lists bronze/silver/gold. `SHOW VOLUMES IN beacon.bronze;` lists both.

### 1.4 Initialize the repo

```bash
mkdir beacon && cd beacon
git init
mkdir -p ingest dbt workflows mcp agent .github/workflows docs
touch README.md technical_decisions.md
cat > .gitignore <<'EOF'
.env
.env.*
!.env.sample
__pycache__/
.venv/
dbt/target/
dbt/logs/
dbt/dbt_packages/
*.duckdb
EOF
git add . && git commit -m "M0: scaffold"
```

Connect it to GitHub, then link it inside Databricks under **Workspace → Repos → Add Repo** so your notebooks/jobs live in Git. (The `workflows/` folder is for exported Job JSON definitions, so your orchestration is version-controlled too.)

### 1.5 Provision API keys

Register and copy keys:

| Service | URL | What to grab |
|---|---|---|
| FRED | fred.stlouisfed.org/docs/api/api_key.html | `FRED_KEY` (30 sec) |
| Tiingo | tiingo.com → API | `TIINGO_TOKEN` |
| Finnhub | finnhub.io | `FINNHUB_TOKEN` |

Create `.env.sample` (committed) and `.env` (ignored):

```bash
cat > .env.sample <<'EOF'
DATABRICKS_HOST=https://dbc-xxxx.cloud.databricks.com
DATABRICKS_TOKEN=            # PAT if available; else use OAuth
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxx
FRED_KEY=
TIINGO_TOKEN=
FINNHUB_TOKEN=
EOF
cp .env.sample .env    # then fill in .env
```

Store the same secrets in Databricks so jobs can read them:

```bash
databricks secrets create-scope beacon
databricks secrets put-secret beacon fred_key --string-value "PASTE"
databricks secrets put-secret beacon tiingo_token --string-value "PASTE"
databricks secrets put-secret beacon finnhub_token --string-value "PASTE"
databricks secrets list-secrets beacon
```

**Verify:** in a notebook, `dbutils.secrets.get("beacon","fred_key")` returns `[REDACTED]` (masked = it worked).

### 1.6 Day-1 auth check: can you make a PAT?

In the workspace: **Settings → Developer → Access tokens → Generate new token.**

- **If it succeeds** → put it in `.env` and Databricks secret `beacon/dbx_pat`. Token auth for dbt and the MCP SQL connector will work.
- **If it's disabled** → record it in `technical_decisions.md` and use **OAuth (U2M)** for the CLI and dbt instead. Because orchestration is native **Databricks Workflows** (in-workspace, no external caller), this does **not** block the graded pipeline. The only place a PAT still helps is the optional managed-MCP demo (M6) — open the 14-day trial for that only if you choose that path.

**Deliverable for M0:** working workspace, UC catalog/schemas/volumes, repo in Git, secrets set, auth path recorded.

---

## 2. M1 — Batch ingestion with PySpark (10h)

**Goal:** FRED macro + Tiingo EOD prices landing in Bronze Delta with incremental (watermark) logic.

### 2.1 FRED batch job

Create `ingest/batch_fred.py` (run as a Databricks notebook/job — `spark` and `dbutils` are pre-injected):

```python
# ingest/batch_fred.py
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

FRED_KEY = dbutils.secrets.get("beacon", "fred_key")
SERIES = ["CPIAUCSL", "UNRATE", "FEDFUNDS", "DGS10", "DGS2", "T10Y2Y", "GDP", "UMCSENT"]
TABLE  = "beacon.bronze.macro_raw"

def latest_dates() -> dict:
    """Return {series_id: max obs_date} already loaded, for incremental pulls."""
    if not spark.catalog.tableExists(TABLE):
        return {}
    rows = (spark.table(TABLE).groupBy("series_id")
                 .agg(F.max("obs_date").alias("mx")).collect())
    return {r["series_id"]: str(r["mx"]) for r in rows}

def fetch(series_id: str, start: str | None):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json"}
    if start:
        params["observation_start"] = start
    r = requests.get(url, params=params, timeout=30); r.raise_for_status()
    return [(series_id, o["date"], o["value"])
            for o in r.json()["observations"] if o["value"] != "."]

def run():
    have = latest_dates()
    rows = [row for s in SERIES for row in fetch(s, have.get(s))]
    if not rows:
        print("No new observations."); return
    schema = StructType([
        StructField("series_id", StringType()),
        StructField("obs_date",  StringType()),
        StructField("value",     StringType()),
    ])
    df = (spark.createDataFrame(rows, schema)
            .withColumn("obs_date", F.to_date("obs_date"))
            .withColumn("value", F.col("value").cast(DoubleType()))
            .withColumn("_ingested_at", F.current_timestamp()))
    df.createOrReplaceTempView("incoming")
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING incoming s
        ON t.series_id = s.series_id AND t.obs_date = s.obs_date
        WHEN NOT MATCHED THEN INSERT *
    """) if spark.catalog.tableExists(TABLE) else \
        df.write.format("delta").saveAsTable(TABLE)
    print(f"Loaded {df.count()} rows.")

run()
```

**Verify:** `SELECT series_id, count(*), max(obs_date) FROM beacon.bronze.macro_raw GROUP BY 1;` — 8 series, recent dates. Run the job twice; the second run prints "No new observations" (proves the watermark).

### 2.2 Markets batch job (Tiingo EOD)

```python
# ingest/batch_markets.py
import time, requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

TOKEN   = dbutils.secrets.get("beacon", "tiingo_token")
TICKERS = ["SPY", "QQQ", "XLF", "TLT", "KRE"]   # add bank-sector KRE for your Zions angle
TABLE   = "beacon.bronze.markets_raw"
HEADERS = {"Content-Type": "application/json"}

def last_date(ticker):
    if not spark.catalog.tableExists(TABLE): return "2015-01-01"
    r = (spark.table(TABLE).where(F.col("ticker")==ticker)
              .agg(F.max("price_date").alias("mx")).collect()[0]["mx"])
    return str(r) if r else "2015-01-01"

def fetch(ticker):
    start = last_date(ticker)
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"startDate": start, "token": TOKEN}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30); r.raise_for_status()
    return [(ticker, d["date"][:10], d["open"], d["high"], d["low"],
             d["close"], int(d["volume"])) for d in r.json()]

def run():
    rows = []
    for t in TICKERS:
        rows += fetch(t); time.sleep(2)     # stay under 50 req/hr
    schema = StructType([
        StructField("ticker", StringType()), StructField("price_date", StringType()),
        StructField("open", DoubleType()), StructField("high", DoubleType()),
        StructField("low", DoubleType()), StructField("close", DoubleType()),
        StructField("volume", LongType()),
    ])
    df = (spark.createDataFrame(rows, schema)
            .withColumn("price_date", F.to_date("price_date"))
            .withColumn("_ingested_at", F.current_timestamp()))
    df.createOrReplaceTempView("incoming_mkt")
    if spark.catalog.tableExists(TABLE):
        spark.sql(f"""
          MERGE INTO {TABLE} t USING incoming_mkt s
          ON t.ticker=s.ticker AND t.price_date=s.price_date
          WHEN NOT MATCHED THEN INSERT *""")
    else:
        df.write.format("delta").saveAsTable(TABLE)
    print(f"Loaded {df.count()} rows across {len(TICKERS)} tickers.")

run()
```

**Verify:** `SELECT ticker, count(*), min(price_date), max(price_date) FROM beacon.bronze.markets_raw GROUP BY 1;`

### 2.3 Package as Databricks Jobs

In **Workflows → Create job**, add two tasks (`ingest_fred`, `ingest_markets`) pointing at the notebooks in your Repo, serverless compute. In M4 you'll chain these into the full orchestrated Workflow.

**Troubleshoot:**
- `requests` not found on serverless → add a `%pip install requests` cell (usually preinstalled).
- FRED 400 → check `observation_start` format `YYYY-MM-DD`.
- Tiingo 404 → ticker unsupported on free tier; drop it.

**Deliverable for M1:** two Bronze Delta tables, incremental, re-runnable.

---

## 3. M2 — Streaming ingestion (12h)

**Goal:** live Finnhub quotes flowing into `beacon.bronze.quotes_raw` via Structured Streaming. *Highest-risk milestone — start tiny.*

> **Why two hops.** Spark Structured Streaming has **no native WebSocket source** — it can't subscribe to `wss://…` directly. The correct pattern (and what this milestone builds) is: a small **always-on consumer** holds the WebSocket and writes each trade as a JSON file into a **landing volume**, and **Auto Loader** then streams those files into Bronze. The producer is plain Python; all the Spark/streaming machinery sits on the file-based side.

### 3.1 Producer: WebSocket → landing volume

Run this as a **separate always-on Databricks job** (or locally during dev). It writes JSON lines into the `quote_landing` volume.

```python
# ingest/ws_producer.py
import json, time, websocket   # pip install websocket-client
TOKEN = dbutils.secrets.get("beacon", "finnhub_token")
SYMBOLS = ["AAPL", "MSFT", "SPY"]         # start with 3
LANDING = "/Volumes/beacon/bronze/quote_landing"

def on_open(ws):
    for s in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": s}))

def on_message(ws, message):
    msg = json.loads(message)
    if msg.get("type") != "trade":
        return                            # ignore ping/keepalive
    fn = f"{LANDING}/quotes_{int(time.time()*1000)}.json"
    with open(fn.replace('/Volumes','/dbfs/Volumes'), "w") as f:
        f.write(json.dumps(msg))

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={TOKEN}",
    on_open=on_open, on_message=on_message)
ws.run_forever(ping_interval=30)
```

### 3.2 Consumer: Auto Loader stream → Bronze

```python
# ingest/stream_quotes.py
from pyspark.sql import functions as F

schema = "data array<struct<s:string,p:double,v:double,t:long>>, type string"

stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/beacon/bronze/checkpoints/quotes_schema")
    .schema(schema)
    .load("/Volumes/beacon/bronze/quote_landing/"))

exploded = (stream.select(F.explode("data").alias("d"))
    .select(F.col("d.s").alias("symbol"),
            F.col("d.p").alias("price"),
            F.col("d.v").alias("volume"),
            (F.col("d.t")/1000).cast("timestamp").alias("event_ts"),
            F.current_timestamp().alias("_ingested_at")))

(exploded.writeStream.format("delta")
    .option("checkpointLocation", "/Volumes/beacon/bronze/checkpoints/quotes")
    .trigger(processingTime="30 seconds")     # micro-batch, NOT continuous
    .toTable("beacon.bronze.quotes_raw"))
```

### 3.3 Bring-up order

1. Start the **consumer** first (it creates the table + schema location).
2. Start the **producer**; watch files appear: `%fs ls /Volumes/beacon/bronze/quote_landing/`.
3. Query the sink after ~1 min: `SELECT * FROM beacon.bronze.quotes_raw ORDER BY event_ts DESC LIMIT 20;`

**Verify:** rows accumulate; the streaming query UI shows input rows > 0 per trigger.

**Troubleshoot:**
- No rows → markets closed (Finnhub trades only during market hours; test 9:30–16:00 ET). For off-hours dev, hand-write a few JSON files into the landing volume in the exact `data[]` shape.
- `checkpointLocation already exists` after a schema change → point to a fresh checkpoint dir or `rm -r` the old one.
- Producer disconnects → Finnhub free tier drops idle sockets; `ping_interval=30` handles most; add reconnect loop if needed.
- Don't run continuous trigger on Free Edition — it holds the single serverless slot.

**Deliverable for M2:** a running stream, checkpointed, with late-data tolerance, populating Bronze.

---

## 4. M3 — dbt-databricks modeling (12h)

**Goal:** Silver staging + Gold marts with tests, docs, and lineage.

### 4.1 Install & init

```bash
cd beacon
uv venv && source .venv/bin/activate       # or python -m venv .venv
uv pip install dbt-databricks
cd dbt && dbt init beacon_dbt              # choose 'databricks' adapter; then move files up if desired
```

### 4.2 `profiles.yml`

Put in `~/.dbt/profiles.yml` (or `dbt/profiles.yml` + `DBT_PROFILES_DIR`):

```yaml
beacon_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: beacon
      schema: silver
      host: "{{ env_var('DATABRICKS_HOST') | replace('https://','') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"   # SQL warehouse path
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
```

Get `http_path` from **SQL Warehouses → your warehouse → Connection details**.

**Verify:** `dbt debug` → "All checks passed!".

### 4.3 Sources

`dbt/models/staging/_sources.yml`:

```yaml
version: 2
sources:
  - name: bronze
    catalog: beacon
    schema: bronze
    tables:
      - name: macro_raw
        freshness: {warn_after: {count: 36, period: hour}, error_after: {count: 72, period: hour}}
        loaded_at_field: _ingested_at
      - name: markets_raw
        loaded_at_field: _ingested_at
      - name: quotes_raw
        loaded_at_field: _ingested_at
```

### 4.4 Silver (staging) models

`dbt/models/staging/stg_macro.sql`:

```sql
with src as (select * from {{ source('bronze','macro_raw') }})
select
    series_id,
    cast(obs_date as date)        as observation_date,
    cast(value as double)         as value,
    _ingested_at                  as ingested_at
from src
where value is not null
```

`stg_markets.sql`:

```sql
select
    ticker,
    price_date,
    open, high, low, close, volume,
    round((close - open)/nullif(open,0), 6) as intraday_return
from {{ source('bronze','markets_raw') }}
```

`stg_quotes.sql`:

```sql
select symbol, price, volume, event_ts,
       to_date(event_ts) as trade_date
from {{ source('bronze','quotes_raw') }}
```

### 4.5 Gold (marts)

`dbt/models/marts/dim_indicator.sql` (seed a small mapping or hardcode):

```sql
select * from (values
  ('CPIAUCSL','Consumer Price Index','monthly','index'),
  ('UNRATE','Unemployment Rate','monthly','percent'),
  ('FEDFUNDS','Federal Funds Rate','monthly','percent'),
  ('DGS10','10-Year Treasury Yield','daily','percent'),
  ('DGS2','2-Year Treasury Yield','daily','percent'),
  ('T10Y2Y','10Y-2Y Spread','daily','percent'),
  ('GDP','Gross Domestic Product','quarterly','usd_bn'),
  ('UMCSENT','Consumer Sentiment','monthly','index')
) as t(series_id, indicator_name, frequency, units)
```

`dbt/models/marts/fct_daily_market.sql`:

```sql
{{ config(materialized='table') }}
select ticker, price_date, close, volume, intraday_return
from {{ ref('stg_markets') }}
```

`dbt/models/marts/mart_macro_market_signals.sql` (the flagship join):

```sql
{{ config(materialized='table') }}
with mkt as (
    select price_date, ticker, close, intraday_return,
           avg(intraday_return) over (
               partition by ticker order by price_date
               rows between 4 preceding and current row) as rolling_5d_return
    from {{ ref('stg_markets') }}
),
macro as (   -- most recent macro value on/before each market date, per series
    select series_id, observation_date, value from {{ ref('stg_macro') }}
)
select m.price_date, m.ticker, m.close, m.intraday_return, m.rolling_5d_return,
       mc.series_id, mc.value as macro_value, mc.observation_date as macro_asof
from mkt m
left join macro mc
  on mc.observation_date = (
     select max(observation_date) from macro
     where series_id = mc.series_id and observation_date <= m.price_date)
```

### 4.6 Tests & docs

`dbt/models/marts/_marts.yml`:

```yaml
version: 2
models:
  - name: fct_daily_market
    columns:
      - name: ticker
        tests: [not_null]
      - name: price_date
        tests: [not_null]
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [ticker, price_date]
  - name: dim_indicator
    columns:
      - name: series_id
        tests: [not_null, unique]
```

Add `packages.yml` (`dbt-labs/dbt_utils`), then:

```bash
dbt deps
dbt build            # runs models + tests
dbt source freshness
dbt docs generate && dbt docs serve
```

**Verify:** `dbt build` is green; `mart_macro_market_signals` exists in `beacon.gold`; docs show lineage graph.

**Troubleshoot:**
- Materialization goes to wrong schema → set `+schema: gold` for `marts/` in `dbt_project.yml` under `models:`.
- Freshness fails → your Bronze `_ingested_at` is stale; run M1 jobs first.

**Deliverable for M3:** Silver views + Gold tables, tests passing, lineage docs.

---

## 5. M4 — Orchestration with Databricks Workflows (4h)

**Goal:** a scheduled, multi-task Databricks **Workflow** that runs `ingest_fred → ingest_markets → dbt build → dbt test`, with the streaming pipeline running as its own always-on job. Native orchestration — no external scheduler, no Docker, no PAT plumbing.

### 5.1 Build the batch multi-task job

In **Workflows → Create job** (`beacon_batch`), add four tasks and wire the dependencies with each task's **"Depends on"** field:

1. `ingest_fred` — Notebook task → `ingest/batch_fred.py`, serverless.
2. `ingest_markets` — Notebook task → `ingest/batch_markets.py`, **Depends on** `ingest_fred`.
3. `dbt_build` — **dbt task type** (see 5.2), **Depends on** `ingest_markets`.
4. `dbt_test` — dbt task running `dbt test`, **Depends on** `dbt_build`. (Or fold tests into `dbt build` and drop this task.)

Set each task's **Retries** (e.g. 2, with a 5-min backoff) in its task settings.

### 5.2 The dbt task

Databricks Jobs has a first-class **dbt task**: point it at your Git-linked Repo, select your **SQL warehouse** as the compute, and give it the commands:

```text
dbt deps
dbt build --target dev
```

Pass `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` (or OAuth) via the job's environment / secrets so `profiles.yml` resolves. This replaces the old "run dbt from an external container" approach — the warehouse runs the models, the Job runs dbt.

### 5.3 Schedule

In the job's **Schedules & Triggers**, add a cron schedule — daily at 06:00:

```text
0 0 6 * * ?      # Quartz cron in the Jobs UI: daily 6:00 AM
```

Set the job's max concurrent runs to 1 so overlapping daily runs can't collide.

### 5.4 Streaming job (separate, always-on)

Create a **second job** `beacon_stream` that runs the consumer (`ingest/ws_producer.py`) and the Auto Loader stream (`ingest/stream_quotes.py`). Keep it **out of** the batch job's critical path — the batch DAG should never wait on the stream. On Free Edition use the `processingTime="30 seconds"` micro-batch trigger (never continuous — it holds the single serverless slot). Add a lightweight **health check** (a small task hitting the Jobs API to confirm the stream is `RUNNING`, restarting it if not) if you want supervision.

### 5.5 Version-control the jobs

Export each job's definition and keep it in the repo so orchestration is reviewable:

```bash
databricks jobs get <JOB_ID> > workflows/beacon_batch.json
databricks jobs get <STREAM_JOB_ID> > workflows/beacon_stream.json
git add workflows/ && git commit -m "M4: workflow definitions"
```

**Verify:** trigger `beacon_batch` manually (**Run now**); all four tasks go green in the run graph; Bronze/Gold refresh; the scheduled run fires next morning.

**Troubleshoot:**
- dbt task can't auth → confirm the warehouse `http_path` and token/OAuth are set on the task; run `dbt debug` locally with the same env first.
- Task stuck queued → Free Edition caps at 5 concurrent job tasks; make sure the stream job isn't hogging slots.
- Wrong schema on build → set `+schema: gold` per-folder in `dbt_project.yml` (same fix as M3).

**Deliverable for M4:** a scheduled multi-task Workflow (batch) + an always-on streaming job, both defined in Git.

---

## 6. M5 — Data quality, CI & observability (6h)

**Goal:** tests gate every change; lineage is visible.

### 6.1 Expand dbt tests

Add `relationships` (mart → dim_indicator), `accepted_values` (frequency in monthly/daily/quarterly), and `not_null` on key columns. Add `dbt_utils.recency` on `fct_daily_market`.

### 6.2 GitHub Actions CI

`.github/workflows/dbt_ci.yml`:

```yaml
name: dbt CI
on: {pull_request: {branches: [main]}}
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_HTTP_PATH: ${{ secrets.DATABRICKS_HTTP_PATH }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install dbt-databricks
      - run: cd dbt && dbt deps && dbt build --target ci
```

Add repo **Secrets** for the three env vars. Use a separate `ci` schema in `profiles.yml` so CI doesn't touch prod Gold.

**Verify:** open a PR → the check runs and blocks merge on a failing test.

### 6.3 Lineage

Open **Unity Catalog → beacon.gold.mart_macro_market_signals → Lineage**; screenshot table + column lineage for the writeup.

**Deliverable for M5:** green CI on PRs, freshness SLAs, captured lineage.

---

## 7. M6 — MCP server + custom analytics agent (10h)

**Goal:** expose Gold via MCP, then an agent that answers economic questions.

### 7.1 Pick the MCP path

- **Managed (trial/Premium):** enable a **Genie space** over `beacon.gold`, then use the **Databricks-managed MCP** endpoint; point your MCP client at it. On-behalf-of-user auth respects Unity Catalog. Best if you have the trial open.
- **Custom (works on Free Edition):** a small FastMCP server (below) that runs governed SQL through the Databricks SQL connector. Recommended default.

### 7.2 Custom MCP server

```python
# mcp/server.py     (pip install mcp databricks-sql-connector)
import os
from mcp.server.fastmcp import FastMCP
from databricks import sql

mcp = FastMCP("beacon")
def conn():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://",""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"])

ALLOW = {"mart_macro_market_signals", "fct_daily_market", "dim_indicator"}

@mcp.tool()
def list_tables() -> list[str]:
    """List queryable Gold tables."""
    return sorted(ALLOW)

@mcp.tool()
def run_sql(query: str) -> list[dict]:
    """Run a READ-ONLY SELECT against beacon.gold. Rejects writes."""
    q = query.strip().lower()
    if not q.startswith("select") or any(k in q for k in ("insert","update","delete","drop","merge")):
        raise ValueError("Only SELECT queries are allowed.")
    with conn() as c, c.cursor() as cur:
        cur.execute(f"USE CATALOG beacon; USE SCHEMA gold;")
        cur.execute(query if "limit" in q else query + " limit 500")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

if __name__ == "__main__":
    mcp.run(transport="stdio")
```

**Verify:** register the server in your MCP client (Claude Desktop config or your agent), call `list_tables`, then `run_sql("select * from mart_macro_market_signals limit 5")`.

### 7.3 Custom analytics agent

`agent/agent.py` — a tool-calling loop (Anthropic/OpenAI) that uses the MCP tools:

```python
# agent/agent.py  (pseudocode-level; fill with your SDK of choice)
SYSTEM = """You are Beacon, a macro-markets analyst. You can call MCP tools
list_tables and run_sql (READ-ONLY) against beacon.gold. Given a question:
1) inspect schema, 2) write ONE SELECT, 3) run it, 4) answer in 2-3 sentences
with a concrete number and a one-line 'why it matters'. Never write DML."""

def answer(question: str) -> str:
    # loop: send question + SYSTEM + tool schemas -> model
    # when model requests run_sql, execute via MCP, feed result back
    # return final natural-language insight
    ...
```

Scope **v1 = query + summarize**. Stretch = multi-step reasoning, chart suggestion, follow-up questions.

Guardrails: read-only warehouse role, `LIMIT` injection (above), table allow-list, log every query to a file.

**Verify (demo questions):**
- "How did SPY move in the 5 days around the last CPI reading?"
- "What's the current 10Y-2Y spread and how has KRE performed this month?"

**Deliverable for M6:** MCP server + agent answering real questions with governed SQL.

---

## 8. M7 — Serving, docs & demo (6h)

1. **Dashboard:** Databricks SQL → new dashboard on `mart_macro_market_signals`: a line chart (macro overlay vs. returns), a table of latest quotes, a KPI tile (yield spread).
2. **README.md:** match your IS 566 README quality — problem, architecture (paste the mermaid), tech stack table, data flow, how-to-run.
3. **technical_decisions.md:** record every "why" (Databricks vs Snowflake, **Databricks Workflows vs Prefect/external scheduler**, Free Edition vs trial, MCP custom vs managed, streaming trigger choice, the WebSocket-consumer-then-Auto-Loader pattern).
4. **Diagram:** export the architecture mermaid to PNG for slides.
5. **Demo (3–5 min):** batch Workflow run → stream live rows → `dbt build` green → ask the agent two questions. Record it.

**Deliverable for M7:** dashboard + full docs + recorded demo.

---

## 9. M8 — Buffer / polish / teardown (4h)

- Full **end-to-end dry run** from a clean state.
- Fix any breakage; re-run `dbt build` and CI.
- **Teardown paid compute** (stop/delete clusters, close trial if you opened one for managed MCP) to protect budget.
- Final pass against the spec's **learning outcomes** — confirm each is demonstrable.

---

## 10. Cost control

- Build the whole pipeline on **Free Edition** ($0).
- Open the **Premium trial only** if you choose the managed-MCP path (M6); set a cloud **budget alert**; **stop clusters** every session.
- SQL warehouse: **2X-Small + auto-stop 10 min**.
- Streaming: **3 symbols, 30s trigger**, never continuous on Free Edition.
- Market APIs: stay on free tiers; upgrade **one** (Tiingo) only if you need more symbols. Budget headroom: ~$30/mo Tiingo power tier if needed.

---

## 11. Master troubleshooting index

| Symptom | Likely cause | Fix |
|---|---|---|
| CLI OAuth loops | Free Edition auth quirk | Use PAT + `databricks configure --token` |
| `dbutils.secrets.get` errors | scope/key typo | `databricks secrets list-secrets beacon` |
| Stream has 0 rows | market closed / no files | test in market hours; hand-seed JSON |
| `checkpointLocation already exists` | schema changed | new checkpoint dir |
| dbt writes to wrong schema | missing `+schema` | set per-folder schema in `dbt_project.yml` |
| Workflow dbt task can't auth | warehouse path / token not set on task | set env/secrets on the task; `dbt debug` locally first |
| Tasks stuck queued | Free Edition 5-task concurrency cap | keep stream job off the batch critical path |
| CI touches prod Gold | shared schema | dedicated `ci` schema/target |
| Agent runs a write query | guardrail gap | enforce SELECT-only + read-only role |
| Trial surprise bill | cluster left running | auto-stop + M8 teardown |

---

## 12. Sources

- Databricks Free Edition & limits: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations · https://docs.databricks.com/aws/en/getting-started/free-edition
- Databricks Workflows / Jobs (orchestration, dbt task): https://docs.databricks.com/aws/en/jobs/ · https://docs.databricks.com/aws/en/jobs/dbt
- Databricks managed MCP (Unity Catalog + Genie, Beta 2025): https://www.databricks.com/blog/announcing-managed-mcp-servers-unity-catalog-and-mosaic-ai-integration · https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp
- Auto Loader / Structured Streaming: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- dbt-databricks: https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup
- FRED API: https://fred.stlouisfed.org/docs/api/fred/ · key: https://fred.stlouisfed.org/docs/api/api_key.html
- Market data free tiers (Finnhub / Tiingo / Alpha Vantage): https://www.alphavantage.co/ · https://www.nb-data.com/p/best-financial-data-apis-in-2026
