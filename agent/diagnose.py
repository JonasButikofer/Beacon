# agent/diagnose.py
# Job task 2 of beacon_quality: if any check in this run didn't pass, hand the
# failures to Claude with a few read-only tools (SQL, job run history, the
# ingest source code), and save its diagnosis to beacon.ops.dq_incidents.
#
# The agent never changes data or code. This task fails the job when an
# `error`-severity check failed, so the job's email notification fires; the
# email links to this run, and the diagnosis is printed in its output.
#
# Args: --run-id {{job.run_id}}  --bundle-root ${workspace.file_path}
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

from databricks.sdk.runtime import dbutils, spark

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--bundle-root", required=True)
args, _ = parser.parse_known_args()
sys.path.insert(0, f"{args.bundle_root}/agent")

import anthropic  # noqa: E402
import config as C  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "America/New_York")

SYSTEM = """You are the data quality on-call engineer for Beacon, a personal market-data \
lakehouse on Databricks Free Edition (serverless, 5-task concurrency cap).

Pipeline:
- Job beacon_daily (6:00 AM ET) runs beacon_batch, then this data quality job (beacon_quality)
  as soon as ingest finishes. Neither child job has a schedule of its own.
- Batch ingest (job beacon_batch) writes to beacon.bronze:
  macro_raw (FRED, MERGE on series_id+obs_date, re-pulls the last 400 days for revisions),
  markets_raw (Tiingo EOD for 9 ETFs, MERGE on symbol+obs_date, full re-pull of a symbol after a split/dividend),
  news_raw (Tiingo news; ingest currently disabled, the free plan has no news access),
  tickers_raw (watchlist metadata, full overwrite).
- tickers_universe: ~100k symbols from Tiingo's bulk zip, weekly full overwrite (beacon_universe_weekly).
- quotes_raw: Finnhub WebSocket consumer (beacon_stream, market hours only) lands JSON in a volume;
  Auto Loader (beacon_quotes_loader, every 30 min) appends to the table.
- Data quality results are in beacon.ops.dq_results; past diagnoses in beacon.ops.dq_incidents.

Your job: for each failed check, find the most likely root cause using the tools, then report.
- Gather evidence before concluding: query the data, check the latest job runs and their errors,
  and read the relevant ingest script when the cause may be in code.
- Look at dq_results history: is this new today, or has it been failing for days?
- Distinguish a real data problem from a check threshold that is too strict (e.g. a market
  holiday, a slow news weekend, a FRED release delay). Say so when it's the threshold.
- You are read-only. Suggest fixes; never claim you applied one.
- If the evidence is inconclusive, say what you'd check next instead of guessing.

Final answer: a Markdown report and nothing else, in this shape:
## Summary
One to three sentences: what's wrong and how urgent.
### <check_name>
- **Likely cause:** ...
- **Evidence:** the queries/runs/lines that show it
- **Suggested fix:** concrete steps (file and line if it's code)
- **Urgency:** fix today | fix this week | tune the check
"""

TOOLS = [
    {
        "name": "run_sql",
        "description": (
            "Run one read-only Spark SQL query (SELECT, WITH, DESCRIBE, SHOW, or LIST '<volume path>' "
            "to list files, e.g. LIST '/Volumes/beacon/bronze/quote_landing') and return up to "
            f"{C.SQL_ROW_LIMIT} rows as JSON lines. Session time zone is America/New_York. "
            "Tables of interest: beacon.bronze.*, beacon.ops.dq_results, beacon.ops.dq_incidents."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "A single SQL statement, no trailing semicolon."}},
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_job_runs",
        "description": (
            "Show a Beacon job's deploy time, schedule and pause status, then its most recent runs "
            "with each task's state and the error message for any failed task. Job names: "
            "beacon_daily, beacon_batch, beacon_stream, beacon_quotes_loader, "
            "beacon_universe_weekly, beacon_quality."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "job_name": {"type": "string", "description": "Job name, matched as a suffix (dev jobs are prefixed)."},
                "limit": {"type": "integer", "description": "How many recent runs, 1-10."},
            },
            "required": ["job_name", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_source_files",
        "description": f"List the project's source files under {', '.join(C.READABLE_DIRS)}.",
        "strict": True,
        "input_schema": {"type": "object", "properties": {}, "required": [], "additionalProperties": False},
    },
    {
        "name": "read_source_file",
        "description": "Read one project source file, e.g. ingest/batch_tiingo_eod.py or resources/beacon_batch.job.yml.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Path relative to the project root."}},
            "required": ["path"],
            "additionalProperties": False,
        },
    },
]

SECRET_PATTERN = re.compile(r"sk-ant-[A-Za-z0-9_\-]+")


def scrub(text: str) -> str:
    """Mask API keys before text is stored in a table or sent to the model."""
    return SECRET_PATTERN.sub("sk-ant-***", text)


READ_ONLY_START = re.compile(r"^\s*(select|with|describe|desc|show|list)\b", re.I)
WRITE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|optimize|"
    r"vacuum|copy|refresh|call|set|reset|use|msck|restore|cache|uncache)\b",
    re.I,
)


def run_sql(query: str) -> str:
    q = query.strip().rstrip(";")
    if ";" in q or not READ_ONLY_START.match(q) or WRITE_KEYWORDS.search(q):
        raise ValueError("Only a single read-only SELECT/WITH/DESCRIBE/SHOW/LIST statement is allowed.")
    rows = spark.sql(q).limit(C.SQL_ROW_LIMIT).collect()
    if not rows:
        return "(0 rows)"
    return "\n".join(json.dumps(r.asDict(recursive=True), default=str) for r in rows)


def ts_iso(ms) -> str | None:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat() if ms else None


def get_job_runs(job_name: str, limit: int) -> str:
    w = WorkspaceClient()
    jobs = [j for j in w.jobs.list() if j.settings and j.settings.name.endswith(job_name)]
    if not jobs:
        return f"No deployed job named *{job_name}."
    out = []
    for j in jobs:
        sched = j.settings.schedule
        out.append({
            "job": j.settings.name,
            "job_id": j.job_id,
            "deployed_at": ts_iso(j.created_time),
            "schedule": sched.quartz_cron_expression if sched else None,
            "timezone": sched.timezone_id if sched else None,
            "schedule_paused": (sched.pause_status.value == "PAUSED")
                               if sched and sched.pause_status else None,
            "note": None if sched else "No schedule of its own; may be started by another job (beacon_daily).",
        })
        runs = list(w.jobs.list_runs(job_id=j.job_id, limit=max(1, min(limit, 10)), expand_tasks=True))
        if not runs:
            out[-1]["runs"] = "none yet"
        for run in runs:
            state = run.state
            entry = {
                "job": j.settings.name,
                "run_id": run.run_id,
                "started_at": ts_iso(run.start_time),
                "life_cycle": state.life_cycle_state.value if state and state.life_cycle_state else None,
                "result": state.result_state.value if state and state.result_state else None,
                "message": state.state_message if state else None,
                "tasks": [],
            }
            for t in run.tasks or []:
                ts = t.state
                task = {
                    "task_key": t.task_key,
                    "result": ts.result_state.value if ts and ts.result_state else None,
                    "message": ts.state_message if ts else None,
                }
                if task["result"] and task["result"] != "SUCCESS":
                    try:
                        o = w.jobs.get_run_output(run_id=t.run_id)
                        task["error"] = (o.error or "")[:2000]
                        task["error_trace"] = (o.error_trace or "")[-3000:]
                    except Exception as e:  # noqa: BLE001
                        task["error"] = f"(could not fetch output: {e})"
                entry["tasks"].append(task)
            out.append(entry)
    return json.dumps(out, default=str, indent=1)


def list_source_files() -> str:
    paths = []
    for d in C.READABLE_DIRS:
        for root, dirs, files in os.walk(f"{args.bundle_root}/{d}"):
            dirs[:] = [x for x in dirs if x not in ("target", "logs", "dbt_packages", "__pycache__")]
            for f in files:
                paths.append(os.path.relpath(os.path.join(root, f), args.bundle_root).replace("\\", "/"))
    return "\n".join(sorted(paths))


def read_source_file(path: str) -> str:
    rel = os.path.normpath(path).replace("\\", "/").lstrip("/")
    if rel.startswith("..") or rel.split("/")[0] not in C.READABLE_DIRS:
        raise ValueError(f"Can only read files under: {', '.join(C.READABLE_DIRS)}")
    with open(f"{args.bundle_root}/{rel}", encoding="utf-8") as f:
        return f.read()[:40_000]


HANDLERS = {
    "run_sql": lambda i: run_sql(i["query"]),
    "get_job_runs": lambda i: get_job_runs(i["job_name"], i["limit"]),
    "list_source_files": lambda i: list_source_files(),
    "read_source_file": lambda i: read_source_file(i["path"]),
}


def failed_results():
    return spark.sql(f"""
        SELECT check_name, table_name, severity, status, message, observed
        FROM {C.RESULTS_TABLE}
        WHERE run_id = '{args.run_id}' AND status != 'pass'
    """).collect()


def api_key() -> str:
    key = dbutils.secrets.get("beacon", "anthropic_api_key").strip()
    if not key.isascii() or not key.isprintable() or not key.startswith("sk-ant-"):
        # Usually the secret was stored with the wrong encoding (e.g. UTF-16 from a
        # Windows terminal paste), which puts NUL bytes in the HTTP header.
        raise ValueError(
            "Secret beacon/anthropic_api_key is malformed (non-printable characters or no "
            "'sk-ant-' prefix). Re-store it with: databricks secrets put-secret beacon "
            "anthropic_api_key --string-value <key>"
        )
    return key


def diagnose(failures) -> tuple[str, int, int]:
    client = anthropic.Anthropic(api_key=api_key())
    listing = "\n".join(
        f"- {r['check_name']} ({r['severity']}, {r['status']}) on {r['table_name']}: {r['message']}\n"
        f"  observed: {r['observed']}"
        for r in failures
    )
    messages = [{
        "role": "user",
        "content": f"Data quality run {args.run_id} has {len(failures)} non-passing checks:\n\n"
                   f"{listing}\n\nInvestigate and write the report.",
    }]
    tokens_in = tokens_out = 0
    # Server-side refusal fallback exists for Opus/Sonnet 5.5 but not Haiku 5.5,
    # which rejects the parameter.
    fallback = (
        {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
        if C.MODEL in ("claude-opus-5-5", "claude-sonnet-5-5") else {}
    )

    for _ in range(C.MAX_AGENT_TURNS):
        response = client.beta.messages.create(
            model=C.MODEL,
            max_tokens=16000,
            system=SYSTEM,
            tools=TOOLS,
            messages=messages,
            output_config={"effort": C.EFFORT},
            # Each turn resends the whole conversation; caching bills the repeated
            # prefix at the cache-read rate instead of full input price.
            cache_control={"type": "ephemeral"},
            **fallback,
        )
        u = response.usage
        # With prompt caching, input_tokens covers only the uncached part.
        tokens_in += (u.input_tokens + (u.cache_creation_input_tokens or 0)
                      + (u.cache_read_input_tokens or 0))
        tokens_out += u.output_tokens
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "refusal":
            return "Diagnosis unavailable: the model declined this request.", tokens_in, tokens_out
        if response.stop_reason != "tool_use":
            text = "\n".join(b.text for b in response.content if b.type == "text").strip()
            return text or f"(no report; stop_reason={response.stop_reason})", tokens_in, tokens_out

        results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            print(f"tool: {block.name} {json.dumps(block.input)[:300]}")
            try:
                content, is_error = HANDLERS[block.name](block.input), False
            except Exception as e:  # noqa: BLE001 — errors go back to the model, not up the stack
                content, is_error = f"{type(e).__name__}: {e}", True
            results.append({"type": "tool_result", "tool_use_id": block.id,
                            "content": scrub(content), "is_error": is_error})
        messages.append({"role": "user", "content": results})

    return f"Diagnosis incomplete: hit the {C.MAX_AGENT_TURNS}-turn limit.", tokens_in, tokens_out


def save_incident(failures, report: str, tokens_in: int, tokens_out: int):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {C.INCIDENTS_TABLE} (
            run_id STRING, created_ts TIMESTAMP, model STRING, failed_checks ARRAY<STRING>,
            report STRING, input_tokens BIGINT, output_tokens BIGINT
        ) USING DELTA
    """)
    df = spark.createDataFrame(
        [(args.run_id, C.MODEL, [r["check_name"] for r in failures], report, tokens_in, tokens_out)],
        "run_id STRING, model STRING, failed_checks ARRAY<STRING>, report STRING, "
        "input_tokens BIGINT, output_tokens BIGINT",
    )
    (df.selectExpr("run_id", "current_timestamp() AS created_ts", "model", "failed_checks",
                   "report", "input_tokens", "output_tokens")
       .write.mode("append").saveAsTable(C.INCIDENTS_TABLE))


def already_diagnosed() -> bool:
    if not spark.catalog.tableExists(C.INCIDENTS_TABLE):
        return False
    return spark.sql(
        f"SELECT 1 FROM {C.INCIDENTS_TABLE} WHERE run_id = '{args.run_id}' LIMIT 1"
    ).count() > 0


def run():
    failures = failed_results()
    if not failures:
        print("All checks passed; nothing to diagnose.")
        return

    if already_diagnosed():
        # Serverless retries this task automatically after it fails on purpose
        # (max_retries: 0 isn't kept). Don't pay for the same diagnosis twice.
        print(f"Run {args.run_id} already has a diagnosis in {C.INCIDENTS_TABLE}; skipping.")
    else:
        diagnose_and_save(failures)

    errors = [r["check_name"] for r in failures if r["severity"] == "error"]
    if errors:
        raise RuntimeError(
            f"Data quality errors: {', '.join(errors)}. "
            f"Diagnosis saved to {C.INCIDENTS_TABLE} (run_id {args.run_id})."
        )


def diagnose_and_save(failures):
    try:
        report, tokens_in, tokens_out = diagnose(failures)
    except Exception as e:  # noqa: BLE001 — still record the failures and fail the job below
        cause = f" (cause: {type(e.__cause__).__name__}: {e.__cause__})" if e.__cause__ else ""
        report, tokens_in, tokens_out = f"Diagnosis unavailable: {type(e).__name__}: {e}{cause}", 0, 0
        if isinstance(e, anthropic.APIConnectionError) and "Connect" in type(e.__cause__).__name__:
            report += (
                "\n\nThe job couldn't reach api.anthropic.com. On Databricks Free Edition, serverless "
                "outbound internet is limited to trusted domains until the account is verified."
            )
    report = scrub(report)
    save_incident(failures, report, tokens_in, tokens_out)
    print(report)
    print(f"\n(tokens: {tokens_in} in / {tokens_out} out)")


run()
