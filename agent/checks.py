# agent/checks.py
# Rule-based data quality checks over beacon.bronze. No LLM here: each check is
# a plain SQL query plus a threshold from config.py, so results are cheap and
# repeatable. run_checks.py runs them all and writes one row per result.
#
# Each check returns a list of Result. `severity` is "error" (fails the job) or
# "warn" (reported and diagnosed, but the job still succeeds).
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from databricks.sdk.runtime import spark

import config as C

MARKETS = "beacon.bronze.markets_raw"
MACRO = "beacon.bronze.macro_raw"
NEWS = "beacon.bronze.news_raw"
QUOTES = "beacon.bronze.quotes_raw"
TICKERS = "beacon.bronze.tickers_raw"
UNIVERSE = "beacon.bronze.tickers_universe"

CHECKS = []


def check(fn):
    CHECKS.append(fn)
    return fn


@dataclass
class Result:
    check_name: str
    table_name: str
    severity: str
    passed: bool
    message: str
    observed: dict = field(default_factory=dict)
    crashed: bool = False

    @property
    def status(self) -> str:
        if self.crashed:
            return "error"
        return "pass" if self.passed else "fail"


def one(sql: str):
    return spark.sql(sql).collect()[0]


def duplicate_keys(table: str, keys: str) -> int:
    return one(
        f"SELECT count(*) AS n FROM (SELECT {keys} FROM {table} GROUP BY {keys} HAVING count(*) > 1)"
    )["n"]


# --- markets_raw -----------------------------------------------------------


@check
def markets_freshness():
    rows = spark.sql(
        f"SELECT symbol, datediff(current_date(), max(obs_date)) AS lag FROM {MARKETS} GROUP BY symbol"
    ).collect()
    lags = {r["symbol"]: r["lag"] for r in rows}
    missing = [s for s in C.WATCHLIST if s not in lags]
    stale = {s: d for s, d in lags.items() if d > C.MARKETS_MAX_LAG_DAYS}
    ok = not missing and not stale
    msg = "All watchlist symbols are current." if ok else (
        f"Missing symbols: {missing}. Stale (days behind): {stale}."
    )
    return [Result("markets_freshness", MARKETS, "error", ok, msg, {"lag_days": lags, "missing": missing})]


@check
def markets_duplicates():
    n = duplicate_keys(MARKETS, "symbol, obs_date")
    return [Result("markets_duplicates", MARKETS, "error", n == 0,
                   f"{n} duplicate (symbol, obs_date) keys.", {"duplicate_keys": n})]


@check
def markets_ohlc_sanity():
    r = one(f"""
        SELECT count(*) AS n FROM {MARKETS}
        WHERE obs_date >= date_sub(current_date(), {C.MARKETS_SANITY_WINDOW_DAYS})
          AND (close IS NULL OR low <= 0 OR high < low
               OR close > high OR close < low OR open > high OR open < low
               OR volume < 0)
    """)
    n = r["n"]
    return [Result("markets_ohlc_sanity", MARKETS, "error", n == 0,
                   f"{n} rows in the last {C.MARKETS_SANITY_WINDOW_DAYS} days with impossible OHLC values.",
                   {"bad_rows": n})]


@check
def markets_adj_jump():
    rows = spark.sql(f"""
        SELECT symbol, obs_date, prev_adj_close, adj_close FROM (
            SELECT symbol, obs_date, adj_close, split_factor,
                   lag(adj_close) OVER (PARTITION BY symbol ORDER BY obs_date) AS prev_adj_close
            FROM {MARKETS}
            WHERE obs_date >= date_sub(current_date(), {C.MARKETS_SANITY_WINDOW_DAYS + 10})
        )
        WHERE obs_date >= date_sub(current_date(), {C.MARKETS_SANITY_WINDOW_DAYS})
          AND prev_adj_close > 0
          AND abs(adj_close / prev_adj_close - 1) > {C.MARKETS_MAX_DAILY_MOVE}
          AND split_factor = 1
    """).collect()
    jumps = [
        {"symbol": r["symbol"], "obs_date": str(r["obs_date"]),
         "prev_adj_close": r["prev_adj_close"], "adj_close": r["adj_close"]}
        for r in rows
    ]
    return [Result("markets_adj_jump", MARKETS, "warn", not jumps,
                   f"{len(jumps)} day-over-day adj_close moves over {C.MARKETS_MAX_DAILY_MOVE:.0%} with no split.",
                   {"jumps": jumps[:20]})]


# --- macro_raw -------------------------------------------------------------


@check
def macro_freshness():
    rows = spark.sql(
        f"SELECT series_id, datediff(current_date(), max(obs_date)) AS lag FROM {MACRO} GROUP BY series_id"
    ).collect()
    lags = {r["series_id"]: r["lag"] for r in rows}
    missing = [s for s in C.FRED_MAX_LAG_DAYS if s not in lags]
    stale = {s: d for s, d in lags.items() if s in C.FRED_MAX_LAG_DAYS and d > C.FRED_MAX_LAG_DAYS[s]}
    ok = not missing and not stale
    msg = "All FRED series are within their freshness window." if ok else (
        f"Missing series: {missing}. Stale (days behind, limit): "
        + str({s: (d, C.FRED_MAX_LAG_DAYS[s]) for s, d in stale.items()})
    )
    return [Result("macro_freshness", MACRO, "error", ok, msg, {"lag_days": lags, "missing": missing})]


@check
def macro_integrity():
    dups = duplicate_keys(MACRO, "series_id, obs_date")
    nulls = one(f"SELECT count(*) AS n FROM {MACRO} WHERE value IS NULL")["n"]
    ok = dups == 0 and nulls == 0
    return [Result("macro_integrity", MACRO, "error", ok,
                   f"{dups} duplicate (series_id, obs_date) keys, {nulls} null values.",
                   {"duplicate_keys": dups, "null_values": nulls})]


# --- news_raw --------------------------------------------------------------


@check
def news_freshness():
    if not C.NEWS_CHECK_ENABLED:
        return []
    r = one(f"""
        SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(published_date))) / 3600 AS lag_h,
               count_if(published_date >= current_timestamp() - INTERVAL 72 HOURS) AS recent
        FROM {NEWS}
    """)
    lag = r["lag_h"]
    ok = lag is not None and lag <= C.NEWS_MAX_LAG_HOURS
    lag_txt = "no rows" if lag is None else f"{lag:.0f}h"
    return [Result("news_freshness", NEWS, "error", ok,
                   f"Newest article is {lag_txt} old (limit {C.NEWS_MAX_LAG_HOURS}h); {r['recent']} articles in the last 72h.",
                   {"lag_hours": lag, "articles_72h": r["recent"]})]


@check
def news_duplicates():
    if not C.NEWS_CHECK_ENABLED:
        return []
    n = duplicate_keys(NEWS, "article_id")
    return [Result("news_duplicates", NEWS, "error", n == 0,
                   f"{n} duplicate article_id values.", {"duplicate_keys": n})]


# --- quotes_raw ------------------------------------------------------------


@check
def quotes_last_session():
    if not C.QUOTES_CHECK_ENABLED:
        return []
    # The latest EOD bar is a reliable "last trading day" without a holiday calendar.
    session = one(f"SELECT max(obs_date) AS d FROM {MARKETS}")["d"]
    r = one(f"""
        SELECT count(*) AS n, collect_set(symbol) AS symbols
        FROM {QUOTES} WHERE to_date(trade_ts) = DATE'{session}'
    """)
    missing = sorted(set(C.WATCHLIST) - set(r["symbols"] or []))
    ok = r["n"] >= C.QUOTES_MIN_ROWS and not missing
    return [Result("quotes_last_session", QUOTES, "warn", ok,
                   f"{r['n']} trades on {session} (min {C.QUOTES_MIN_ROWS}); missing symbols: {missing}.",
                   {"session": str(session), "rows": r["n"], "missing": missing})]


# --- reference tables ------------------------------------------------------


@check
def tickers_refresh():
    r = one(f"""
        SELECT count(*) AS n, datediff(current_date(), max(to_date(_ingested_at))) AS age
        FROM {TICKERS}
    """)
    ok = r["n"] == len(C.WATCHLIST) and r["age"] is not None and r["age"] <= C.TICKERS_MAX_AGE_DAYS
    return [Result("tickers_refresh", TICKERS, "error", ok,
                   f"{r['n']} rows (expected {len(C.WATCHLIST)}), last loaded {r['age']} days ago.",
                   {"rows": r["n"], "age_days": r["age"]})]


@check
def universe_refresh():
    r = one(f"""
        SELECT count(*) AS n, datediff(current_date(), max(to_date(_ingested_at))) AS age
        FROM {UNIVERSE}
    """)
    ok = r["n"] >= C.UNIVERSE_MIN_ROWS and r["age"] is not None and r["age"] <= C.UNIVERSE_MAX_AGE_DAYS
    return [Result("universe_refresh", UNIVERSE, "error", ok,
                   f"{r['n']} rows (min {C.UNIVERSE_MIN_ROWS}), last loaded {r['age']} days ago.",
                   {"rows": r["n"], "age_days": r["age"]})]


# --- job runs --------------------------------------------------------------


@check
def job_runs():
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    jobs = list(w.jobs.list())
    now = datetime.now(timezone.utc)
    results = []
    for suffix, max_age_h in C.JOB_CHECKS.items():
        # Only the jobs deployed to the same bundle target as this check: in dev,
        # "[dev <user>] beacon_batch"; in prod, exactly "beacon_batch".
        if C.TARGET == "prod":
            matches = [j for j in jobs if j.settings and j.settings.name == suffix]
        else:
            matches = [j for j in jobs if j.settings and j.settings.name.startswith("[dev ")
                       and j.settings.name.endswith(f"] {suffix}")]
        if not matches:
            results.append(Result(f"job_runs:{suffix}", suffix, "error", False,
                                  f"No {C.TARGET} job named {suffix}.", {}))
            continue
        for j in matches:
            runs = list(w.jobs.list_runs(job_id=j.job_id, completed_only=True, limit=1))
            if not runs:
                # A freshly deployed job hasn't failed; it just hasn't been due yet
                # (e.g. a weekly job deployed mid-week). Only flag it once it has
                # existed longer than its allowed gap between runs.
                created = datetime.fromtimestamp((j.created_time or 0) / 1000, timezone.utc)
                exists_h = (now - created) / timedelta(hours=1)
                ok = exists_h <= max_age_h
                msg = (f"New job (deployed {exists_h:.0f}h ago); first run not due yet (limit {max_age_h}h)."
                       if ok else f"Job has never completed a run, though it was deployed {exists_h:.0f}h ago.")
                results.append(Result(f"job_runs:{suffix}", j.settings.name, "error", ok, msg,
                                      {"job_id": j.job_id, "created_hours_ago": round(exists_h, 1)}))
                continue
            run = runs[0]
            state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
            started = datetime.fromtimestamp(run.start_time / 1000, timezone.utc)
            age_h = (now - started) / timedelta(hours=1)
            ok = state == "SUCCESS" and age_h <= max_age_h
            results.append(Result(
                f"job_runs:{suffix}", j.settings.name, "error", ok,
                f"Latest run {run.run_id}: {state}, started {age_h:.0f}h ago (limit {max_age_h}h).",
                {"job_id": j.job_id, "run_id": run.run_id, "result_state": state,
                 "age_hours": round(age_h, 1), "run_page_url": run.run_page_url},
            ))
    return results


def run_all():
    """Run every check. A check that crashes becomes an `error`-status result."""
    out = []
    for fn in CHECKS:
        try:
            out.extend(fn())
        except Exception as e:  # noqa: BLE001 — a broken check must not hide the others
            out.append(Result(fn.__name__, "", "error", False,
                              f"Check crashed: {type(e).__name__}: {e}", crashed=True))
    return out


def to_row(r: Result, run_id: str):
    return (run_id, r.check_name, r.table_name, r.severity, r.status, r.message,
            json.dumps(r.observed, default=str))
