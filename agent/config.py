# agent/config.py
# Shared settings for the data quality job (run_checks.py) and the diagnosis
# agent (diagnose.py). Tune thresholds here, not inside the checks.

OPS_SCHEMA = "beacon.ops"
RESULTS_TABLE = f"{OPS_SCHEMA}.dq_results"
INCIDENTS_TABLE = f"{OPS_SCHEMA}.dq_incidents"

# Same 9 ETFs as the ingest jobs.
WATCHLIST = ["SPY", "QQQ", "DIA", "IWM", "XLF", "XLE", "XLK", "XLU", "TLT"]

# markets_raw: max calendar days between today and the latest obs_date.
# 4 covers a weekend plus one market holiday (e.g. Tuesday after Memorial Day).
MARKETS_MAX_LAG_DAYS = 4
# Day-over-day adj_close move that's suspicious when there's no split that day.
# Broad ETFs rarely move 15% in a day; a jump this size usually means the
# adjusted history is on two different bases.
MARKETS_MAX_DAILY_MOVE = 0.15
MARKETS_SANITY_WINDOW_DAYS = 30

# macro_raw: max calendar days between today and each series' latest obs_date.
# FRED dates monthly observations to the 1st of the month and publishes them
# weeks later, so "fresh" monthly data is still 45-75 days old. GDP is dated to
# the quarter start and first printed ~30 days after the quarter ends.
FRED_MAX_LAG_DAYS = {
    "DGS10": 7,
    "DGS2": 7,
    "T10Y2Y": 7,
    "CPIAUCSL": 100,
    "UNRATE": 100,
    "FEDFUNDS": 100,
    "UMCSENT": 100,
    "GDP": 200,
}

# news_raw: hours since the newest published article. Weekends are slow.
# Disabled while ingest_tiingo_news is off (free Tiingo plan has no news access).
NEWS_CHECK_ENABLED = False
NEWS_MAX_LAG_HOURS = 72

# quotes_raw: minimum trades expected on the last trading session.
# Set QUOTES_CHECK_ENABLED = False while beacon_stream isn't running daily
# (e.g. schedules paused in the dev target), or this fails every day.
QUOTES_CHECK_ENABLED = True
QUOTES_MIN_ROWS = 1000

# tickers_universe: full overwrite weekly, ~100k rows.
UNIVERSE_MIN_ROWS = 50_000
UNIVERSE_MAX_AGE_DAYS = 9
TICKERS_MAX_AGE_DAYS = 2

# Bundle target this run belongs to (dev | prod); run_checks.py sets it from
# --target so the job_runs check only looks at jobs from the same target.
TARGET = "dev"

# Jobs whose latest run must have succeeded and be no older than max_age_hours.
JOB_CHECKS = {
    "beacon_batch": 30,
    "beacon_universe_weekly": 8 * 24,
}

# Diagnosis agent. Options, most to least capable (input / output $ per 1M tokens):
#   "claude-opus-5-5"    $4 / $20
#   "claude-sonnet-5-5"  $2 / $10
#   "claude-haiku-5-5"   $0.10 / $0.50 (prompts under 100K tokens; $0.50 / $2.50 above)
# EFFORT ("low" | "medium" | "high") trades depth of investigation for tokens.
MODEL = "claude-sonnet-5-5"
EFFORT = "medium"
MAX_AGENT_TURNS = 15
SQL_ROW_LIMIT = 200
# Only files under these bundle folders can be read by the agent.
READABLE_DIRS = ("ingest", "resources", "sql", "agent", "dbt")
