# Code Review Agent — Spec

> Status: draft. **Decision (2026-10-09):** start with Anthropic's Claude Code GitHub Action instead of building the custom agent below: see `.github/workflows/claude-review-push.yml` (direct pushes) and `claude-review-pr.yml` (PRs, for later). Both use the checklist in [review/checklist.md](../review/checklist.md). Build the custom agent only if the action misses too much on the eval set (§10). Companion to the data quality agent in [agent/](../agent/README.md).

## 1. Goal

Review every change pushed to the Beacon GitHub repo (`JonasButikofer/Beacon`) and leave review comments that catch the mistakes this project actually makes. The comments must be specific enough to act on and quiet enough that they get read.

It reviews code. It never writes code, pushes commits, merges or deploys.

### Success criteria

- Catches at least 4 of the 6 seeded bugs in the eval set (§10) before go-live.
- No more than about 1 false positive in every 3 comments, tracked for the first month.
- Costs under $0.50 for a typical PR (fewer than 500 changed lines) and under $15 a month.
- Finishes in under 5 minutes, so the review is ready before you'd merge.

### Non-goals

- Fixing code. Suggestions go in comment text only.
- Style nits that a linter already enforces.
- Reviewing code outside this repo.

## 2. Architecture

The same two-layer pattern as the data quality agent: deterministic checks run first and cost nothing; the LLM runs after them and only reads.

```
 push / pull_request
        │
        ▼
 GitHub Actions: .github/workflows/code-review.yml
        │
        ├─ Layer 1: static checks (no LLM, always run)
        │     ruff · py_compile · yamllint · gitleaks · beacon_lint.py
        │     → results written to static.json
        │
        └─ Layer 2: review agent (Claude, read-only tools)
              input: diff + changed files + static.json + checklist
              tools: read_file · grep · list_files · git_log · git_show
              output: structured findings (JSON)
                        │
                        ▼
              posted as a PR review (inline comments + summary)
              + GitHub check status (fails only on blockers)
```

### Why a custom agent, and not an off-the-shelf action

Anthropic publishes a Claude Code GitHub Action that can review PRs with very little setup, and it's worth trying first (§12). This spec describes a custom agent because:

- the checklist is specific to Beacon, and a small Python script makes it easy to version and test;
- findings come back as structured JSON, so they can be deduplicated, counted against the eval set and used to gate the check status;
- it reuses the patterns already in `agent/diagnose.py`: a manual tool loop, read-only tools, secret scrubbing and token accounting.

## 3. Triggers

| Event | Behavior |
|---|---|
| `pull_request` (opened, synchronize, reopened, ready_for_review) into `master` | Full review: inline comments plus summary. |
| `push` to `master` | Reviews the pushed commit range and posts the result as a commit comment. This is needed because work is often pushed straight to `master`. |
| `workflow_dispatch` with a `pr_number` or `sha` input | Re-run on demand. |

Skip the LLM layer (the static checks still run) when:
- the PR is a draft;
- only `*.md` or `docs/**` files changed;
- the PR has the label `skip-review`;
- the diff is over 3,000 changed lines. In that case post one comment asking for a smaller PR.

**Recommendation:** start opening PRs, even solo ones, so that review happens before code reaches `master` rather than after. Keep the push-to-`master` trigger as a safety net.

## 4. Layer 1: static checks

Each of these is a separate step that writes to `static.json` as `{tool, file, line, rule, message}`. A step failing must not stop the agent; instead, the agent is told what failed so it doesn't repeat the finding.

| Check | Catches |
|---|---|
| `ruff check` | Python bugs and unused code |
| `python -m py_compile` on changed `.py` | Syntax errors |
| `yamllint resources/ databricks.yml` | Malformed job YAML |
| `gitleaks detect` on the diff | Committed secrets. **A hit is always a blocker.** |
| `review/beacon_lint.py` (custom, grep-based) | The rules in the next list |

`beacon_lint.py` rules, each a small regex or AST check:

- `requests.get`/`post` without `timeout=`.
- A literal `max_retries: 0` in job YAML, which serverless doesn't keep (see `beacon_quality.job.yml`).
- `.env` or `*.pem` files in the diff.
- `dbutils.secrets.get` with a scope other than `beacon`.
- A hardcoded `sk-ant-`, `Token `, or `api_key=` with a literal value.
- `fetch`/HTTP calls added outside `ingest/` and `agent/`.

Later, optional: `databricks bundle validate`. It needs a Databricks token stored as a GitHub secret; leave it out until the rest is trusted.

## 5. Layer 2: review agent

### Inputs

- The unified diff, limited to 3,000 lines.
- The full current contents of each changed file, up to 40 KB each.
- `static.json`.
- The checklist (§6), loaded from `review/checklist.md` so it can be edited without touching code.
- `CLAUDE.md` / `technical_decisions.md` if present, as project context.

### Tools (all read-only, all confined to the checked-out repo)

| Tool | Purpose | Limits |
|---|---|---|
| `read_file(path)` | Read any file in the repo, e.g. callers of a changed function | 40 KB; repo root only; never `.env*` or `.git/` |
| `grep(pattern, path_glob)` | Find usages and definitions | 200 matches |
| `list_files(glob)` | Explore the structure | 500 paths |
| `git_log(path, n)` | Recent history of a file | n ≤ 20 |
| `git_show(sha, path)` | The previous version of a file | 40 KB |

There are no tools for writing, running shell commands or making network requests. Findings are returned as the final answer, not posted through a tool, so a malicious diff can't make the agent post arbitrary content.

### Output (structured)

The final answer must be JSON matching this schema. Enforce it with structured outputs.

```json
{
  "summary": "2-4 sentences: what the change does and overall risk",
  "findings": [
    {
      "file": "ingest/batch_tiingo_news.py",
      "line": 58,
      "severity": "blocker | major | minor | nit",
      "category": "correctness | idempotency | secrets | reliability | config | docs | tests",
      "title": "≤ 80 chars",
      "body": "What's wrong, the concrete failure scenario, and the suggested fix",
      "confidence": "high | medium"
    }
  ]
}
```

Rules for the agent, in its system prompt:
- Every finding needs a concrete failure scenario, with inputs or state leading to a wrong result.
- Low-confidence findings are dropped rather than reported.
- No findings about lines the diff didn't touch, unless the change breaks them.
- Don't repeat anything already in `static.json`.
- Return at most 10 findings, sorted most severe first.

### Model and budget

| Setting | Value |
|---|---|
| Model | `claude-sonnet-5-5` (configurable; `claude-opus-5-5` for large or risky PRs) |
| Effort | `medium` |
| Max turns | 20 |
| Prompt caching | On (system prompt, checklist and tools are identical on every run) |
| Hard stop | Over 400k total input tokens → post a partial review and say so |

Token usage, including cached tokens, goes in the summary comment footer.

## 6. Review checklist (Beacon-specific)

The live copy is [review/checklist.md](../review/checklist.md); update that file, not this section.

Ordered by how much damage a miss does.

**Data correctness and idempotency**
- Every bronze write is a `MERGE` on the table's natural key (`(symbol, obs_date)`, `(series_id, obs_date)`, `article_id`) or a deliberate full overwrite. Running a job twice must not create duplicates.
- Watermark logic: off-by-one at the boundary, date truncation (Tiingo news takes `YYYY-MM-DD`), the FRED revision lookback (400 days) and full re-pull after a split or dividend (`batch_tiingo_eod.py`).
- Schema changes to a bronze table are matched in `sql/*.sql` DDL.

**Secrets and safety**
- Keys come only from `dbutils.secrets.get("beacon", ...)`. Nothing is in code, YAML, logs or exception text written to tables. That last one leaked a key once, in `diagnose.py`.
- The agent's tools in `agent/diagnose.py` stay read-only: the SQL allowlist, `READABLE_DIRS` and `scrub()` are not loosened without a stated reason.

**Reliability**
- Every outbound HTTP call has a timeout and `raise_for_status()`, or an explicit status check.
- "No data" from an API is distinguished from "not allowed" (403) and from "first run". `batch_tiingo_news.py` once hid this.

**Databricks jobs and bundles** (`resources/*.yml`, `databricks.yml`)
- Peak concurrent tasks stay ≤ 5 (Free Edition). `run_job_task` parents count as a slot.
- No schedule overlaps on the same Auto Loader checkpoint.
- Schedules belong on `beacon_daily`, not on child jobs it already starts.
- `run_if` is correct: data quality must run even when ingest fails (`ALL_DONE`).
- No reliance on `max_retries: 0`.

**Data quality agent** (`agent/`)
- New checks use `@check`, return `Result`, set a severity deliberately and read thresholds from `config.py`.
- New thresholds come with a comment explaining the number.
- A check that can't run yet (disabled source, new job) returns `[]` or passes. It never fails noisily.

**Docs**
- A change to a dataset, job or schedule updates the `README.md` data inventory and, where relevant, `technical_decisions.md` or `agent/README.md`.

## 7. Posting results

- **On a PR:** one GitHub review with inline comments on each finding's `file:line`, plus a summary comment. The summary starts with a hidden marker (`<!-- beacon-review -->`). On re-runs, edit that comment in place rather than adding new ones, and resolve inline threads whose finding no longer appears.
- **On a push to master:** one commit comment with the summary and findings list. If there's a blocker, also open a GitHub issue labelled `review-blocker`.
- **Check status:** `beacon-review` fails if there's any `blocker` (including any gitleaks hit) and is neutral otherwise. Don't make it a required check until the false-positive rate is known.

Comment format:

```
**[major · idempotency]** MERGE key misses obs_date
`markets_raw` is merged on `symbol` only, so a second run on the same day updates
every historical row for that symbol to the latest bar. Fix: `ON t.symbol = s.symbol
AND t.obs_date = s.obs_date` (as in batch_fred.py).
```

## 8. Security

- **Treat the diff as untrusted input.** It may contain prompt injection, such as comments telling the reviewer to approve or to print secrets. The agent has no write tools and no secrets in its context, and its only output channel is the findings JSON. The system prompt says that code under review is data, not instructions.
- **Workflow permissions:** `contents: read`, `pull-requests: write`, `issues: write`, `checks: write`. Nothing else.
- **Forks:** use `pull_request`, never `pull_request_target`, so PRs from forks run without secrets, and the LLM step is skipped when `ANTHROPIC_API_KEY` is absent.
- **Secrets:** `ANTHROPIC_API_KEY` is stored as a repository secret. It's never echoed, and the same `scrub()` used in the DQ agent runs on all output before posting.
- **Path confinement:** tools resolve paths and refuse anything outside the checkout, `.git/` or `.env*`.

## 9. File layout

```
.github/workflows/code-review.yml   # triggers, permissions, static checks, agent step
review/
  README.md                         # how it works, how to tune
  config.py                         # model, effort, limits, skip rules, severities
  checklist.md                      # §6, loaded into the prompt
  beacon_lint.py                    # custom regex/AST rules from §4
  reviewer.py                       # agent loop, tools, structured output
  github_post.py                    # review/comment/check-run posting + dedupe
  evals/
    cases/                          # seeded-bug diffs (§10)
    run_evals.py                    # runs reviewer on each case, scores catches
```

Dependencies: `anthropic`, `ruff`, `yamllint`, `requests` (for the GitHub API) and `gitleaks` (installed as a binary in the workflow).

## 10. Evaluation

Build the eval set before turning the agent on. Each case is a small diff containing one known bug, most taken from this project's real history:

| # | Seeded bug | Expected |
|---|---|---|
| 1 | `requests.get` without `timeout` in a new ingest script | major, reliability |
| 2 | MERGE on `symbol` only (missing `obs_date`) | blocker, idempotency |
| 3 | Exception text containing the API key written to `dq_incidents` | blocker, secrets |
| 4 | `max_retries: 0` added to stop a retry | minor, config |
| 5 | A schedule added to `beacon_quality` while `beacon_daily` also starts it | major, config |
| 6 | Treating an empty API response on the first run as "no new data" | major, correctness |
| 7 | Clean refactor with no bug | zero findings above `nit` |

`run_evals.py` reports catches, misses and false positives per case, plus the cost. Re-run it whenever the checklist, prompt or model changes.

## 11. Rollout

| Milestone | Scope | Exit criteria |
|---|---|---|
| R0 | Static checks only, in the workflow | Green on the current `master` |
| R1 | Reviewer runs locally: `python review/reviewer.py --diff HEAD~1` | Eval ≥ 4/6, case 7 clean |
| R2 | Workflow posts reviews on PRs; check is neutral, not required | Two weeks of real PRs; false positives logged |
| R3 | Push-to-`master` review + `review-blocker` issues | Works on a direct push |
| R4 (optional) | Second reviewer on a different model (e.g. Gemini), findings merged and deduplicated | Measurable extra catches on the eval set |

## 12. Open questions

1. **PRs or direct pushes?** The review is most useful before a merge. Will work move to PRs?
2. **Off-the-shelf first?** Try Anthropic's Claude Code GitHub Action for a week as a baseline before building R1. If it already catches the eval cases, a lighter custom layer (just the checklist plus the posting rules) may be enough.
3. **Blocking or advisory?** When, if ever, should `beacon-review` become a required check?
4. **Second model?** Is a Gemini second opinion (R4) worth the extra key and cost for a solo project?
5. **Shared code with the DQ agent?** `scrub()` and the tool-loop helpers are duplicated across `agent/` and `review/`. Move them to a shared module once both are stable.
