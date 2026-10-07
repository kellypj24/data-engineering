---
name: backfill-runbook
description: Use when reprocessing history in a dbt project — "backfill X for last quarter", "rerun fct_y from January", "the numbers were wrong for these dates, rebuild them", "should I full-refresh this", "restate a date range". Scopes the window and blast radius, protects history, runs bounded batches with parity checks, and writes up what was done.
argument-hint: "<model> <start-date> <end-date>   e.g. fct_daily_order_revenue 2026-01-01 2026-03-31"
---

# Run a backfill

Runs **in the downstream project**. Locate the dbt root first:

```bash
find . -name dbt_project.yml -not -path "*/dbt_packages/*" -not -path "*/.venv/*"
```

A backfill rewrites history that other people have already read. Do it in
this order, and do not skip the backup or the parity check because the change
"is small".

## Step 1 — scope

Write down: the models, the date window, and **why** (the bug or late data
being corrected). Then the blast radius, which is everything downstream that
will change:

```bash
uv run dbt ls --select fct_daily_order_revenue+ --resource-type model --resource-type exposure
```

Exposures in that list are files or dashboards other people receive. Tell their
owners before you start, not after.

## Step 2 — decide how, never `--full-refresh` by default

| Model | How to backfill |
|-------|-----------------|
| view or table | `dbt run` rebuilds it all; no batching needed |
| incremental with window vars (`backfill_start` / `backfill_end`) | bounded batches, step 4 |
| incremental without them | add them first (below), then batch |
| durable fact (`full_refresh=false`) | bounded batches only. Full refresh is refused by design: it would drop every day the source no longer holds |

**Check source retention before any rebuild.** If the source keeps 90 days and
the table keeps years, a full rebuild silently deletes the difference. Compare
`MIN(<date>)` in the source with `MIN(<date>)` in the table. If the table goes
further back, only a bounded window is safe.

To give an incremental model a backfill window, follow
`fct_daily_order_revenue` in the toolkit: both vars or neither, start ≤ end,
and the window replaces the incremental filter.

```sql
{% if is_incremental() and var('backfill_start', none) is not none %}
    WHERE CAST(created_at AS DATE)
        BETWEEN CAST('{{ var("backfill_start") }}' AS DATE) AND CAST('{{ var("backfill_end") }}' AS DATE)
{% elif is_incremental() %}
    ... the normal incremental filter ...
{% endif %}
```

This only replaces rows cleanly when the model's `unique_key` / strategy
deletes what it reinserts (`delete+insert` on the date, or `merge`). With
`append`, a backfill duplicates rows.

Surrogate keys that need recomputing in place (not a date window) are a
different operation: `backfill_surrogate_keys` (`macros/utils/`), dry run by
default.

## Step 3 — back up, then baseline

Keep a copy you can diff against and restore from:

```sql
CREATE TABLE marts.fct_daily_order_revenue__backup_20260115 AS
SELECT * FROM marts.fct_daily_order_revenue;            -- Snowflake: ... CLONE ... (zero-copy)
```

Record the baseline per period for the window:

```bash
uv run dbt show --inline "SELECT order_date, COUNT(*) AS n, SUM(revenue) AS revenue
  FROM {{ ref('fct_daily_order_revenue') }}
  WHERE order_date BETWEEN '2026-01-01' AND '2026-03-31' GROUP BY 1 ORDER BY 1" --limit 200
```

## Step 4 — dry run, then bounded batches

Dry run first: compile the batch and read the WHERE clause it will run.

```bash
uv run dbt compile --select fct_daily_order_revenue \
  --vars '{backfill_start: 2026-01-01, backfill_end: 2026-01-07}'
```

Then run one batch at a time, a week or a month depending on volume, oldest
first:

```bash
uv run dbt build --select fct_daily_order_revenue \
  --vars '{backfill_start: 2026-01-01, backfill_end: 2026-01-07}'
```

`build`, not `run`: each batch's data tests (including a control total) run
against it immediately. Stop at the first failing batch.

## Step 5 — parity per batch

After each batch, compare against the backup for that window:

- **Counts and sums per period**: re-run the step 3 baseline query and diff it.
  Every period outside the intended correction must match exactly.
- **Rows that changed**: the set difference both ways.

```sql
SELECT 'after' AS side, * FROM (SELECT * FROM marts.fct_daily_order_revenue
  WHERE order_date BETWEEN '2026-01-01' AND '2026-01-07'
  EXCEPT SELECT * FROM marts.fct_daily_order_revenue__backup_20260115
  WHERE order_date BETWEEN '2026-01-01' AND '2026-01-07') AS a
UNION ALL
SELECT 'before', * FROM (SELECT * FROM marts.fct_daily_order_revenue__backup_20260115
  WHERE order_date BETWEEN '2026-01-01' AND '2026-01-07'
  EXCEPT SELECT * FROM marts.fct_daily_order_revenue
  WHERE order_date BETWEEN '2026-01-01' AND '2026-01-07') AS b
```

Every difference must be explained by the reason in step 1. An unexplained one
stops the backfill. For whole-table captures, `tooling/release-diff` does the
same with checked arithmetic.

## Step 6 — rebuild downstream, then write it up

Rebuild the blast radius from step 1 (`dbt build --select fct_daily_order_revenue+`
with the same window vars where downstream models accept them). Then record:
the reason, models, window, batches run, parity results, unexplained
differences (should be none), who was told, and when the backup table can be
dropped (keep it at least one full reporting cycle).

## Verify

```bash
uv run dbt build --select fct_daily_order_revenue+   # the model and everything downstream, tests included
```

It exits 0, and the step 5 diff for the whole window lists only the intended
corrections.

## Common mistakes

- **`--full-refresh` on a durable fact.** It rebuilds from what the source
  still holds and drops older history with no error. The toolkit's durable
  facts set `full_refresh=false` so the flag is ignored. Do the same.
- **Backfilling an `append` incremental.** Every batch duplicates the window.
  Check the strategy deletes what it reinserts.
- **One giant batch.** A failure halfway leaves a half-restated window and no
  obvious place to resume. Bounded batches, oldest first.
- **`dbt run` for batches.** The data tests do not run, so a bad batch is found
  only after every later batch has built on it.
- **No backup.** Without it there is no parity check and no restore. A clone
  costs nothing on Snowflake.
- **Setting only `backfill_start`.** The toolkit's model refuses a half-set
  window rather than guessing the end.
