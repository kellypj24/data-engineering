# Architecture Patterns

Working reference for the architectural decisions behind this toolkit.

---

## EL vs ELT

**ELT (Extract, Load, Transform)** is the modern standard. The older ETL approach transformed data before loading it into the warehouse, which made sense when warehouse compute was expensive. That constraint no longer applies.

With ELT:

1. **Extract** raw data from sources (APIs, databases, files)
2. **Load** it into the warehouse as-is, preserving the original schema
3. **Transform** it inside the warehouse using SQL (dbt, SQLMesh, etc.)

Why ELT wins:

- **Raw data is preserved.** You can always re-transform without re-extracting. When business logic changes (and it will), you rewrite the transformation, not the pipeline.
- **Warehouse compute scales.** Snowflake, BigQuery, and Redshift handle the heavy lifting. You are not bottlenecked by your pipeline's compute.
- **Separation of concerns.** The EL layer does not need to understand business logic. It just moves data. Transformation is handled by people who understand the domain.
- **Faster iteration.** Changing a dbt model is a SQL edit and a `dbt run`. Changing an ETL pipeline means redeploying code, re-running extracts, and hoping nothing breaks.

The EL tools in this toolkit (Airbyte, dlt) handle steps 1 and 2. The transformation tools (dbt) handle step 3.

---

## Asset-based vs Task-based Orchestration

This is the core philosophical divide in orchestration.

### Task-based (Airflow)

You define a DAG of tasks -- Python functions, bash commands, API calls -- and wire them together with dependencies. The orchestrator runs tasks in order and tracks success/failure.

```
extract_task >> load_task >> transform_task >> test_task
```

**Pros:**
- Intuitive mental model (do this, then that)
- Mature ecosystem with operators for everything
- Well-understood by most data engineers

**Cons:**
- No awareness of what the tasks produce. Airflow knows "task X succeeded" but not "table Y is fresh."
- Testing requires running the full DAG or mocking heavily
- Refactoring a DAG often means rewriting it entirely

### Asset-based (Dagster)

You define assets -- the tables, files, and models your pipeline produces -- and declare how each asset depends on others. The orchestrator materializes assets and tracks their state.

```python
@asset(deps=[raw_users])
def clean_users():
    ...
```

**Pros:**
- The orchestrator understands your data, not just your code
- Built-in lineage: you can see which assets depend on which
- Easy to test individual assets in isolation
- Partial pipeline runs ("just refresh this one table") are native

**Cons:**
- Newer paradigm, smaller community (though growing fast)
- Not every workload maps cleanly to assets
- Requires rethinking if you are coming from a task-based background

### Recommendation

Use **Dagster** for new projects. The asset model pays for itself quickly once you have more than a handful of tables. Use **Airflow** when you are integrating with an existing Airflow deployment or need a specific operator that only exists in the Airflow ecosystem.

---

## Event-driven vs Schedule-driven

### Schedule-driven (cron)

Run the pipeline every hour, every day, every Monday at 6am. Simple, predictable, easy to reason about.

```
# Run at 6am UTC daily
0 6 * * *
```

**Use when:**
- Source data updates on a known schedule
- Freshness requirements are measured in hours, not minutes
- You want simplicity and predictability

**Drawbacks:**
- Wasteful if source data has not changed
- Latency between source update and warehouse update equals the schedule interval

### Event-driven (sensors/triggers)

Run the pipeline when something happens: a file lands in S3, a webhook fires, a database row changes.

```python
@sensor(job=ingest_job)
def new_file_sensor(context):
    new_files = check_s3_for_new_files()
    for f in new_files:
        yield RunRequest(run_key=f.key)
```

**Use when:**
- Source data arrives unpredictably
- Low-latency freshness matters
- You want to avoid unnecessary runs

**Drawbacks:**
- More complex to set up and debug
- Sensors need monitoring themselves (what if the sensor fails?)
- Can cause thundering herd problems if many events arrive at once

### Hybrid approach

Most production systems use both. Scheduled runs provide a baseline guarantee ("data is never more than 6 hours stale"), while sensors provide opportunistic freshness ("if new data arrives, process it immediately").

### Chaining jobs: one cron at the head, run-status sensors for the rest

Don't give dependent jobs staggered crons ("extract at 06:00, transform at 07:00"). The gap is a guess about how long upstream takes. When upstream runs long, downstream reads half-built data. When upstream fails, downstream runs anyway.

Schedule only the head of the chain. Launch each later job from a run-status sensor that monitors the job before it:

```python
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[extract_job],
    request_job=transform_job,
)
def transform_after_extract(context):
    return RunRequest(run_key=context.dagster_run.run_id)  # one run per upstream success
```

Rules, enforced by `orchestration/dagster/tests/test_invariants.py`:

- a chained job has **no** schedule of its own;
- each chained job has exactly one upstream sensor;
- every job a sensor monitors exists. A typo'd upstream otherwise leaves the chain silently never firing.

Worked example: `orchestration/dagster/src/jobs/chain.py` and `src/sensors/chain.py`.

### Event-driven chain: ingest → dbt subgraph → export

When a file or message starts the pipeline (`s3_sensor.py`, `sqs_sensor.py`),
the same chaining rules apply, with three jobs:

| Job | What | Why a separate job |
|---|---|---|
| Ingest | Loads the landed data into a raw table | A table-materialized staging model in the same job as the load can start reading the raw table before the load finishes. Job boundaries are the only ordering Dagster guarantees across steps that do not share an asset dependency |
| dbt subgraph | `dbt build` of the models downstream of what was ingested, not the whole project | One event should not rebuild unrelated marts |
| Export | Writes files for recipients ([file-export](../delivery/file-export/)) | Exports are side effects, not data: running one twice sends a file twice. A job, not an asset, so materializing assets never sends anything |

```python
ingest_job = define_asset_job("ingest_orders", selection=AssetSelection.keys(["raw", "orders"]))
orders_dbt_job = define_asset_job(
    "orders_dbt",
    selection=build_dbt_asset_selection([dbt_project_assets], "source:raw.orders+"),
)

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, monitored_jobs=[ingest_job], request_job=orders_dbt_job)
def dbt_after_ingest(context):
    return RunRequest(run_key=context.dagster_run.run_id)

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, monitored_jobs=[orders_dbt_job], request_job=export_job)
def export_after_dbt(context):
    return RunRequest(run_key=context.dagster_run.run_id)
```

- The sensor that sees the event launches only the ingest job; each later job
  is launched by a run-status sensor, so a failed ingest stops the chain.
- An export job in a chain gets no schedule of its own. `build_export_definitions`
  generates a STOPPED schedule per recipient: drop it for chained recipients,
  or the invariants fail (a chained job that is also scheduled).
- `run_key` = the upstream run id: one downstream run per upstream success.

---

## The Modern Data Stack

How the three roles connect end-to-end:

```
                    Orchestration (Dagster / Temporal / Airflow)
                    ┌──────────────────────────────────────────┐
                    │                                          │
                    │   schedules, triggers, monitors          │
                    │                                          │
                    └──────┬───────────────┬───────────────────┘
                           │               │
                           v               v
┌─────────┐     ┌──────────────┐   ┌──────────────┐    ┌──────────┐
│ Sources  │────>│  EL Layer    │──>│  Transform   │───>│Consumers │
│          │     │ (Airbyte/dlt)│   │  (dbt)       │    │          │
│ - APIs   │     │              │   │              │    │ - BI     │
│ - DBs    │     │ raw tables   │   │ modeled      │    │ - ML     │
│ - Files  │     │ in warehouse │   │ tables       │    │ - Apps   │
│ - Events │     └──────────────┘   └──────────────┘    └──────────┘
└─────────┘            │                    │
                       v                    v
                  ┌─────────────────────────────┐
                  │       Data Warehouse        │
                  │   (Snowflake / BigQuery /    │
                  │    Postgres / DuckDB)        │
                  └─────────────────────────────┘
```

**Key principle:** Each layer is independently replaceable. You can swap Airbyte for dlt without touching dbt. You can swap Dagster for Temporal without rewriting your EL pipelines. The stacks in this repo are opinionated combinations, but the individual tools are designed to work independently.

---

## Idempotency

Every pipeline in this toolkit should be idempotent: running it twice with the same inputs produces the same result as running it once. No duplicates, no missing data, no side effects.

### Why it matters

Pipelines fail. Networks timeout. Warehouses go down. When a pipeline fails halfway through and you re-run it, you need confidence that the re-run will not corrupt your data.

### How to achieve it

**EL layer:**
- Use **merge/upsert** loading instead of append. dlt and Airbyte both support this natively.
- Track state with **cursors** (e.g., `updated_at > last_sync_timestamp`). If a run fails and restarts, it re-processes the same window and upserts, producing the same result.
- Use **run IDs** or **load timestamps** to identify which batch loaded which rows, making it easy to delete and reload a specific batch.

**Transformation layer:**
- dbt models are idempotent by default when using `table` or `view` materializations (they `CREATE OR REPLACE`).
- `incremental` models need explicit logic to handle re-runs. Use `unique_key` to merge instead of append:
  ```sql
  {{ config(materialized='incremental', unique_key='id') }}
  ```

**Orchestration layer:**
- Dagster tracks asset materializations, so re-running a job re-materializes the same assets.
- Temporal workflows are inherently idempotent through their durable execution model.
- For Airflow, set `depends_on_past=False` and `retries` appropriately, and ensure your operators are idempotent.

### The test

Ask yourself: "If this pipeline runs three times right now, will my warehouse look the same as if it ran once?" If the answer is no, fix the pipeline before deploying it.
