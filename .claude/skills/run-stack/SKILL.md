---
name: run-stack
description: Use when bringing a stack up locally and proving it works end to end — "run the stack", "start everything locally", "does the pipeline actually work", "set up airbyte-dagster-dbt", "my dbt models aren't showing up in Dagster". Covers startup order and the checks that prove each hop.
argument-hint: "[stack-name]   e.g. dlt-dagster-dbt"
---

# Run a stack end to end

A stack is EL → warehouse → dbt → orchestrator. Proving it works means proving
each hop separately, in order. Skipping to "open the UI and look" is how people
spend an hour on a problem that a one-line check would have located.

Identify the stack first — `stacks/<name>/README.md` states its components and
prerequisites. `airbyte-dagster-dbt` needs Docker and a separately installed
Airbyte; `dlt-dagster-dbt` and `dlt-temporal-dbt` run in-process with no extra
infrastructure. Prefer a dlt stack when you just need to prove the wiring.

## The ordering rule that catches everyone

**dbt must be parsed before the orchestrator starts.**

Dagster's `src/assets/dbt.py` reads `target/manifest.json` at *module import*
time. No manifest means `dbt_project_assets` is `None`, the conditional splat in
`src/assets/__init__.py` drops it, and the code location loads **successfully**
with every dbt asset missing. No error, no warning — just an incomplete graph
that looks like a Dagster problem and is not.

So: `dbt parse` first, `dagster dev` second. Always.

## The sequence

### 1. Warehouse

The dbt profile defaults to DuckDB, so there is nothing to stand up and no
credentials to find. Keep it there while proving wiring; switch with
`DBT_TARGET=snowflake` (plus `SNOWFLAKE_*`) once the shape is confirmed.

Environment is separate from warehouse: `DBT_ENV` (`dev` default, `prod`) drives
`limit_data_in_dev` and schema routing. Leave it unset locally.

### 2. EL — load something real

```bash
# dlt stacks
cd extract_load/dlt && uv run python -m pipelines.example_pipeline

# Airbyte stacks: abctl local install, configure the connection in the UI at
# :8000, note the connection UUID, then trigger a sync.
```

Prove it landed before moving on — a row count, not a green log line.

### 3. dbt — build, then parse

```bash
cd transformation/dbt
export DBT_PROFILES_DIR="$PWD"
uv run dbt deps        # first run only
uv run dbt build       # models + tests
uv run dbt parse       # writes target/manifest.json for the orchestrator
```

**In the toolkit's own checkout `dbt build` fails by design.** The example models
read `source('raw', …)` and nothing creates that schema — task #16 in
`docs/ci-cd-hardening.md`. There, `parse` and `compile` are as far as you can
get, and that is expected. In a real downstream project it is not: a failing
`dbt build` there is a genuine failure, so do not carry the toolkit's excuse
across.

### 4. Orchestrator

```bash
cd orchestration/dagster
uv run dagster definitions validate    # loads the way the daemon does
uv run dagster dev                     # UI at :3000
```

The dbt models should appear as assets. If they do not, go back to step 3 —
it is the manifest, essentially every time.

For Temporal stacks, the worker must be running *and* registered against the
same task queue as the workflow; a workflow submitted to a queue with no worker
sits in `Running` forever rather than failing.

## Prove it, hop by hop

Do not accept a green UI as proof. Each of these fails independently:

```bash
# EL landed rows
uv run python -c "import duckdb; print(duckdb.connect('dev.duckdb').sql('SHOW TABLES'))"

# dbt produced models, and the tests pass
uv run dbt build

# the orchestrator can actually see them
uv run dagster definitions validate
```

Then materialise one asset from the UI and confirm the row count changes. That
is the only check that exercises the whole chain in one motion.

## Common mistakes

- **Starting the orchestrator before `dbt parse`.** Silent, complete
  disappearance of every dbt asset. The single most common failure here.
- **Reading the toolkit's failing `dbt build` as a broken stack.** It is task
  #16 and expected in this repo only.
- **Forgetting `DBT_PROFILES_DIR`.** `profiles.yml` lives in the project
  directory, not `~/.dbt`. `mod.just` exports it; bare `dbt` calls do not.
- **`just airbyte::validate` on a fresh clone.** Needs `terraform init
  -backend=false` in `extract_load/airbyte/terraform/` first — there is no
  `init` recipe yet.
- **Assuming `abctl local install` gives you a connection.** It gives you an
  Airbyte instance; sources, destination, and connection are manual UI work, and
  the connection UUID has to be copied into the Dagster asset.
- **Chasing warehouse credentials before the wiring is proven.** DuckDB is the
  default precisely so you can prove the shape first.
- **Setting `base_dir: ~/...` in `dagster.yaml`.** Dagster does not expand `~`
  there, so it creates a literal directory named `~` inside the repo. Storage is
  left unconfigured on purpose — set `DAGSTER_HOME` instead.
- **Declaring success from logs.** Check row counts.
