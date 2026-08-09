---
name: dbt-source
description: Use when declaring a new raw table to dbt — "add a source for X", "register the raw orders table", "dbt can't find source('raw', ...)", "add a source and its staging model", "source freshness". Adds the table to _sources.yml with tests and generates the matching staging model.
argument-hint: "<source-name> <table-name>   e.g. raw orders"
---

# Add a dbt source

Runs **in the downstream project**, not the toolkit. Locate the dbt root first:

```bash
find . -name dbt_project.yml -not -path "*/dbt_packages/*" -not -path "*/.venv/*"
```

All commands below run from that directory.

A source is half a change. `{{ source('raw', 'orders') }}` resolving is the
point, but a declared source with no staging model in front of it is a table
every downstream model will reach into directly — which is the thing the staging
layer exists to prevent. Do both halves.

## Step 1 — declare the table

Sources live in `models/staging/_sources.yml`, grouped under a source `name`
(the toolkit ships one, `raw`, described as "Raw data loaded by Airbyte or
dlt"). Add a table to the existing group rather than creating a second group for
the same schema.

```yaml
version: 2

sources:
  - name: raw
    description: Raw data loaded by Airbyte or dlt
    schema: raw
    tables:
      - name: orders
        description: Raw orders from the source system
        columns:
          - name: id
            description: Primary key
            tests:
              - unique
              - not_null
          - name: created_at
            description: Order creation timestamp
```

Note the split: `name` is what you write in `source()`, `schema` is the physical
schema. They differ often enough that it is worth setting `schema:` explicitly
rather than relying on the name.

Test the **key at minimum** — `unique` + `not_null` on the primary key. Source
tests are the earliest possible failure point; a key that is not unique in the
raw table produces a fan-out that is far more confusing to debug three models
downstream.

`_sources.yml` is the one deliberate exception to the project's one-file-per-
model rule: sources are grouped in a single file per layer, models are not.

### Freshness, if the source is genuinely scheduled

```yaml
      - name: orders
        loaded_at_field: _airbyte_extracted_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

Only add this when the table has a real load cadence and a real load-timestamp
column. `freshness` without a correct `loaded_at_field` fails
`dbt source freshness` for everyone, permanently, and gets muted — which costs
you the check on the sources that needed it.

## Step 2 — generate the staging model

One staging model per source table, 1:1, named `stg_<source>_<table>`. Use the
**`dbt-model`** skill for this — it owns the macro constraints
(`audit_columns` last, `limit_data_in_dev` needing `WHERE 1 = 1`), the sqlfluff
rules, and the paired-`.yml` requirement. Do not re-derive them here.

The one thing this skill adds: pass the EL tool's load timestamp into
`audit_columns` when the source has one, so `_loaded_at` reflects the real
extract time rather than the moment dbt happened to run:

```sql
{{ audit_columns('_airbyte_extracted_at') }}
```

If you do not know the column, `codegen` will tell you — it is already in
`packages.yml`:

```bash
uv run dbt run-operation generate_source \
  --args '{"schema_name": "raw", "table_names": ["orders"], "generate_columns": true}'
uv run dbt run-operation generate_base_model \
  --args '{"source_name": "raw", "table_name": "orders"}'
```

These read the warehouse's real schema and print YAML/SQL to stdout. Treat the
output as a **draft**: `generate_columns` defaults to `false`, so without it you
get table stubs and no columns at all — and even with it, the output carries no
descriptions and no tests, so it cannot pass dbt-checkpoint as-is.

## Verify

From the dbt root:

```bash
uv run dbt deps                          # first run only
uv run dbt parse                         # source resolves, yml is valid
uv run dbt compile --select source:raw.orders+
uv run sqlfluff lint models/staging/stg_raw_orders.sql
```

Where the warehouse is reachable and the raw table actually exists:

```bash
uv run dbt test --select source:raw.orders   # the source tests you just wrote
uv run dbt build --select stg_raw_orders
uv run dbt source freshness --select source:raw.orders   # only if you set freshness
```

In the **toolkit's own** copy there is no `raw` schema, so everything past
`parse` / `compile` / `lint` fails by design — task #16 in
`docs/ci-cd-hardening.md`. Downstream that excuse does not apply; run the tests.

`profiles.yml` lives in the project directory, so `DBT_PROFILES_DIR` must point
at the dbt root. `mod.just` exports it; direct `dbt` calls need it set.

## Common mistakes

- **Declaring the source and stopping.** Without a staging model, the next
  person writes `source()` straight into a mart and the layer boundary is gone.
- **A second `sources:` group for a schema that already has one.** Both resolve,
  and now the tests and descriptions live in two places.
- **`freshness` with a guessed `loaded_at_field`.** Permanently red, then muted.
  Leave freshness off rather than wrong.
- **Trusting `codegen` output as finished.** No descriptions, no tests —
  dbt-checkpoint rejects it, and the failure appears to be about your model
  rather than about the generator.
- **Assuming `name` and `schema` are the same.** Set `schema:` explicitly.
- **Testing every column instead of the key.** Source tests run against raw data
  you do not control; over-testing there produces failures nobody can fix and
  trains people to ignore the whole suite.
