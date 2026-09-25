# file-export

A config-driven file export engine. One YAML file per export; the engine turns
it into SQL, writes the files, keeps a run log with watermarks, and generates
one Dagster job and schedule per scheduled recipient.

Hand-written export jobs drift: every file gets its own SQL, its own windowing
bug, and its own idea of "incremental". One contract makes a delivery
reviewable as data. The most expensive failure an export can have is sending
one recipient another recipient's rows, so shared exports check tenant
isolation before writing and **fail closed**.

## Layout

```
src/file_export/
  config.py     # the YAML contract (pydantic); every rule is enforced at load time
  sql.py        # pure SQL builder: filters, window semantics, literals
  engine.py     # modes, window resolution, tenant check, writes, run log
  runlog.py     # run log + watermark store
  dialects.py   # duckdb COPY ... TO (executed); Snowflake COPY INTO @stage (generated)
  cli.py        # `file-export validate` / `file-export run`
  dagster.py    # optional adapter (the `dagster` extra); the core never imports it
  exposures.py  # dbt exposures generator + drift check (offline, from manifest.json)
configs/        # one YAML per export -- every file is validated in CI
```

## The config contract

```yaml
name: order-lines
kind: shared                  # shared | dedicated | ad_hoc
owner:                        # required; becomes the dbt exposure owner
  name: data-platform
source: marts.order_lines     # shared / ad_hoc; dedicated sets it per recipient
tenant_column: customer_id    # shared only, required

columns:                      # optional; default *
  - name: order_id
  - name: amount
    alias: line_amount
  - expr: "{range_end}"       # computed; {range_start} / {range_end} become literals
    alias: extracted_through

filters:                      # in | not_in | ilike | not_null -- one operator each
  - column: status
    in: [active, pending]

window:                       # boundary semantics are in the key name
  column: updated_at
  start_exclusive_end_inclusive:   # (start, end];  start_inclusive_end_inclusive: [start, end]
    start: last_run_end            # keywords: today, last_sunday, max_value, last_run_end
    end: max_value                 # or a literal date; omit a side for unbounded

outputs:                      # one query, several files
  - path: "{recipient}/order_lines_{run_date}.csv"
    header_record: "H|{export}|{recipient}|{run_date}"
    trailer_record: "T|{row_count}"
    control_file: true        # <path>.ctl: row count, window, sha256
  - path: "{recipient}/order_lines_{run_date}.parquet"
    format: parquet

recipients:
  - name: north
    tenant_keys: [101, 102]   # the only tenant values north may receive
    schedule:
      cron: "0 6 * * *"       # always UTC; generated schedules start STOPPED
```

| Kind | Source | Per recipient |
|---|---|---|
| `shared` | one model | `tenant_keys` (required), optional `filters` |
| `dedicated` | one model **per recipient**, filters baked in | `source`; delivery settings only |
| `ad_hoc` | any relation | never scheduled |

A shared recipient without `filters` is selected by `tenant_column IN
tenant_keys`. **Either way, the result is checked against `tenant_keys`
before any write.** A wrong filter, an upstream join that leaks rows, or a
model change then fails the run instead of reaching a recipient.

## Running

```bash
uv run file-export validate configs
uv run file-export run configs/order_lines_shared.yml --recipient north \
    --mode dry-run --duckdb warehouse.duckdb        # prints SQL, writes nothing
uv run file-export run configs/order_lines_shared.yml --all \
    --mode execute --duckdb warehouse.duckdb --output-root exports
```

| Mode | Queries | Writes files | Run log |
|---|---|---|---|
| `dry-run` | resolves window keywords only | no | no |
| `select-only` | counts rows | no | no |
| `execute` | yes | yes | one row per attempt |

## Watermarks and windows

Incremental windows are `(start, end]`: the row *at* the watermark was sent
last time. `last_run_end` is the window end of the latest **successful** run.

- A **0-row run** is a success and advances the watermark, so idle days don't
  stall it.
- A **first run** with no watermark exports all history. To cut over from a
  legacy job, insert one `success` row into the run log carrying the legacy
  job's last window end.
- Both bounds are resolved to literals **before** querying, and the rows are
  materialised once. The count, the tenant check, and every file come from
  the same snapshot. A stale upstream causes lag, never loss.
- `max_value` is `MAX(column)` over the whole source and **ignores row
  filters**. With an incremental window and row filters, the watermark can
  advance past rows a filter excluded. Loading such a config warns.

## Failure behaviour

- A tenant violation raises before anything is written. The recipient's
  remaining outputs are not attempted, and the failure is logged.
- Every output is written to `<path>.partial` and renamed only after all of
  them succeed. A failure midway leaves no partial delivery.
- `run --all` treats recipients independently: one failure is logged and
  reported, and the others still run.
- A failed run does not advance the watermark.

## Dagster

```python
from file_export.dagster import ExportWarehouse, build_export_definitions

defs = build_export_definitions(
    "configs/",
    ExportWarehouse(duckdb_path="warehouse.duckdb", output_root="exports"),
)
```

One job per scheduled recipient (`export__<export>__<recipient>`), each with a
schedule that is STOPPED by default and runs in UTC.

## Generated dbt exposures

Every delivered file gets a dbt exposure: one per (export, recipient, output),
with a deterministic name (`export__<export>__<recipient>__<format>`), the
export's `owner`, and `depends_on` the model it reads. Recipients then show up
in the DAG, and `dbt ls --select +exposure:*` answers "what does this PR change
for the people we send files to?".

```bash
just file-export::exposures-write   # dbt parse, then regenerate the file
just file-export::exposures-check   # dbt parse, then fail if it is stale or wrong
```

`--check` works from `target/manifest.json`, with no warehouse connection. It
fails when:

- a config changed without regenerating the file;
- an export's source matches no dbt model, seed, or snapshot (matched by
  alias, with ties broken by schema);
- any column the export references (selected, filter, window, tenant) is not
  documented on that model. The manifest only knows documented columns;
- two exposures would share a name.

It runs in CI (`exposures-drift`, on changes to either the configs or the dbt
project) and as a local pre-commit hook in `transformation/dbt`. The generated
file is `transformation/dbt/models/exports/_generated_exposures.yml`. Never
edit it by hand.

## Warehouses

Execution runs on duckdb (`COPY ... TO`). `SnowflakeDialect(stage=...)`
generates the equivalent `COPY INTO @stage/...` statements. They are covered by
string-level tests only, not run against an account.

## Tests

```bash
just file-export::test
```

These cover config validation (including every file under `configs/`), SQL
rendering, dry-run and select-only side effects, a three-run watermark chain
with a 0-row run, fail-closed tenant isolation, all-or-nothing delivery, and
the generated Dagster definitions.
