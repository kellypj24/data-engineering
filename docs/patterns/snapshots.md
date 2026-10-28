# Snapshots

A dbt snapshot keeps type-2 history of a table that only holds current state:
each run compares the source with the latest version of every row, closes the
versions that changed, and opens new ones. Example:
`transformation/dbt/snapshots/customers_snapshot.sql`, tested in
`tests/python/test_snapshot.py`.

## History begins at the first run

A snapshot of a current-state source **cannot reconstruct the past**. The first
run records today; nothing before it can be recovered, because the source no
longer has it. Start snapshotting a table before anyone needs its history.
"We'll add a snapshot when the question comes up" means the answer starts
from that day.

## Strategy

| Use | When | Config |
|-----|------|--------|
| `timestamp` | the source has an `updated_at` that changes on **every** change to a row, and is never backdated | `strategy='timestamp', updated_at='updated_at'` |
| `check` | no such column, or you do not trust it | `strategy='check', check_cols=['email', 'plan']` |

For `check`, list the columns explicitly. `check_cols='all'` makes every column
added to the source later a "change" to every row, which writes a new version of
the whole table on the next run. Leave out columns that change without meaning
anything (a `last_seen_at` heartbeat), or every run versions every row.

### One hash column instead of a long `check_cols` list

`row_content_hash` (`macros/utils/row_content_hash.sql`) puts a row's content in
one column, so `check_cols` has one entry that cannot drift from the table:

```sql
SELECT
    id,
    email,
    plan,
    {{ row_content_hash(source('raw', 'customers'), exclude=['_loaded_at', 'last_seen_at']) }} AS content_hash
FROM {{ source('raw', 'customers') }}
-- config: strategy='check', check_cols=['content_hash']
```

- `exclude` drops volatile columns by name (case-insensitive).
- Arrays are sorted first, so element order is not a change; list
  order-significant arrays in `keep_array_order`.
- Columns are hashed in sorted name order, so reordering them is not a change.
- Given a relation, the hash covers every column. **Adding a column to the
  source changes every row's hash once**, so the next run opens one new version
  per row. Pass an explicit column list for wide sources that gain columns
  often.
- On Snowflake, ARRAY / VARIANT / OBJECT columns cannot be compared with `=`,
  which is what a `check` snapshot does; the hash serialises them first
  (`macros/snowflake/row_content_hash.sql`).

Switching an existing snapshot to a hash column re-versions every row on the
next run: ship it with a migration file (`docs/patterns/state-migrations.md`).

A `timestamp` column that the source sets on insert but not on update makes the
snapshot miss every update, silently. Verify it changes on update before trusting
it.

## Hard deletes

What happens when a row disappears from the source is set by `hard_deletes`
(dbt ≥ 1.9; on 1.8, `invalidate_hard_deletes=true` is the `invalidate` option):

| `hard_deletes` | The deleted row's history | Downstream meaning |
|----------------|---------------------------|--------------------|
| `ignore` (default) | stays current forever | "who exists now" is wrong: deleted rows still count |
| `invalidate` | current version closed (`dbt_valid_to` set) | "who existed on date t" is right; deletions are visible |
| `new_record` | closed, plus a new row with `dbt_is_deleted = True` | the deletion itself is an event you can count |

The example uses `invalidate`. The default, `ignore`, is rarely what anyone
wants and is easy to miss, because nothing fails.

## The point-in-time query

The version current at instant `t`:

```sql
SELECT *
FROM {{ ref('customers_snapshot') }}
WHERE dbt_valid_from <= t
  AND (t < dbt_valid_to OR dbt_valid_to IS NULL)
```

`dbt_valid_from` is inclusive and `dbt_valid_to` exclusive, so each key matches
at most one version at any `t`. Using `<=` on both ends double-counts the
instant of a change, and using `BETWEEN` does the same. Current rows have
`dbt_valid_to IS NULL`, so that branch is required.

## Cadence

Schedule a snapshot **shortly before** the jobs that read it, in the same
chain (see "Chaining jobs" in `docs/architecture-patterns.md`), not on an
unrelated cron. A snapshot that runs after the models reading it makes them
one cycle stale. One that runs at a random time loses any change that is made
and reverted between two runs. Snapshot frequency is the resolution of your
history: daily snapshots cannot tell you the state at noon.

## Operating it

- **Never `--full-refresh` a snapshot table, and never drop it.** It is the
  only copy of its history, like a durable fact (`docs/patterns/durable-facts.md`).
- Changing `check_cols` or the strategy does not rewrite past versions; it
  changes what counts as a change from the next run on.
- `unique_key` must be unique in the source. Duplicates produce overlapping
  versions that break the point-in-time query. Test it on the source.
- Test the history itself with `scd2_invariants`
  (`tests/generic/scd2_invariants.sql`): no zero-length spans, at most one open
  version per key, no overlapping spans. Gaps are allowed. `unique` on the
  surrogate key catches none of these, and the point-in-time query fails
  silently on all three:

  ```yaml
  snapshots:
    - name: customers_snapshot
      data_tests:
        - scd2_invariants:
            key: id   # valid_from / valid_to default to dbt_valid_from / dbt_valid_to
  ```
