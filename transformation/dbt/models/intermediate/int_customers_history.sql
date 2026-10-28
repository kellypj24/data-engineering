{#-
    Type-2 history of customers rebuilt from archived full extracts
    (reconstruct_scd2, E38), so history starts before the first snapshot run.
    Seed a go-forward snapshot from it once, at a fixed cutover:
    seed_snapshot_from_history (docs/patterns/snapshots.md).

    A table rebuilt in full each run: the archive does not change, so the
    rebuild is deterministic.
-#}

{{ config(materialized='table') }}

{{ reconstruct_scd2(source('raw', 'customer_extracts'), key='id', extract_date='extract_date') }}
