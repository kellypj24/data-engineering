{#-
    DURABLE FACT: this table keeps history longer than its source does.

    Upstream orders are only retained for a limited window, but this fact keeps
    every day forever. A --full-refresh would rebuild it from what the source
    still holds and silently drop every day that has aged out -- no error, just
    less data. So:

      - full_refresh=false: `dbt build --full-refresh` leaves this table alone.
      - Only the last `fct_daily_order_revenue_restate_periods` days (default 2)
        before the latest loaded day are ever reprocessed. Older days are final.
      - revenue is tied to an independent control total per complete day
        (ties_to_control_total), which fails if no day was compared at all.

    To deliberately rebuild, drop the table by hand after confirming the source
    still covers the full history. See docs/patterns/durable-facts.md.

    Backfill: `--vars '{backfill_start: 2026-01-01, backfill_end: 2026-01-07}'`
    reprocesses exactly those days (inclusive) instead of the restate window.
    Days outside it are untouched. Run long ranges in bounded batches (the
    `backfill-runbook` skill).

    delete+insert is supported on duckdb, Snowflake, and Postgres; on BigQuery
    use merge or insert_overwrite.
-#}

{{ config(
    materialized='incremental',
    unique_key='order_date',
    incremental_strategy='delete+insert',
    full_refresh=false
) }}

WITH orders AS (

    SELECT
        CAST(created_at AS DATE) AS order_date,
        amount
    FROM {{ ref('stg_example') }}
    {% set backfill_start = var('backfill_start', none) %}
    {% set backfill_end = var('backfill_end', none) %}
    {% if (backfill_start is none) != (backfill_end is none) %}
        {{ exceptions.raise_compiler_error("set both backfill_start and backfill_end, or neither") }}
    {% endif %}
    {% if backfill_start is not none and backfill_start | string > backfill_end | string %}
        {{ exceptions.raise_compiler_error("backfill_start is after backfill_end") }}
    {% endif %}
    {% if is_incremental() and backfill_start is not none %}
        WHERE CAST(created_at AS DATE) BETWEEN CAST('{{ backfill_start }}' AS DATE) AND CAST('{{ backfill_end }}' AS DATE)
    {% elif is_incremental() %}
        WHERE CAST(created_at AS DATE) > (
            SELECT {{ dbt.dateadd('day', -var('fct_daily_order_revenue_restate_periods', 2), 'MAX(order_date)') }}
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM orders
GROUP BY order_date
