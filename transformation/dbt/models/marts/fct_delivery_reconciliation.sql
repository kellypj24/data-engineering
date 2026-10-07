{#
    Expected deliveries (every file a successful export run wrote) against
    confirmed transfers (what a transport recorded arriving), one row per
    expected file plus one per unmatched confirmation:

      delivered   confirmed by due_at (written_at + delivery_grace_minutes)
      late        confirmed after due_at
      missing     not confirmed, and due_at has passed
      unexpected  a confirmation that matches no written file

    Files not yet due and not yet confirmed are left out until they are due.
    A confirmation belongs to the latest run that wrote that file name for that
    recipient before the confirmation, so a fixed file name re-sent every day
    reconciles run by run. A scheduled run that never happened writes no run-log
    row, so it is not here: that is the orchestrator's alert (run telemetry).
    `reconciliation_as_of` pins "now" for tests.
#}

{%- set as_of = var('reconciliation_as_of', none) -%}
{%- set now_sql = "CAST('" ~ as_of ~ "' AS TIMESTAMP)" if as_of
    else 'CAST(' ~ dbt.current_timestamp() ~ ' AS TIMESTAMP)' -%}

WITH expected AS (

    SELECT
        *,
        {{ dbt.dateadd('minute', var('delivery_grace_minutes'), 'written_at') }} AS due_at
    FROM {{ ref('stg_export_run_log_files') }}

),

confirmed AS (

    SELECT * FROM {{ ref('stg_delivery_confirmations') }}

),

candidates AS (

    SELECT
        confirmed.recipient,
        confirmed.file_name,
        confirmed.confirmed_at,
        confirmed.transport,
        expected.run_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                confirmed.recipient,
                confirmed.file_name,
                confirmed.confirmed_at,
                confirmed.transport
            ORDER BY expected.written_at DESC
        ) AS match_rank
    FROM confirmed
    LEFT JOIN expected
        ON
            confirmed.recipient = expected.recipient
            AND confirmed.file_name = expected.file_name
            AND confirmed.confirmed_at >= expected.written_at

),

matched AS (

    SELECT * FROM candidates
    WHERE match_rank = 1

),

first_confirmation AS (

    SELECT
        run_id,
        file_name,
        confirmed_at,
        transport,
        COUNT(*) OVER (PARTITION BY run_id, file_name) AS confirmation_count,
        ROW_NUMBER() OVER (
            PARTITION BY run_id, file_name ORDER BY confirmed_at
        ) AS confirmation_rank
    FROM matched
    WHERE run_id IS NOT NULL

),

reconciled AS (

    SELECT
        expected.export_name,
        expected.recipient,
        expected.file_name,
        expected.run_id,
        expected.written_at,
        expected.due_at,
        first_confirmation.confirmed_at,
        first_confirmation.transport,
        COALESCE(first_confirmation.confirmation_count, 0) AS confirmation_count,
        CASE
            WHEN first_confirmation.confirmed_at IS NULL THEN 'missing'
            WHEN first_confirmation.confirmed_at > expected.due_at THEN 'late'
            ELSE 'delivered'
        END AS delivery_state
    FROM expected
    LEFT JOIN first_confirmation
        ON
            expected.run_id = first_confirmation.run_id
            AND expected.file_name = first_confirmation.file_name
            AND first_confirmation.confirmation_rank = 1
    WHERE
        first_confirmation.confirmed_at IS NOT NULL
        OR expected.due_at <= {{ now_sql }}

),

unexpected AS (

    SELECT
        CAST(NULL AS {{ dbt.type_string() }}) AS export_name,
        recipient,
        file_name,
        CAST(NULL AS {{ dbt.type_string() }}) AS run_id,
        CAST(NULL AS {{ dbt.type_timestamp() }}) AS written_at,
        CAST(NULL AS {{ dbt.type_timestamp() }}) AS due_at,
        confirmed_at,
        transport,
        1 AS confirmation_count,
        'unexpected' AS delivery_state
    FROM matched
    WHERE run_id IS NULL

)

SELECT
    *,
    CASE
        WHEN delivery_state = 'late'
            THEN {{ dbt.datediff('due_at', 'confirmed_at', 'minute') }}
    END AS minutes_late
FROM (
    SELECT * FROM reconciled
    UNION ALL
    SELECT * FROM unexpected
) AS all_rows
