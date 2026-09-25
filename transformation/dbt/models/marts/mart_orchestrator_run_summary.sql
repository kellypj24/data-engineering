{#
    One row per orchestrator run: the first STARTED event paired with the
    run's completion (SUCCESS or FAILURE) on run_id. A run with no completion
    yet is IN_PROGRESS, with null completion metrics. The jobs in the
    `orchestrator_monitoring_jobs` var are excluded, so the job that refreshes
    this model does not report on itself.
#}

WITH events AS (

    SELECT *
    FROM {{ ref('stg_orchestrator_run_events') }}
    {% if var('orchestrator_monitoring_jobs') %}
        WHERE job_name NOT IN (
            {%- for job in var('orchestrator_monitoring_jobs') %}
                '{{ job }}'{{ ',' if not loop.last }}
            {%- endfor %}
        )
    {% endif %}

),

started_ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY event_at) AS event_rank
    FROM events
    WHERE status = 'STARTED'

),

completed_ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY event_at) AS event_rank
    FROM events
    WHERE status IN ('SUCCESS', 'FAILURE')

),

started AS (

    SELECT * FROM started_ranked
    WHERE event_rank = 1

),

completed AS (

    SELECT * FROM completed_ranked
    WHERE event_rank = 1

),

runs AS (

    SELECT
        started.run_id,
        started.job_name,
        started.trigger_source,
        started.trigger_name,
        started.dbt_command,
        started.warehouse,
        started.event_at AS started_at,
        completed.event_at AS completed_at,
        completed.tests_total,
        completed.tests_failed,
        completed.error,
        COALESCE(completed.status, 'IN_PROGRESS') AS run_status
    FROM started
    LEFT JOIN completed ON started.run_id = completed.run_id

)

SELECT
    run_id,
    job_name,
    run_status,
    trigger_source,
    trigger_name,
    dbt_command,
    warehouse,
    started_at,
    completed_at,
    {{ dbt.datediff('started_at', 'completed_at', 'second') }} AS duration_seconds,  -- noqa: LT05
    run_status = 'SUCCESS' AS is_success,
    run_status = 'FAILURE' AS is_failure,
    tests_total AS test_count,
    tests_failed AS test_failures,
    CAST(tests_failed AS {{ dbt.type_float() }})
    / NULLIF(tests_total, 0) AS test_failure_rate,
    error,
    CAST(started_at AS DATE) AS run_date,
    {{ dbt.date_trunc('week', 'started_at') }} AS run_week,
    {{ dbt.date_trunc('month', 'started_at') }} AS run_month,
    EXTRACT(HOUR FROM started_at) AS run_hour,
    -- 0 = Sunday on duckdb and Postgres; Snowflake follows WEEK_START.
    EXTRACT(DOW FROM started_at) AS run_day_of_week
FROM runs
