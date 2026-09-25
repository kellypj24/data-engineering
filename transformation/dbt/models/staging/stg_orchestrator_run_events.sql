WITH source AS (

    SELECT *
    FROM {{ source('raw', 'orchestrator_run_events') }}

),

renamed AS (

    SELECT
        run_id,
        job_name,
        trigger_source,
        trigger_name,
        dbt_command,
        warehouse,
        tests_total,
        tests_failed,
        error,
        event_at,
        UPPER(status) AS status
    FROM source

)

SELECT * FROM renamed
