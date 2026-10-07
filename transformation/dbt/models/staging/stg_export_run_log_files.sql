{#- One row per file a successful export run wrote. Failed runs write nothing
    (file-export removes partial files), so they expect no delivery. -#}

WITH source AS (

    SELECT *
    FROM {{ source('file_export', 'export_run_log') }}
    WHERE
        status = 'success'
        AND files IS NOT NULL

),

exploded AS (

    SELECT
        source.run_id,
        source.export_name,
        source.recipient,
        source.finished_at AS written_at,
        exploded.line AS file_path
    FROM {{ explode_lines('source', 'files') }}

)

SELECT
    run_id,
    export_name,
    recipient,
    file_path,
    written_at,
    {{ dbt.split_part('file_path', "'/'", -1) }} AS file_name
FROM exploded
WHERE file_path <> ''
