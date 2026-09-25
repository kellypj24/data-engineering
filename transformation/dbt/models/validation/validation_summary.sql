SELECT
    validation_name,
    source_table,
    CAST(validated_at AS DATE) AS validation_date,
    max_failed_severity,
    validation_result,
    COUNT(*) AS failure_count,
    COUNT(DISTINCT record_id) AS failed_records,
    SUM(CASE WHEN should_notify THEN 1 ELSE 0 END) AS notification_count
FROM {{ ref('validation_log') }}
GROUP BY
    validation_name,
    source_table,
    CAST(validated_at AS DATE),
    max_failed_severity,
    validation_result
