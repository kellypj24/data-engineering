{% test no_validation_failures(model, min_severity='CRITICAL') %}
{#-
    Fails for every row of a validation model whose worst failed rule is at
    `min_severity` or above. Attach it with the dbt severity that matches the
    tier -- `error` to stop the pipeline, `warn` to report and continue:

        tests:
          - no_validation_failures:
              min_severity: CRITICAL
              config:
                severity: error
-#}
SELECT
    validation_key,
    record_id,
    max_failed_severity,
    failed_rules
FROM {{ model }}
WHERE max_failed_severity_rank >= {{ validation_severity_ranks()[min_severity] }}
{% endtest %}
