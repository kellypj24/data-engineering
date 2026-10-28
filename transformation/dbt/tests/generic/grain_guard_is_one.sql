{% test grain_guard_is_one(model, column_name='_grain_rows') %}
{#-
    Fails (warns, by default) on every row whose grain had more than one row
    before grain_guard deduped it: a key collision upstream that the dedup
    would otherwise hide. Raise it to an error per model:

        columns:
          - name: _grain_rows
            data_tests:
              - grain_guard_is_one:
                  config:
                    severity: error
-#}
{{ config(severity='warn') }}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} > 1

{% endtest %}
