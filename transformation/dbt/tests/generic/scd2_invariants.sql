{% test scd2_invariants(model, key, valid_from='dbt_valid_from', valid_to='dbt_valid_to') %}
{#-
    The three ways a type-2 history table breaks, per key:

        zero_length  a closed version with valid_to <= valid_from
        open_twice   more than one open version (valid_to IS NULL)
        overlap      a version that ends after the next one starts, or an
                     open version that is not the last

    A point-in-time query over a broken table returns zero or two rows for a
    key without erroring, and `unique` on the surrogate key catches none of
    these. Gaps between versions are allowed: a hard delete followed by a
    reappearance leaves one. Returns one row per violation.

    Usage (model level):
        tests:
          - scd2_invariants:
              key: id
              # valid_from / valid_to default to dbt's snapshot columns
-#}

WITH versions AS (
    SELECT
        {{ key }} AS scd2_key,
        {{ valid_from }} AS valid_from,
        {{ valid_to }} AS valid_to,
        LEAD({{ valid_from }}) OVER (
            PARTITION BY {{ key }}
            ORDER BY {{ valid_from }}, {{ valid_to }} NULLS LAST
        ) AS next_valid_from
    FROM {{ model }}
)

SELECT
    'zero_length' AS violation,
    scd2_key
FROM versions
WHERE valid_to IS NOT NULL AND valid_to <= valid_from

UNION ALL
SELECT
    'open_twice' AS violation,
    scd2_key
FROM versions
WHERE valid_to IS NULL
GROUP BY scd2_key
HAVING COUNT(*) > 1

UNION ALL
SELECT
    'overlap' AS violation,
    scd2_key
FROM versions
WHERE
    next_valid_from IS NOT NULL
    AND (valid_to IS NULL OR valid_to > next_valid_from)

{% endtest %}
