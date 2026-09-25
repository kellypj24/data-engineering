-- Verify that the staging model has at least one record.
-- This singular test fails if the result set is non-empty (dbt convention).
--
-- No trailing semicolon: dbt wraps a singular test in an outer query, so a
-- terminator here produces "syntax error at or near ;". The same holds for
-- models, which is why .sqlfluff sets require_final_semicolon = false.

SELECT 1
FROM {{ ref('stg_example') }}
HAVING COUNT(*) = 0
