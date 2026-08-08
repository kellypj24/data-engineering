-- Verify that the staging model has at least one record.
-- This singular test fails if the result set is non-empty (dbt convention).
--
-- No trailing semicolon: dbt wraps a singular test in an outer query, so a
-- terminator here produces "syntax error at or near ;". This is why `just
-- dbt::lint` covers models/ and macros/ but not tests/ -- .sqlfluff sets
-- require_final_semicolon, which is correct for models and wrong for tests.

SELECT 1
FROM {{ ref('stg_example') }}
HAVING COUNT(*) = 0
