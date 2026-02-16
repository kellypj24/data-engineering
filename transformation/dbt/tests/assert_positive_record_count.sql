-- Verify that the staging model has at least one record.
-- This singular test fails if the result set is non-empty (dbt convention).

SELECT 1
FROM {{ ref('stg_example') }}
HAVING COUNT(*) = 0;
