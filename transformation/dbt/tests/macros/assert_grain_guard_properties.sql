-- Properties of grain_guard over literal rows, on the default duckdb target.
-- The adapter's form (QUALIFY on duckdb) and the ANSI fallback
-- (default__grain_guard, used by Postgres and BigQuery) must return the same
-- rows and columns. Returns one row per violated property.

WITH cases AS (
    SELECT *
    FROM (
        VALUES
        (1, 'a', 10),
        (1, 'b', 20),
        (1, 'c', 30),
        (2, 'x', 10),
        (CAST(NULL AS INTEGER), 'n1', 10),
        (CAST(NULL AS INTEGER), 'n2', 20)
    ) AS c (k, label, seq)
),

native AS (
    {{ grain_guard('cases', grain=['k'], order_by=['seq DESC', 'label']) }}
),

ansi AS (
    {{ default__grain_guard('cases', ['k'], ['seq DESC', 'label']) }}
),

differences AS (
    (SELECT * FROM native EXCEPT SELECT * FROM ansi)
    UNION ALL
    (SELECT * FROM ansi EXCEPT SELECT * FROM native)
)

SELECT 'QUALIFY and ANSI forms differ: ' || CAST(k AS VARCHAR) || ' ' || label AS failure
FROM differences

UNION ALL
SELECT 'kept the wrong row for k = 1: ' || label
FROM native
WHERE k = 1 AND label <> 'c'

UNION ALL
SELECT 'wrong pre-dedup count for k = 1'
FROM native
WHERE k = 1 AND _grain_rows <> 3

UNION ALL
SELECT 'a clean key was counted as a collision'
FROM native
WHERE k = 2 AND _grain_rows <> 1

UNION ALL
SELECT 'NULL keys were not grouped as one collision'
FROM native
WHERE k IS NULL AND _grain_rows <> 2

UNION ALL
SELECT 'not one row per key: ' || CAST(COUNT(*) AS VARCHAR) || ' rows'
FROM native
HAVING COUNT(*) <> 3
