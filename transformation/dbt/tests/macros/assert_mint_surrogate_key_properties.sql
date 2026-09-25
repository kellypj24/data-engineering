-- Properties of mint_surrogate_key, checked over literal rows so it needs no
-- source and runs on the default duckdb target. Returns one row per violated
-- property (dbt convention: any row is a failure).
--
-- The golden key pins the version-1 encoding. If it changes, the encoding
-- changed: bump surrogate_key_version() and update the golden value together.

WITH cases AS (
    SELECT *
    FROM (
        VALUES
        (1, 'a|b', 'c'),
        (2, 'a', 'b|c'),
        (3, NULL, 'x'),
        (4, '', 'x'),
        (5, 'N/A', 'x'),
        (6, 'a|b', 'c'),
        (7, '~', 'x')
    ) AS c (case_id, field_a, field_b)
),

keyed AS (
    SELECT
        case_id,
        {{ mint_surrogate_key(['field_a', 'field_b']) }} AS sk,
        {{ mint_surrogate_key(['field_a', 'field_b'], null_as='N/A') }} AS sk_na
    FROM cases
),

k AS (
    SELECT
        MAX(CASE WHEN case_id = 1 THEN sk END) AS sk_1,
        MAX(CASE WHEN case_id = 2 THEN sk END) AS sk_2,
        MAX(CASE WHEN case_id = 3 THEN sk END) AS sk_3,
        MAX(CASE WHEN case_id = 4 THEN sk END) AS sk_4,
        MAX(CASE WHEN case_id = 6 THEN sk END) AS sk_6,
        MAX(CASE WHEN case_id = 7 THEN sk END) AS sk_7,
        MAX(CASE WHEN case_id = 1 THEN sk_na END) AS sk_na_1,
        MAX(CASE WHEN case_id = 3 THEN sk_na END) AS sk_na_3,
        MAX(CASE WHEN case_id = 5 THEN sk_na END) AS sk_na_5,
        COUNT(DISTINCT CASE WHEN case_id <> 6 THEN sk END) AS distinct_keys,
        {{ mint_surrogate_key(["'a'", "'b'"]) }} AS golden
    FROM keyed
)

SELECT 'NULL and empty string mint the same key' AS failure
FROM k
WHERE sk_3 = sk_4

UNION ALL
SELECT 'NULL and the NULL token mint the same key'
FROM k
WHERE sk_3 = sk_7

UNION ALL
SELECT 'delimiter collision: (a|b, c) = (a, b|c)'
FROM k
WHERE sk_1 = sk_2

UNION ALL
SELECT 'same inputs minted different keys'
FROM k
WHERE sk_1 <> sk_6

UNION ALL
SELECT 'distinct inputs minted colliding keys'
FROM k
WHERE distinct_keys <> 6

UNION ALL
SELECT 'null_as did not map NULL onto the literal''s key'
FROM k
WHERE sk_na_3 <> sk_na_5

UNION ALL
SELECT 'null_as changed the key of a row with no NULLs'
FROM k
WHERE sk_na_1 <> sk_1

UNION ALL
SELECT 'version-1 encoding changed without a version bump: ' || golden
FROM k
WHERE golden <> 'bd41e52e-af1f-4b17-f835-3c0f50fb1101'

UNION ALL
SELECT 'key is not a lowercase UUID: ' || sk
FROM keyed
WHERE
    LENGTH(sk) <> 36
    OR SUBSTR(sk, 9, 1) <> '-'
    OR SUBSTR(sk, 14, 1) <> '-'
    OR SUBSTR(sk, 19, 1) <> '-'
    OR SUBSTR(sk, 24, 1) <> '-'
    OR TRANSLATE(sk, '0123456789abcdef-', '') <> ''
