-- Properties of row_content_hash (list form), checked over literal rows so it
-- needs no source and runs on the default duckdb target. Returns one row per
-- violated property. The relation form is tested in
-- tests/python/test_row_content_hash.py.
--
-- The golden hash pins the version-1 encoding. If it changes, the encoding
-- changed: bump row_content_hash_version() and update the golden value.

WITH cases AS (
    SELECT *
    FROM (
        VALUES
        (1, 'x', 'y', 1, [1, 2]),
        (2, 'x', 'y', 2, [1, 2]),
        (3, 'x', 'y', 1, [2, 1]),
        (4, NULL, 'y', 1, [1, 2]),
        (5, '', 'y', 1, [1, 2]),
        (6, 'x|', 'y', 1, [1, 2]),
        (7, 'x', '|y', 1, [1, 2])
    ) AS c (case_id, field_a, field_b, loaded_at, items)
),

hashed AS (
    SELECT
        case_id,
        {{ row_content_hash(['field_a', 'field_b', 'loaded_at', 'items'], exclude=['LOADED_AT'], arrays=['items']) }} AS h,
        {{ row_content_hash(['items', 'field_b', 'field_a'], exclude=['loaded_at'], arrays=['items']) }} AS h_reordered,
        {{ row_content_hash(['field_a', 'field_b', 'items'], arrays=['items'], keep_array_order=['items']) }} AS h_ordered
    FROM cases
),

h AS (
    SELECT
        MAX(CASE WHEN case_id = 1 THEN h END) AS h_1,
        MAX(CASE WHEN case_id = 2 THEN h END) AS h_2,
        MAX(CASE WHEN case_id = 3 THEN h END) AS h_3,
        MAX(CASE WHEN case_id = 4 THEN h END) AS h_4,
        MAX(CASE WHEN case_id = 5 THEN h END) AS h_5,
        MAX(CASE WHEN case_id = 6 THEN h END) AS h_6,
        MAX(CASE WHEN case_id = 7 THEN h END) AS h_7,
        MAX(CASE WHEN case_id = 1 THEN h_ordered END) AS h_ordered_1,
        MAX(CASE WHEN case_id = 3 THEN h_ordered END) AS h_ordered_3,
        {{ row_content_hash(["'a'", "'b'"]) }} AS golden
    FROM hashed
)

SELECT 'changing an excluded column changed the hash' AS failure
FROM h
WHERE h_1 <> h_2

UNION ALL
SELECT 'reordering an array changed the hash'
FROM h
WHERE h_1 <> h_3

UNION ALL
SELECT 'keep_array_order did not keep array order'
FROM h
WHERE h_ordered_1 = h_ordered_3

UNION ALL
SELECT 'NULL and empty string hash the same'
FROM h
WHERE h_4 = h_5

UNION ALL
SELECT 'delimiter collision: (x|, y) = (x, |y)'
FROM h
WHERE h_6 = h_7

UNION ALL
SELECT 'reordering the column list changed the hash'
FROM hashed
WHERE h <> h_reordered

UNION ALL
SELECT 'hash is not 32 lowercase hex characters: ' || h
FROM hashed
WHERE LENGTH(h) <> 32 OR TRANSLATE(h, '0123456789abcdef', '') <> ''

UNION ALL
SELECT 'version-1 encoding changed without a version bump: ' || golden
FROM h
WHERE golden <> '056b5264ee8117054efb49c275035c43'
