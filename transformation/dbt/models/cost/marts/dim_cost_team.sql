{#- From the mapping seeds plus the two explicit buckets, never from the fact. -#}

SELECT team FROM {{ ref('cost_user_mapping') }}
WHERE team IS NOT NULL
UNION DISTINCT
SELECT team FROM {{ ref('cost_database_mapping') }}
UNION DISTINCT
SELECT 'SHARED'
UNION DISTINCT
SELECT 'NEEDS_OWNER_REVIEW'
