{#- From the user mapping plus the fixed workloads the allocation assigns. -#}

SELECT workload FROM {{ ref('cost_user_mapping') }}
UNION DISTINCT
SELECT 'IDLE'
UNION DISTINCT
SELECT 'CLOUD_SERVICES'
UNION DISTINCT
SELECT 'STORAGE'
UNION DISTINCT
SELECT 'OTHER'
UNION DISTINCT
SELECT 'NEEDS_OWNER_REVIEW'
