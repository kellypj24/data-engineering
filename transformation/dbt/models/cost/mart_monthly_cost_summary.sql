{#- One row per month: total, the SHARED share, and the NEEDS_OWNER_REVIEW
    share. Unattributed spend is reported, not hidden. -#}

SELECT
    cost_month,
    currency,
    SUM(cost) AS total_cost,
    SUM(CASE WHEN team = 'SHARED' THEN cost ELSE 0 END) AS shared_cost,
    SUM(CASE WHEN team = 'NEEDS_OWNER_REVIEW' THEN cost ELSE 0 END) AS needs_owner_review_cost,
    SUM(CASE WHEN team = 'SHARED' THEN cost ELSE 0 END)
    / NULLIF(SUM(cost), 0) AS shared_share,
    SUM(CASE WHEN team = 'NEEDS_OWNER_REVIEW' THEN cost ELSE 0 END)
    / NULLIF(SUM(cost), 0) AS needs_owner_review_share
FROM {{ ref('fct_monthly_cost') }}
GROUP BY cost_month, currency
