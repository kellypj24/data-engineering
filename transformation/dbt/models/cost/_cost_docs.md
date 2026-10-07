{% docs cost_month %}
Month of the cost, as its first day.
{% enddocs %}

{% docs cost_team %}
Team that owns the cost: a team from the mapping seeds, `SHARED` (idle time,
cloud services, other usage), or `NEEDS_OWNER_REVIEW` (no mapped owner).
{% enddocs %}

{% docs cost_workload %}
What the spend was for: a workload from `cost_user_mapping`, or one of the
allocation's buckets `IDLE`, `CLOUD_SERVICES`, `STORAGE`, `OTHER`,
`NEEDS_OWNER_REVIEW`.
{% enddocs %}

{% docs cost_warehouse_name %}
Warehouse the compute ran on; `NONE` for storage, cloud services, and other
usage.
{% enddocs %}
