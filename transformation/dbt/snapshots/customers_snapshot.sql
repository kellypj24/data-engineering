{#-
    Type-2 history of customers. See docs/patterns/snapshots.md.

    Strategy `check`: raw.customers has no trustworthy updated_at, so a change
    is detected by comparing the listed columns. The list is explicit and
    stable; `check_cols='all'` would turn every added column into a spurious
    new version of every row.

    hard_deletes='invalidate' (dbt >= 1.9): a customer that disappears from the
    source gets its current row closed (dbt_valid_to set), so "who existed on
    date t" stays answerable. On dbt 1.8 the equivalent is
    invalidate_hard_deletes=true.

    History starts at the first run. A snapshot of a current-state table cannot
    reconstruct anything before that.
-#}

{% snapshot customers_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='check',
    check_cols=['email'],
    hard_deletes='invalidate',
) }}

SELECT
    id,
    email
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
