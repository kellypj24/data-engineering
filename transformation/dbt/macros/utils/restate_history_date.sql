{#-
    Repair one frozen date in a history table stamped with `current_date`:
    replace its rows with a neighbouring date's, restamped. Dry run by
    default: logs the statements and writes nothing.

        dbt run-operation restate_history_date --args '{
            relation: analytics.customer_history, date_column: snapshot_date,
            bad_date: 2026-10-14, from_date: 2026-10-15, dry_run: false}'

    `from_date` is usually the next day: the frozen day kept its first run's
    state, and the next day is closer to what it ended as. The copy is an
    approximation; record the restatement (docs/patterns/state-migrations.md).
    Find frozen dates with the `no_frozen_days` test.

    The DELETE and INSERT share one transaction, committed after the INSERT.
    On adapters that autocommit each statement (Snowflake), a failure between
    them leaves `bad_date` empty; re-run the operation, which is idempotent
    because `from_date` is never touched.
-#}
{% macro restate_history_date(relation, date_column, bad_date, from_date, dry_run=true) %}
    {%- if bad_date | string == from_date | string -%}
        {{ exceptions.raise_compiler_error("restate_history_date: bad_date and from_date are the same") }}
    {%- endif -%}
    {%- set parts = relation.split('.') -%}
    {%- set target_relation = adapter.get_relation(
        database=parts[-3] if parts | length == 3 else target.database,
        schema=parts[-2],
        identifier=parts[-1],
    ) -%}
    {%- if target_relation is none -%}
        {{ exceptions.raise_compiler_error("restate_history_date: " ~ relation ~ " does not exist") }}
    {%- endif -%}
    {%- set columns = adapter.get_columns_in_relation(target_relation) | map(attribute='name') | list -%}
    {%- set select_list = [] -%}
    {%- for column in columns -%}
        {%- do select_list.append("CAST('" ~ bad_date ~ "' AS DATE)" if column | lower == date_column | lower else column) -%}
    {%- endfor -%}
    {%- set statements = [
        "DELETE FROM " ~ target_relation ~ " WHERE " ~ date_column ~ " = CAST('" ~ bad_date ~ "' AS DATE)",
        "INSERT INTO " ~ target_relation ~ " (" ~ columns | join(', ') ~ ") SELECT " ~ select_list | join(', ')
            ~ " FROM " ~ target_relation ~ " WHERE " ~ date_column ~ " = CAST('" ~ from_date ~ "' AS DATE)",
    ] -%}
    {%- for statement in statements -%}
        {{ log(("DRY RUN " if dry_run else "") ~ "restate_history_date: " ~ statement, info=true) }}
    {%- endfor -%}
    {%- if not dry_run -%}
        {%- for statement in statements -%}
            {%- do run_query(statement) -%}
        {%- endfor -%}
        {%- do adapter.commit() -%}
    {%- endif -%}
{% endmacro %}
