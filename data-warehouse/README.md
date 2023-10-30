# Data Warehouse

## Getting Started

TODO

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## Jinja Templated SQL Style Guide for dbt Project

This style guide provides recommendations for writing Jinja-templated SQL models in a dbt project. Use this guide to create consistent and maintainable SQL code.

### General Guidelines

1. Use lower case for keywords (e.g., `select`, `from`, `where`).
2. Use snake_case for table and column names.
3. Indent each level of the query using 4 spaces.
4. Separate each subquery with a comma and newline.
5. Use meaningful aliases for tables and columns.
6. Use single quotes for string literals.
7. End each statement with a comma, except for the last statement in a subquery or CTE.
8. Place comments at the beginning of the line using two dashes `--`.

### Jinja Template Guidelines

1. Use double curly braces `{{ }}` for Jinja expressions.
2. Use `import` for importing sources and assigning aliases.
3. Place Jinja control structures (`{% for %}`, `{% if %}`, etc.) on a separate line and align with the surrounding SQL.
4. Use a comma followed by a newline (`{{ '\n' }}`) to separate elements within Jinja loops or conditionals.
5. Use `-` (dash) in Jinja tags to control whitespace (e.g., `{%- for ... -%}`).

### Example Template

Below is an example of a Jinja-templated SQL model following this style guide:

```sql
with
{{ import(source('schema_name', 'table_a'), 'table_a_alias') }},
{{ import(source('schema_name', 'table_b'), 'table_b_alias') }},

cte_example as (
    select
        table_a_alias.column_a,
        table_b_alias.column_b
    from table_a_alias
    join table_b_alias on table_a_alias.join_key = table_b_alias.join_key
    where table_a_alias.column_c in (
    {% for value in var('values', []) -%}
        '{{ value }}'
        {%- if not loop.last %},{{ '\n' }}{% endif %}
    {%- endfor %}
    )
),

final as (
    select
        cte_example.column_a,
        cte_example.column_b
    from cte_example
)

```

### CASE Statements

1. Use proper indentation for each `when` and `then` clause.
2. Align the `when`, `then`, and `end` keywords vertically.
3. Place each `when` and `then` clause on separate lines.
4. Use a consistent column alias format for each output value.

Example:

```sql
    select
        category,
        value,
        case
            when category = 'A' then 'Category A'
            when category = 'B' then 'Category B'
            when category = 'C' then 'Category C'
        end as category_description
    from sample_data
```

### Window Functions

1. Place each window function parameter on a separate line and use proper indentation.
2. Align the `over` keyword with the function name.
3. Align the `partition by`, `order by`, and other window function parameters vertically.

Example:

```sql
row_number() over (
    partition by opportunity_id, status
    order by map_transmission_date
) as map_send_sequence,
```

### Function Calls

1. When function calls include complex expressions or multiple lines, place each argument on a separate line and use proper indentation.
2. Align the opening and closing parentheses with the function name.

Example:

```sql
convert_timezone(
    'UTC',
    timestamp 'epoch' + document.completeddatetime::bigint / 1000 * interval '1 second'
) as map_transmission_date,
```
