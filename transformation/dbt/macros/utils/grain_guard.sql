{% macro grain_guard(relation, grain, order_by) -%}
    {#-
        Dedup `relation` to one row per `grain`, and keep the evidence.

        Deduping to a declared grain makes `unique` pass by construction: a
        real key collision upstream (two source rows claiming one order_id)
        becomes an arbitrary pick and every test stays green. This keeps the
        pre-dedup count as `_grain_rows`, so `grain_guard_is_one` (warn by
        default) still reports the collision.

        Usage, with `relation` a ref/source or a CTE name:
            deduped AS (
                {{ grain_guard('renamed', grain=['order_id'], order_by=['created_at DESC', 'status']) }}
            )

        order_by is the tie-break, and is required: it must fully order the
        rows of one grain using STABLE columns. A volatile column (a load
        timestamp, a last_updated heartbeat) makes the kept row change from
        run to run, and every snapshot downstream opens spurious versions. The
        audit columns this project adds (_loaded_at, _dbt_updated_at) and any
        in the `grain_guard_volatile_columns` var are refused.

        Rows with a NULL grain value group together, so two of them count as
        a collision; not_null reports the NULL itself.

        Keep `_grain_rows` out of snapshots and marts: select the columns you
        need, or `{{ star_except(ref('<model>'), ['_grain_rows']) }}`.

        duckdb and Snowflake use QUALIFY; other adapters a ROW_NUMBER()
        subquery (ANSI). Both give the same rows.
    -#}
    {%- set grain = [grain] if grain is string else grain -%}
    {%- set order_by = [order_by] if order_by is string else order_by -%}
    {%- if not grain -%}
        {{ exceptions.raise_compiler_error("grain_guard: `grain` must name at least one column") }}
    {%- endif -%}
    {%- if not order_by -%}
        {{ exceptions.raise_compiler_error(
            "grain_guard: `order_by` is required: a tie-break that fully orders the rows of one grain"
        ) }}
    {%- endif -%}
    {%- set volatile = ['_loaded_at', '_dbt_updated_at'] + var('grain_guard_volatile_columns', []) -%}
    {%- for term in order_by -%}
        {%- set column = term.split() | first | lower -%}
        {%- if column in volatile | map('lower') | list -%}
            {{ exceptions.raise_compiler_error(
                "grain_guard: `" ~ column ~ "` is volatile and cannot be a tie-break: the kept row "
                ~ "would change from run to run. Order by stable columns."
            ) }}
        {%- endif -%}
    {%- endfor -%}
    {{- return(adapter.dispatch('grain_guard', 'data_warehouse')(relation, grain, order_by)) -}}
{%- endmacro %}


{#-
    ANSI: `_grain_rows` is set on the kept row only (NULL elsewhere), and the
    outer query keeps those rows, so no helper rank column leaks through `*`
    and the columns match the QUALIFY form exactly.
-#}
{% macro default__grain_guard(relation, grain, order_by) -%}
    SELECT *
    FROM (
        SELECT
            *,
            CASE
                WHEN ROW_NUMBER() OVER (PARTITION BY {{ grain | join(', ') }} ORDER BY {{ order_by | join(', ') }}) = 1
                    THEN COUNT(*) OVER (PARTITION BY {{ grain | join(', ') }})
            END AS _grain_rows
        FROM {{ relation }}
    ) AS ranked
    WHERE _grain_rows IS NOT NULL
{%- endmacro %}


{% macro duckdb__grain_guard(relation, grain, order_by) -%}
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY {{ grain | join(', ') }}) AS _grain_rows
    FROM {{ relation }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ grain | join(', ') }} ORDER BY {{ order_by | join(', ') }}) = 1
{%- endmacro %}

{% macro snowflake__grain_guard(relation, grain, order_by) -%}
    {{ duckdb__grain_guard(relation, grain, order_by) }}
{%- endmacro %}
