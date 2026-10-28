{% macro reconstruct_scd2(extracts, key, extract_date, exclude=[], calendar=none) -%}
    {#-
        Type-2 history rebuilt from a stack of archived FULL extracts, one row
        per key per extract date, by gaps-and-islands. This is how history
        from before the first snapshot run is recovered (see
        docs/patterns/snapshots.md for the cutover to a go-forward snapshot).

        Usage, as the whole body of a table model:
            {{ config(materialized='table') }}
            {{ reconstruct_scd2(source('raw', 'customer_extracts'), key='id', extract_date='extract_date') }}

        Presence-aware:
            * The calendar is every extract date. A key missing from an
              extract closes its version at that extract's date.
            * A key that reappears opens a NEW version, even if its content
              is unchanged: absence is a fact the history must keep.
            * A day with no extract file is not a deletion: it is not in the
              calendar. A genuinely EMPTY extract has no rows, so it is
              invisible too; if empty extracts can happen, pass `calendar`, a
              relation with one row per extract date in a column named like
              `extract_date`.
            * Keys in the latest extract stay open (dbt_valid_to IS NULL).

        Change detection is row_content_hash over every column except
        `extract_date` and `exclude`, kept as `content_hash`. A go-forward
        snapshot must compute the same hash (same columns, same exclude) so
        the two meet at the cutover; seed_snapshot_from_history checks it.

        Output: the content columns, content_hash, and dbt's snapshot columns
        (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to), as
        timestamps at midnight of the extract date. Assumes one row per key
        per extract: test unique_combination_of_columns on the extracts.

        `key` is a column name or a list of them.
    -#}
    {%- if not execute -%}
        {{- return("SELECT 1 AS placeholder") -}}
    {%- endif -%}
    {%- set keys = [key] if key is string else key -%}
    {%- set key_list = keys | join(', ') -%}
    {%- set ts = dbt.type_timestamp() -%}
    {%- set content_columns = [] -%}
    {%- for column in adapter.get_columns_in_relation(extracts) -%}
        {%- if column.name | lower != extract_date | lower -%}
            {%- do content_columns.append(adapter.quote(column.name)) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if content_columns | length == 0 -%}
        {%- if adapter.get_relation(database=extracts.database, schema=extracts.schema, identifier=extracts.identifier) is none -%}
            {#- Not built yet (e.g. `dbt compile` before seeds/EL). Still read from
                it, so a run against a missing table fails in the warehouse
                instead of building something empty. -#}
            {{- return("SELECT * FROM " ~ extracts ~ " /* reconstruct_scd2: not built yet, columns unknown */") -}}
        {%- endif -%}
        {{ exceptions.raise_compiler_error(
            "reconstruct_scd2: " ~ extracts ~ " has no columns besides " ~ extract_date
        ) }}
    {%- endif -%}

WITH extracts AS (
    SELECT
        {{ content_columns | join(', ') }},
        CAST({{ extract_date }} AS {{ ts }}) AS extract_at,
        {{ row_content_hash(extracts, exclude=[extract_date] + exclude) }} AS content_hash
    FROM {{ extracts }}
),

calendar AS (
    SELECT
        extract_at,
        ROW_NUMBER() OVER (ORDER BY extract_at) AS extract_n
    FROM (
        SELECT DISTINCT CAST({{ extract_date }} AS {{ ts }}) AS extract_at
        FROM {{ calendar if calendar is not none else extracts }}
    ) AS dates
),

observed AS (
    SELECT
        extracts.*,
        calendar.extract_n,
        LAG(calendar.extract_n) OVER (PARTITION BY {{ key_list }} ORDER BY calendar.extract_n) AS prev_extract_n,
        LAG(extracts.content_hash) OVER (PARTITION BY {{ key_list }} ORDER BY calendar.extract_n) AS prev_content_hash
    FROM extracts
    INNER JOIN calendar ON extracts.extract_at = calendar.extract_at
),

islands AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN prev_extract_n IS NULL THEN 1             -- first sighting
                WHEN prev_extract_n <> extract_n - 1 THEN 1    -- back after an absence
                WHEN prev_content_hash <> content_hash THEN 1  -- changed
                ELSE 0
            END
        ) OVER (PARTITION BY {{ key_list }} ORDER BY extract_n ROWS UNBOUNDED PRECEDING) AS island
    FROM observed
),

spans AS (
    SELECT
        {{ key_list }},
        island,
        MIN(extract_n) AS first_extract_n,
        MAX(extract_n) AS last_extract_n
    FROM islands
    GROUP BY {{ key_list }}, island
),

versions AS (
    SELECT
        {% for column in content_columns -%}
        islands.{{ column }},
        {% endfor -%}
        islands.content_hash,
        islands.extract_at AS dbt_valid_from,
        closed.extract_at AS dbt_valid_to
    FROM spans
    INNER JOIN islands
        ON
            {% for k in keys -%}
            islands.{{ k }} = spans.{{ k }} AND
            {% endfor -%}
            islands.island = spans.island
            AND islands.extract_n = spans.first_extract_n
    -- The first extract after the island: the key changed or was missing.
    LEFT JOIN calendar AS closed ON closed.extract_n = spans.last_extract_n + 1
)

SELECT
    {{ content_columns | join(', ') }},
    content_hash,
    {{ mint_surrogate_key(keys + ['dbt_valid_from']) }} AS dbt_scd_id,
    dbt_valid_from AS dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM versions
{%- endmacro %}
