{#-
    Snowflake extra for row_content_hash (macros/utils/row_content_hash.sql).

    ARRAY, VARIANT, and OBJECT values cannot be compared with `=`, which is
    what a `check` snapshot does to every check column. Hashing them avoids
    that, but only if the string form is canonical:
        * ARRAY  -> TO_JSON(ARRAY_SORT(x)), or TO_JSON(x) in keep_array_order
        * VARIANT / OBJECT -> TO_JSON(x). Snowflake stores OBJECT keys in
          sorted order, so key order in the input is not a change.
    The relation form finds these types itself. In the list form, name
    ARRAY columns in `arrays`; VARIANT/OBJECT columns need the relation form.

    Rendered and checked on duckdb (tests/python/test_row_content_hash.py),
    never executed: CI has no Snowflake account.
-#}

{% macro snowflake___content_hash_kind(data_type) -%}
    {%- set t = data_type | upper | trim -%}
    {%- if t == 'ARRAY' or t.startswith('ARRAY(') -%}
        {{- return('array') -}}
    {%- elif t in ['VARIANT', 'OBJECT'] or t.startswith('OBJECT(') -%}
        {{- return('semi') -}}
    {%- endif -%}
    {{- return('scalar') -}}
{%- endmacro %}


{% macro snowflake___content_hash_value(expr, kind, sorted) -%}
    {%- if kind == 'array' and sorted -%}
        TO_JSON(ARRAY_SORT({{ expr }}))
    {%- elif kind in ['array', 'semi'] -%}
        TO_JSON({{ expr }})
    {%- else -%}
        CAST({{ expr }} AS VARCHAR)
    {%- endif -%}
{%- endmacro %}
