{% macro row_content_hash_version() -%}
    {#-
        The encoding version baked into every row_content_hash. Separate from
        surrogate_key_version(): changing the key policy must not re-version
        every snapshot row, and vice versa. Bumping it changes every hash, so
        every snapshot using it opens one new version per row.
    -#}
    {{- return(1) -}}
{%- endmacro %}


{% macro row_content_hash(columns, exclude=[], arrays=[], keep_array_order=[]) -%}
    {#-
        One MD5 (32 lowercase hex chars) over a row's content, for use as the
        single `check_cols` column of a `check` snapshot.

        Usage:
            SELECT
                id,
                email,
                {{ row_content_hash(source('raw', 'customers'), exclude=['_loaded_at']) }} AS content_hash
            FROM {{ source('raw', 'customers') }}

            -- snapshot config: strategy='check', check_cols=['content_hash']

        `columns` is either
            * a relation: every column, read from the warehouse at compile
              time, minus `exclude`. Array columns are found from their types.
              ADDING a column to the relation changes every row's hash once,
              so every row gets one new snapshot version. For wide sources
              whose columns change often, pass an explicit list instead.
            * a list of column names (expressions are allowed). Name array
              columns in `arrays`.

        Rules:
            * Columns are taken in sorted (case-insensitive) name order, so
              reordering columns is not a change.
            * `exclude` drops volatile columns (load timestamps, last_updated)
              by name, case-insensitively.
            * Arrays are sorted before encoding, so element order is not a
              change. Name order-significant arrays (a ranking) in
              `keep_array_order`.
            * Each value is cast to a string and length-framed exactly as in
              mint_surrogate_key (_length_framed): NULL and '' differ, and no
              delimiter can collide. Prefixed with 'h<version>|'.

        Adapters: duckdb (default, tested). Snowflake in macros/snowflake/:
        ARRAY and VARIANT/OBJECT are serialised, since they cannot be compared
        with `=` (rendered in tests, not executed). Others get scalar columns
        only, and naming an array is a compile error rather than a silent
        wrong answer.
    -#}
    {%- if not execute -%}
        {#- Parsing: relations are not resolved yet, and the SQL is not used. -#}
        {{- return("''") -}}
    {%- endif -%}
    {%- set exclude_lower = exclude | map('lower') | list -%}
    {%- set keep_lower = keep_array_order | map('lower') | list -%}
    {%- set array_lower = arrays | map('lower') | list -%}
    {%- set entries = [] -%}

    {%- if columns is not string and columns.identifier is defined -%}
        {#- A relation (it is iterable in Jinja, so test for an attribute). -#}
        {%- for column in adapter.get_columns_in_relation(columns) -%}
            {%- if column.name | lower not in exclude_lower -%}
                {%- do entries.append({
                    'name': column.name,
                    'expr': adapter.quote(column.name),
                    'kind': adapter.dispatch('_content_hash_kind', 'data_warehouse')(column.data_type),
                }) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- else -%}
        {%- if columns is string or columns is not iterable or columns | length == 0 -%}
            {{ exceptions.raise_compiler_error(
                "row_content_hash: `columns` must be a relation or a non-empty list of columns, got " ~ columns
            ) }}
        {%- endif -%}
        {%- for column in columns if column | lower not in exclude_lower -%}
            {%- do entries.append({
                'name': column,
                'expr': column,
                'kind': 'array' if column | lower in array_lower else 'scalar',
            }) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if entries | length == 0 -%}
        {{ exceptions.raise_compiler_error("row_content_hash: every column was excluded") }}
    {%- endif -%}

    {%- set parts = ["'h" ~ row_content_hash_version() ~ "|'"] -%}
    {%- for entry in entries | sort(attribute='name', case_sensitive=false) -%}
        {%- set sorted = entry.kind == 'array' and entry.name | lower not in keep_lower -%}
        {%- set value = adapter.dispatch('_content_hash_value', 'data_warehouse')(entry.expr, entry.kind, sorted) -%}
        {%- do parts.append(_length_framed(value)) -%}
    {%- endfor -%}
    {{- adapter.dispatch('_content_hash_md5', 'data_warehouse')(parts | join(' || ')) -}}
{%- endmacro %}


{#- Column type -> 'scalar' | 'array' | 'semi' (Snowflake VARIANT/OBJECT). -#}

{% macro default___content_hash_kind(data_type) -%}
    {{- return('scalar') -}}
{%- endmacro %}

{% macro duckdb___content_hash_kind(data_type) -%}
    {%- set t = data_type | upper | trim -%}
    {{- return('array' if t.endswith(']') or t.startswith('LIST') else 'scalar') -}}
{%- endmacro %}


{#- One column as a string expression, arrays sorted when `sorted`. -#}

{% macro default___content_hash_value(expr, kind, sorted) -%}
    {%- if kind != 'scalar' -%}
        {{ exceptions.raise_compiler_error(
            "row_content_hash: array columns are not supported on " ~ target.type ~ " (" ~ expr ~ ")"
        ) }}
    {%- endif -%}
    CAST({{ expr }} AS {{ dbt.type_string() }})
{%- endmacro %}

{% macro duckdb___content_hash_value(expr, kind, sorted) -%}
    {%- if sorted -%}
        CAST(LIST_SORT({{ expr }}) AS VARCHAR)
    {%- else -%}
        CAST({{ expr }} AS VARCHAR)
    {%- endif -%}
{%- endmacro %}


{% macro default___content_hash_md5(payload) -%}
    MD5({{ payload }})
{%- endmacro %}

{% macro bigquery___content_hash_md5(payload) -%}
    TO_HEX(MD5({{ payload }}))
{%- endmacro %}
