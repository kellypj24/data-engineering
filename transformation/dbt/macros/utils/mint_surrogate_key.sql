{% macro surrogate_key_version() -%}
    {#-
        The hash-policy version baked into every key mint_surrogate_key emits.

        Bump it whenever the encoding below changes. Store it in a
        `key_hash_version` column on any table whose rows are not all minted in
        one run, so rows minted under an older policy can be found and migrated
        (see backfill_surrogate_keys).
    -#}
    {{- return(1) -}}
{%- endmacro %}


{% macro mint_surrogate_key(fields, null_as=none) -%}
    {#-
        Mint a UUID-shaped surrogate key from one MD5 over an unambiguous
        encoding of `fields`.

        Usage:
            SELECT
                {{ mint_surrogate_key(['order_id', 'line_number']) }} AS order_line_key,
                {{ mint_surrogate_key(['coupon_code'], null_as='N/A') }} AS coupon_key

        Encoding (version 1), per field, in the order given:
            NULL      -> '~'
            otherwise -> '<length>:<value>'   e.g. 'a|b' -> '3:a|b'
        prefixed with 'v<version>|' and hashed once.

        Why not dbt_utils.generate_surrogate_key:
            - It coalesces NULL to a placeholder string, so NULL collides with
              that string. Here NULL is '~', and no encoded value can start
              with '~' (every value starts with a digit), so NULL is distinct
              from every string, including '' ('0:') and '~' ('1:~').
            - It joins fields with a delimiter, so ('a|b', 'c') and
              ('a', 'b|c') can collide. Length-prefixing removes that outright.
            - It has no version, so a policy change is invisible.

        null_as: when set, NULL in any field is replaced by this literal before
        encoding, so a nullable foreign key resolves to a real "not applicable"
        dimension member instead of NULL (which a `relationships` test passes
        silently). The key then equals the key of the literal itself. Leave it
        unset for identity keys.

        Fields are hashed in a FIXED ORDER. Changing the order, the encoding,
        or the version re-mints every key: that is a version bump, migrated
        with backfill_surrogate_keys. There is deliberately no parameter to
        mint an older version.

        Values are cast to the adapter's string type. Keys are stable within a
        warehouse; across warehouses they match only where the string cast of
        each value matches (true for strings and integers, not in general for
        floats or timestamps -- cast those explicitly first).
    -#}
    {%- if fields is string or fields is not iterable or fields | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "mint_surrogate_key: `fields` must be a non-empty list of column expressions, got " ~ fields
        ) }}
    {%- endif -%}
    {{- return(adapter.dispatch('mint_surrogate_key', 'data_warehouse')(fields, null_as)) -}}
{%- endmacro %}


{% macro _surrogate_key_payload(fields, null_as) -%}
    {%- set parts = ["'v" ~ surrogate_key_version() ~ "|'"] -%}
    {%- for field in fields -%}
        {%- if null_as is not none -%}
            {%- set value = "COALESCE(CAST(" ~ field ~ " AS " ~ dbt.type_string() ~ "), '" ~ (null_as | replace("'", "''")) ~ "')" -%}
        {%- else -%}
            {%- set value = "CAST(" ~ field ~ " AS " ~ dbt.type_string() ~ ")" -%}
        {%- endif -%}
        {%- do parts.append(
            "CASE WHEN " ~ value ~ " IS NULL THEN '~' ELSE CAST(LENGTH(" ~ value ~ ") AS "
            ~ dbt.type_string() ~ ") || ':' || " ~ value ~ " END"
        ) -%}
    {%- endfor -%}
    {{- parts | join(' || ') -}}
{%- endmacro %}


{#-
    The hash is formatted with one REGEXP_REPLACE so MD5 is evaluated once.
    Only the backreference escaping differs between adapters.
-#}

{% macro default__mint_surrogate_key(fields, null_as) -%}
    REGEXP_REPLACE(
        MD5({{ _surrogate_key_payload(fields, null_as) }}),
        '^(.{8})(.{4})(.{4})(.{4})(.{12})$',
        '\1-\2-\3-\4-\5'
    )
{%- endmacro %}


{% macro snowflake__mint_surrogate_key(fields, null_as) -%}
    REGEXP_REPLACE(
        MD5({{ _surrogate_key_payload(fields, null_as) }}),
        '^(.{8})(.{4})(.{4})(.{4})(.{12})$',
        '\\1-\\2-\\3-\\4-\\5'
    )
{%- endmacro %}


{% macro bigquery__mint_surrogate_key(fields, null_as) -%}
    REGEXP_REPLACE(
        TO_HEX(MD5({{ _surrogate_key_payload(fields, null_as) }})),
        r'^(.{8})(.{4})(.{4})(.{4})(.{12})$',
        r'\1-\2-\3-\4-\5'
    )
{%- endmacro %}
