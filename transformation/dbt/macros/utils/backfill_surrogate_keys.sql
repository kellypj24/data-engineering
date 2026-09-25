{% macro backfill_surrogate_keys(
    relation,
    key_expressions,
    dependent_keys=none,
    version_column=none,
    scope_predicate=none,
    dry_run=true
) -%}
    {#-
        Fill or upgrade surrogate-key columns IN PLACE on an existing table,
        without reading any source.

        Why not --full-refresh: adding a key column to an incremental model
        leaves history NULL, and a full refresh re-reads upstream -- which
        silently replaces real history wherever a source keeps less than the
        table. A surrogate key is a pure function of columns already in the
        row, so an UPDATE writes exactly what the model would have.

        Usage (dry run is the default -- it logs the statements and writes nothing):
            dbt run-operation backfill_surrogate_keys --args '{
                relation: analytics.fct_order_lines,
                key_expressions: {order_line_key: [order_id, line_number],
                                  coupon_key: {fields: [coupon_code], null_as: "N/A"}},
                dependent_keys: {order_coupon_key: [order_line_key, coupon_key]},
                version_column: key_hash_version,
                dry_run: false
            }'

        Arguments:
            relation         'schema.table' or 'database.schema.table'.
            key_expressions  {column: spec}, written in ONE UPDATE. A spec is a
                             list of fields (minted with mint_surrogate_key), a
                             mapping {fields: [...], null_as: '...'}, or a raw
                             SQL string.
            dependent_keys   {column: spec} for keys hashed FROM columns that
                             key_expressions writes. Applied in a SECOND
                             UPDATE: a SET list is evaluated against pre-update
                             values, so folding them into the first statement
                             hashes the NULLs being replaced and mints one
                             identical key for every row.
            version_column   Integer column recording surrogate_key_version().
                             Rows where it is NULL or older are in scope. It is
                             written in the LAST statement, so it certifies
                             every pass completed, and a re-run of a finished
                             table touches zero rows. Required with
                             dependent_keys: it is what carries the row scope
                             from the first UPDATE to the second.
            scope_predicate  Optional SQL narrowing the rows touched. Required
                             when version_column is none. Must not reference a
                             column this operation writes, or the second
                             statement would see a different scope.
            dry_run          Defaults to true.

        The statements share one transaction, committed after the last one.
        On adapters that autocommit each statement (Snowflake), a failure
        midway leaves version_column unwritten, so the next run redoes those
        rows.
    -#}
    {%- set dependent_keys = dependent_keys or {} -%}
    {%- set version = surrogate_key_version() -%}

    {%- if not key_expressions -%}
        {{ exceptions.raise_compiler_error("backfill_surrogate_keys: key_expressions is empty") }}
    {%- endif -%}
    {%- if version_column is none and not scope_predicate -%}
        {{ exceptions.raise_compiler_error(
            "backfill_surrogate_keys: pass version_column or scope_predicate -- refusing to rewrite every row unscoped"
        ) }}
    {%- endif -%}
    {%- if dependent_keys and version_column is none -%}
        {{ exceptions.raise_compiler_error(
            "backfill_surrogate_keys: dependent_keys requires version_column to carry the row scope between statements"
        ) }}
    {%- endif -%}

    {%- set written = (key_expressions.keys() | list) + (dependent_keys.keys() | list) -%}
    {%- if version_column is not none -%}
        {%- do written.append(version_column) -%}
    {%- endif -%}
    {%- for column in key_expressions if column in dependent_keys -%}
        {{ exceptions.raise_compiler_error(
            "backfill_surrogate_keys: " ~ column ~ " is in both key_expressions and dependent_keys"
        ) }}
    {%- endfor -%}
    {%- if scope_predicate -%}
        {%- for column in written
            if modules.re.search('\\b' ~ modules.re.escape(column) ~ '\\b', scope_predicate, modules.re.IGNORECASE) -%}
            {{ exceptions.raise_compiler_error(
                "backfill_surrogate_keys: scope_predicate references " ~ column ~ ", which this operation writes"
            ) }}
        {%- endfor -%}
    {%- endif -%}

    {%- set target_relation = _resolve_backfill_relation(relation) -%}

    {%- set scope = [] -%}
    {%- if version_column is not none -%}
        {%- do scope.append("(" ~ version_column ~ " IS NULL OR " ~ version_column ~ " < " ~ version ~ ")") -%}
    {%- endif -%}
    {%- if scope_predicate -%}
        {%- do scope.append("(" ~ scope_predicate ~ ")") -%}
    {%- endif -%}
    {%- set where = scope | join(' AND ') -%}

    {#- The version is written with the last statement, never earlier. -#}
    {%- set passes = [key_expressions] -%}
    {%- if dependent_keys -%}
        {%- do passes.append(dependent_keys) -%}
    {%- endif -%}
    {%- set statements = [] -%}
    {%- for assignments in passes -%}
        {%- set set_list = [] -%}
        {%- for column, spec in assignments.items() -%}
            {%- do set_list.append(column ~ " = " ~ _backfill_key_sql(spec)) -%}
        {%- endfor -%}
        {%- if loop.last and version_column is not none -%}
            {%- do set_list.append(version_column ~ " = " ~ version) -%}
        {%- endif -%}
        {%- do statements.append(
            "UPDATE " ~ target_relation ~ "\nSET\n    " ~ set_list | join(",\n    ") ~ "\nWHERE " ~ where
        ) -%}
    {%- endfor -%}

    {%- set in_scope = run_query("SELECT COUNT(*) FROM " ~ target_relation ~ " WHERE " ~ where).columns[0].values()[0] -%}
    {{ log("backfill_surrogate_keys: " ~ in_scope ~ " row(s) in scope on " ~ target_relation
        ~ " (key version " ~ version ~ ")", info=true) }}

    {%- for statement in statements -%}
        {{ log("backfill_surrogate_keys: statement " ~ loop.index ~ " of " ~ statements | length
            ~ (" [dry run]" if dry_run else "") ~ ":\n" ~ statement, info=true) }}
    {%- endfor -%}

    {%- if dry_run -%}
        {{ log("backfill_surrogate_keys: dry run -- nothing written. Pass dry_run: false to execute.", info=true) }}
    {%- elif in_scope == 0 -%}
        {{ log("backfill_surrogate_keys: nothing to do.", info=true) }}
    {%- else -%}
        {%- for statement in statements -%}
            {%- do run_query(statement) -%}
        {%- endfor -%}
        {#- run-operation does not commit on its own; without this the updates are rolled back. -#}
        {%- do adapter.commit() -%}
        {{ log("backfill_surrogate_keys: updated " ~ in_scope ~ " row(s).", info=true) }}
    {%- endif -%}
{%- endmacro %}


{% macro _resolve_backfill_relation(relation) -%}
    {%- set parts = relation.split('.') -%}
    {%- if parts | length == 2 -%}
        {%- set found = adapter.get_relation(database=target.database, schema=parts[0], identifier=parts[1]) -%}
    {%- elif parts | length == 3 -%}
        {%- set found = adapter.get_relation(database=parts[0], schema=parts[1], identifier=parts[2]) -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "backfill_surrogate_keys: relation must be 'schema.table' or 'database.schema.table', got " ~ relation
        ) }}
    {%- endif -%}
    {%- if found is none -%}
        {{ exceptions.raise_compiler_error("backfill_surrogate_keys: relation " ~ relation ~ " does not exist") }}
    {%- endif -%}
    {{- return(found) -}}
{%- endmacro %}


{% macro _backfill_key_sql(spec) -%}
    {%- if spec is string -%}
        {{- return(spec) -}}
    {%- elif spec is mapping -%}
        {{- return(mint_surrogate_key(spec['fields'], spec.get('null_as'))) -}}
    {%- else -%}
        {{- return(mint_surrogate_key(spec)) -}}
    {%- endif -%}
{%- endmacro %}
