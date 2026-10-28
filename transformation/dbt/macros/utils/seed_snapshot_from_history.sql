{% macro seed_snapshot_from_history(
    snapshot,
    history,
    current,
    key,
    exclude=[],
    allow_mismatch=false,
    dry_run=true
) -%}
    {#-
        One-time cutover: create a snapshot's table from reconstructed history
        (reconstruct_scd2) BEFORE the snapshot's first run, so `dbt snapshot`
        continues that history instead of starting it. See
        docs/patterns/snapshots.md.

        Usage (dry run is the default -- it checks the seam, logs the DDL, and
        writes nothing):
            dbt run-operation seed_snapshot_from_history --args '{
                snapshot: customers_snapshot,
                history: int_customers_history,
                current: raw.customers,
                key: id,
                dry_run: false
            }'

        Arguments:
            snapshot        Snapshot name. Its table must NOT exist yet: a
                            snapshot table is the only copy of its history,
                            so this refuses to replace one.
            history         Model built with reconstruct_scd2, already built.
            current         What the snapshot reads: a model name or
                            'source_name.table_name'.
            key             The snapshot's unique_key (a column or a list).
            exclude         The columns the snapshot's row_content_hash
                            excludes. Must match the reconstruction's exclude.
            allow_mismatch  Seed even if the seam check finds differences.
            dry_run         Defaults to true.

        The seam check: every open version's content_hash must equal the
        row_content_hash of the same key in `current`, and the key sets must
        match. A difference means the snapshot's first run will version those
        rows: a real change since the last extract, or a hash computed over
        different columns. It is listed, and refused unless allow_mismatch.

        The snapshot must select the history's columns: the content columns
        plus `content_hash` (its check_cols), with the same names.
    -#}
    {%- if not execute -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set keys = [key] if key is string else key -%}
    {%- set snapshot_relation = _graph_relation(snapshot, 'snapshot') -%}
    {%- set history_relation = _graph_relation(history, 'model') -%}
    {%- set current_relation = _graph_relation(current, 'model') -%}

    {%- if adapter.get_relation(
        database=history_relation.database, schema=history_relation.schema, identifier=history_relation.identifier
    ) is none -%}
        {{ exceptions.raise_compiler_error(
            "seed_snapshot_from_history: " ~ history_relation ~ " does not exist; build " ~ history ~ " first"
        ) }}
    {%- endif -%}
    {%- if adapter.get_relation(
        database=snapshot_relation.database, schema=snapshot_relation.schema, identifier=snapshot_relation.identifier
    ) is not none -%}
        {{ exceptions.raise_compiler_error(
            "seed_snapshot_from_history: " ~ snapshot_relation ~ " already exists. A snapshot table is the "
            ~ "only copy of its history; this never replaces one."
        ) }}
    {%- endif -%}

    {%- set seam_sql -%}
        WITH history_open AS (
            SELECT {{ keys | join(', ') }}, content_hash
            FROM {{ history_relation }}
            WHERE dbt_valid_to IS NULL
        ),

        current_rows AS (
            SELECT {{ keys | join(', ') }}, {{ row_content_hash(current_relation, exclude=exclude) }} AS content_hash
            FROM {{ current_relation }}
        )

        SELECT
            {% for k in keys -%}
            COALESCE(h.{{ k }}, c.{{ k }}) AS {{ k }},
            {% endfor -%}
            CASE
                WHEN h.content_hash IS NULL THEN 'only in current'
                WHEN c.content_hash IS NULL THEN 'only in history'
                ELSE 'content differs'
            END AS seam
        FROM history_open AS h
        FULL OUTER JOIN current_rows AS c
            ON {% for k in keys %}h.{{ k }} = c.{{ k }}{% if not loop.last %} AND {% endif %}{% endfor %}
        WHERE h.content_hash IS NULL OR c.content_hash IS NULL OR h.content_hash <> c.content_hash
        ORDER BY 1
    {%- endset -%}
    {%- set mismatches = run_query(seam_sql) -%}
    {%- if mismatches.rows | length == 0 -%}
        {{ log("seed_snapshot_from_history: seam check passed: every open version matches " ~ current_relation, info=true) }}
    {%- else -%}
        {{ log("seed_snapshot_from_history: seam check found " ~ (mismatches.rows | length)
            ~ " difference(s); the snapshot's first run will version these:", info=true) }}
        {%- for row in mismatches.rows[:20] -%}
            {{ log("  " ~ (row.values() | join(' | ')), info=true) }}
        {%- endfor -%}
        {%- if not allow_mismatch -%}
            {{ exceptions.raise_compiler_error(
                "seed_snapshot_from_history: seam mismatch. Rebuild the history from an extract matching "
                ~ current ~ ", align the hash columns (exclude), or pass allow_mismatch: true."
            ) }}
        {%- endif -%}
    {%- endif -%}

    {%- set ddl = "CREATE TABLE " ~ snapshot_relation ~ " AS SELECT * FROM " ~ history_relation -%}
    {{ log("seed_snapshot_from_history" ~ (" [dry run]" if dry_run else "") ~ ": " ~ ddl, info=true) }}
    {%- if dry_run -%}
        {{ log("seed_snapshot_from_history: dry run -- nothing written. Pass dry_run: false to seed.", info=true) }}
        {{ return(none) }}
    {%- endif -%}
    {%- do adapter.create_schema(snapshot_relation) -%}
    {%- do run_query(ddl) -%}
    {%- do adapter.commit() -%}
    {%- set seeded = run_query("SELECT COUNT(*) FROM " ~ snapshot_relation).columns[0].values()[0] -%}
    {{ log("seed_snapshot_from_history: seeded " ~ snapshot_relation ~ " with " ~ seeded ~ " version(s).", info=true) }}
{%- endmacro %}


{% macro _graph_relation(name, resource_type) -%}
    {#- A snapshot or model by name, or a source as 'source_name.table_name'. -#}
    {%- if '.' in name -%}
        {%- set source_name, table_name = name.split('.', 1) -%}
        {%- for node in graph.sources.values() if node.source_name == source_name and node.name == table_name -%}
            {{- return(api.Relation.create(database=node.database, schema=node.schema, identifier=node.identifier)) -}}
        {%- endfor -%}
    {%- else -%}
        {%- for node in graph.nodes.values() if node.resource_type == resource_type and node.name == name -%}
            {{- return(api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias)) -}}
        {%- endfor -%}
    {%- endif -%}
    {{ exceptions.raise_compiler_error("seed_snapshot_from_history: no " ~ resource_type ~ " or source named " ~ name) }}
{%- endmacro %}
