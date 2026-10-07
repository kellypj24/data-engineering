{#-
    Snowflake extra. Generate a `CREATE SEMANTIC VIEW` for one mart domain from
    the dbt graph, so natural-language query tools read curated marts instead
    of raw tables.

        dbt run-operation generate_semantic_view --args '{domain: revenue}'                 # print DDL
        dbt run-operation generate_semantic_view --args '{domain: revenue, dry_run: false}' # create it

    A domain is a workload tag (docs/patterns/tagging.md): every enabled model
    tagged both `marts` and <domain> becomes one logical table. The models must
    be built: column types are read from the warehouse.

    GENERATED here, from the dbt graph -- regenerate after every model change:
      * TABLES       one per model, with PRIMARY KEY from the model's uniqueness
                     test (`unique`, or dbt_utils.unique_combination_of_columns)
      * FACTS        documented numeric columns that are not keys
      * DIMENSIONS   everything else documented: text, dates, and keys (the
                     primary key, and *_id / *_key columns), which identify
                     rather than measure
      * COMMENT      every table and column comment, from its dbt description
    Only DOCUMENTED columns are exposed: an undocumented column has no
    description to give the query tool. A column with `meta: {sensitive: true}`
    is never exposed.

    HAND-CURATED, never generated -- from the `semantic_views` var, per domain:
      * metrics        list of `<table>.<name> AS <aggregate>` entries
      * relationships  list of `<name> AS <table> (<col>) REFERENCES <table>`
      * synonyms       {<table or table.column>: [synonym, ...]}
    These encode business meaning (what "revenue" sums, which join is valid)
    that column metadata cannot supply, so a generator must not guess them.
    Snowflake needs at least one dimension or metric.
-#}

{% macro semantic_view_ddl(domain) %}
    {%- set models = [] -%}
    {%- for node in graph.nodes.values()
          if node.resource_type == 'model' and 'marts' in node.tags and domain in node.tags -%}
        {%- do models.append(node) -%}
    {%- endfor -%}
    {%- if not models -%}
        {{ exceptions.raise_compiler_error("generate_semantic_view: no models tagged both marts and " ~ domain) }}
    {%- endif -%}

    {%- set curated = var('semantic_views', {}).get(domain, {}) -%}
    {%- set synonyms = curated.get('synonyms', {}) -%}
    {%- set tables, facts, dimensions = [], [], [] -%}

    {%- for node in models | sort(attribute='name') -%}
        {%- set relation = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) -%}
        {%- set built = adapter.get_columns_in_relation(relation) -%}
        {%- if not built -%}
            {{ exceptions.raise_compiler_error("generate_semantic_view: " ~ node.name ~ " is not built; run dbt build first") }}
        {%- endif -%}
        {%- set types = {} -%}
        {%- for col in built -%}
            {%- do types.update({col.name | lower: col}) -%}
        {%- endfor -%}

        {%- set key = semantic_view_primary_key(node) -%}
        {%- set table_sql = node.name ~ ' AS ' ~ relation
            ~ (' PRIMARY KEY (' ~ key | join(', ') ~ ')' if key else '')
            ~ _semantic_view_synonyms(synonyms.get(node.name))
            ~ " COMMENT = '" ~ _semantic_view_text(node.description) ~ "'" -%}
        {%- do tables.append(table_sql) -%}

        {%- for name, column in node.columns.items()
              if not (column.meta or {}).get('sensitive') and name | lower in types -%}
            {%- set entry = node.name ~ '.' ~ name ~ _semantic_view_synonyms(synonyms.get(node.name ~ '.' ~ name))
                ~ ' AS ' ~ name ~ " COMMENT = '" ~ _semantic_view_text(column.description) ~ "'" -%}
            {%- set is_key = name in key or (name | lower).endswith('_id') or (name | lower).endswith('_key') -%}
            {%- if not is_key and _semantic_view_is_numeric(types[name | lower].data_type) -%}
                {%- do facts.append(entry) -%}
            {%- else -%}
                {%- do dimensions.append(entry) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- set schema = var('semantic_view_schema', 'semantic') -%}
    {%- set lines = ['CREATE OR REPLACE SEMANTIC VIEW ' ~ target.database ~ '.' ~ schema ~ '.' ~ domain ~ '_semantic_view'] -%}
    {%- do lines.append('  TABLES (\n    ' ~ tables | join(',\n    ') ~ '\n  )') -%}
    {%- if curated.get('relationships') -%}
        {%- do lines.append('  RELATIONSHIPS (\n    ' ~ curated.relationships | join(',\n    ') ~ '\n  )') -%}
    {%- endif -%}
    {%- if facts -%}
        {%- do lines.append('  FACTS (\n    ' ~ facts | join(',\n    ') ~ '\n  )') -%}
    {%- endif -%}
    {%- if dimensions -%}
        {%- do lines.append('  DIMENSIONS (\n    ' ~ dimensions | join(',\n    ') ~ '\n  )') -%}
    {%- endif -%}
    {%- if curated.get('metrics') -%}
        {%- do lines.append('  METRICS (\n    ' ~ curated.metrics | join(',\n    ') ~ '\n  )') -%}
    {%- endif -%}
    {%- do lines.append("  COMMENT = 'Generated by generate_semantic_view for domain " ~ domain ~ "; metrics, relationships, and synonyms are hand-curated in the semantic_views var.'") -%}
    {{ return(lines | join('\n')) }}
{% endmacro %}


{#- Primary key from the model's uniqueness test: the grain (the dbt-test skill). -#}
{% macro semantic_view_primary_key(node) %}
    {%- for test in graph.nodes.values() if test.resource_type == 'test' and test.attached_node == node.unique_id -%}
        {%- set meta = test.test_metadata or {} -%}
        {%- if meta.get('name') == 'unique_combination_of_columns' -%}
            {{ return(meta.kwargs.combination_of_columns) }}
        {%- endif -%}
    {%- endfor -%}
    {%- for test in graph.nodes.values() if test.resource_type == 'test' and test.attached_node == node.unique_id -%}
        {%- if (test.test_metadata or {}).get('name') == 'unique' -%}
            {{ return([test.column_name]) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return([]) }}
{% endmacro %}


{#- By type name, lowercased: adapters differ in case and in which types
    dbt's Column.is_numeric() recognises (duckdb's DECIMAL(p,s) is missed). -#}
{% macro _semantic_view_is_numeric(data_type) %}
    {%- set base = (data_type or '') | lower | trim -%}
    {%- for prefix in ['number', 'numeric', 'decimal', 'int', 'bigint', 'smallint', 'tinyint', 'hugeint', 'float', 'double', 'real'] -%}
        {%- if base.startswith(prefix) -%}
            {{ return(true) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(false) }}
{% endmacro %}


{% macro _semantic_view_text(text) %}
    {{- return((text or '') | replace('\n', ' ') | replace("'", "''") | trim) -}}
{% endmacro %}


{% macro _semantic_view_synonyms(values) %}
    {%- if not values -%}
        {{ return('') }}
    {%- endif -%}
    {%- set quoted = [] -%}
    {%- for value in values -%}
        {%- do quoted.append("'" ~ _semantic_view_text(value) ~ "'") -%}
    {%- endfor -%}
    {{ return(' WITH SYNONYMS (' ~ quoted | join(', ') ~ ')') }}
{% endmacro %}


{% macro generate_semantic_view(domain, dry_run=true) %}
    {%- if not dry_run and adapter.type() != 'snowflake' -%}
        {{ exceptions.raise_compiler_error("generate_semantic_view creates a Snowflake SEMANTIC VIEW; adapter is " ~ adapter.type() ~ ". Use dry_run.") }}
    {%- endif -%}
    {%- set ddl = semantic_view_ddl(domain) -%}
    {{ log(ddl, info=true) }}
    {%- if not dry_run -%}
        {%- do run_query('CREATE SCHEMA IF NOT EXISTS ' ~ target.database ~ '.' ~ var('semantic_view_schema', 'semantic')) -%}
        {%- do run_query(ddl) -%}
        {%- do adapter.commit() -%}
    {%- endif -%}
{% endmacro %}
