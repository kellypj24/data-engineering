{% macro audit_columns(loaded_at_column=none) -%}
    {#-
        Add standard audit timestamp columns to staging models.

        - _loaded_at: When the row was first loaded into the warehouse by the
          EL tool. Pass the source column name to preserve the original
          timestamp (e.g., `_airbyte_extracted_at`). Falls back to
          CURRENT_TIMESTAMP() if no source column is available.
        - _dbt_updated_at: When this row was last processed by dbt.

        Usage:
            SELECT
                id,
                name,
                {{ audit_columns('_airbyte_extracted_at') }}
            FROM {{ source('raw', 'customers') }}

            -- Or without a source timestamp:
            {{ audit_columns() }}
    -#}
    {%- if loaded_at_column is not none -%}
        {{ loaded_at_column }} AS _loaded_at,
    {%- else -%}
        CURRENT_TIMESTAMP() AS _loaded_at,
    {%- endif %}
    CURRENT_TIMESTAMP() AS _dbt_updated_at
{%- endmacro %}
