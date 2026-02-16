# Run dbt tests (schema + unit)
test:
    cd {{justfile_directory()}} && uv run dbt test

# Run dbt models
run:
    cd {{justfile_directory()}} && uv run dbt run

# Lint SQL with sqlfluff
lint:
    cd {{justfile_directory()}} && uv run sqlfluff lint models/ macros/

# Fix SQL with sqlfluff
fix:
    cd {{justfile_directory()}} && uv run sqlfluff fix models/ macros/

# Generate and serve dbt docs
docs:
    cd {{justfile_directory()}} && uv run dbt docs generate && uv run dbt docs serve
