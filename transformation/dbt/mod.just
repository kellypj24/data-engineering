# profiles.yml lives here in the project directory rather than ~/.dbt, so every
# dbt command needs to be pointed at it. CI sets the same variable.
export DBT_PROFILES_DIR := source_directory()

# Run dbt tests (schema + unit)
test:
    uv run dbt test

# Run dbt models
run:
    uv run dbt run

# Lint SQL with sqlfluff
lint:
    uv run sqlfluff lint models/ macros/

# Fix SQL with sqlfluff
fix:
    uv run sqlfluff fix models/ macros/

# Generate and serve dbt docs
docs:
    uv run dbt docs generate && uv run dbt docs serve
