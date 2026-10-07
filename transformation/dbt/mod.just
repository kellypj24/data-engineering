# profiles.yml lives here in the project directory rather than ~/.dbt, so every
# dbt command needs to be pointed at it. CI sets the same variable.
export DBT_PROFILES_DIR := source_directory()

# Run the Python tests, then build the example project on its seeded fixtures:
# every model, data test, and unit test. Seeds (and their tests) build first,
# then everything else without them: models and source tests read the fixtures
# through source(), which dbt does not order after seeds, and a seed rebuilt in
# the same run is dropped and recreated while they read it. DBT_ENV=prod
# because the fixtures have fixed dates that the dev-only limit_data_in_dev
# window would filter out.
test:
    uv run pytest tests/python
    DBT_ENV=prod uv run dbt build --select resource_type:seed
    DBT_ENV=prod uv run dbt build --exclude resource_type:seed

# The database is the `snowflake` dev target's default; re-running replaces it.
# Pass a name for a named experiment: `just dbt::dev-database experiment_x`.
# Clone prod into your personal Snowflake database, ANALYTICS_PROD_<USER>
dev-database username="":
    DBT_TARGET=snowflake uv run dbt run-operation refresh_dev_database --args '{username: "{{ username }}", dry_run: false}'

# Run dbt models
run:
    uv run dbt run

# Lint SQL with sqlfluff
lint:
    uv run sqlfluff lint models/ macros/
    uv run yamllint --strict .

# Fix SQL with sqlfluff
fix:
    uv run sqlfluff fix models/ macros/

# Generate and serve dbt docs
docs:
    uv run dbt docs generate && uv run dbt docs serve
