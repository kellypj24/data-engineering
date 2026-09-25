# Run Dagster tests. Parses the dbt project first so the dbt assets load from a
# current manifest; DAGSTER_REQUIRE_DBT_MANIFEST makes a missing one fail.
test:
    cd ../../transformation/dbt && DBT_PROFILES_DIR=. uv run dbt deps --quiet && DBT_PROFILES_DIR=. uv run dbt parse --quiet
    DAGSTER_REQUIRE_DBT_MANIFEST=1 uv run pytest tests/ -v

# Start Dagster dev server
dev:
    uv run dagster dev

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check src/ tests/
