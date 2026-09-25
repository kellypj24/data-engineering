# Run file-export tests (includes validating every config under configs/)
test:
    uv run pytest tests/ -v

# Validate every export config
validate:
    uv run file-export validate configs

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check src/ tests/

# The repo's dbt project, which the example configs read from.
dbt_dir := "../../transformation/dbt"
exposures_args := "--manifest " + dbt_dir + "/target/manifest.json --out " + dbt_dir + "/models/exports/_generated_exposures.yml"

# Regenerate the dbt exposures file from configs/ (parses dbt first)
exposures-write:
    cd {{dbt_dir}} && DBT_PROFILES_DIR=. uv run dbt parse --quiet
    uv run file-export exposures --write {{exposures_args}}

# Fail if the dbt exposures file is stale or disagrees with the dbt project
exposures-check:
    cd {{dbt_dir}} && DBT_PROFILES_DIR=. uv run dbt parse --quiet
    uv run file-export exposures --check {{exposures_args}}
