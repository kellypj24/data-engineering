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
