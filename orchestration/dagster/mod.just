# Run Dagster tests
test:
    cd {{justfile_directory()}} && uv run pytest tests/ -v

# Start Dagster dev server
dev:
    cd {{justfile_directory()}} && uv run dagster dev

# Lint Python code
lint:
    cd {{justfile_directory()}} && uv run ruff check src/ tests/

# Format Python code
fmt:
    cd {{justfile_directory()}} && uv run ruff format src/ tests/
