# Run Dagster tests
test:
    cd {{source_directory()}} && uv run pytest tests/ -v

# Start Dagster dev server
dev:
    cd {{source_directory()}} && uv run dagster dev

# Lint Python code
lint:
    cd {{source_directory()}} && uv run ruff check src/ tests/

# Format Python code
fmt:
    cd {{source_directory()}} && uv run ruff format src/ tests/
