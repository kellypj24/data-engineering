# Run Dagster tests
test:
    uv run pytest tests/ -v

# Start Dagster dev server
dev:
    uv run dagster dev

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/
