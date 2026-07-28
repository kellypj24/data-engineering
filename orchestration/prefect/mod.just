# Run Prefect tests
test:
    uv run pytest tests/ -v

# Start Prefect server
server:
    uv run prefect server start

# Run a specific flow
run flow_name:
    uv run python -m flows.{{flow_name}}

# Lint Python code
lint:
    uv run ruff check flows/ blocks/ tests/

# Format Python code
fmt:
    uv run ruff format flows/ blocks/ tests/
