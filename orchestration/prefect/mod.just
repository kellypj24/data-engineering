# Run Prefect tests
test:
    cd {{justfile_directory()}} && uv run pytest tests/ -v

# Start Prefect server
server:
    cd {{justfile_directory()}} && uv run prefect server start

# Run a specific flow
run flow_name:
    cd {{justfile_directory()}} && uv run python -m flows.{{flow_name}}

# Lint Python code
lint:
    cd {{justfile_directory()}} && uv run ruff check flows/ blocks/ tests/

# Format Python code
fmt:
    cd {{justfile_directory()}} && uv run ruff format flows/ blocks/ tests/
