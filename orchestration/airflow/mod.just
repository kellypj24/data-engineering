# Run Airflow tests
test:
    cd {{justfile_directory()}} && uv run pytest tests/ -v

# Start Airflow via Docker Compose
up:
    cd {{justfile_directory()}} && docker compose up -d

# Stop Airflow
down:
    cd {{justfile_directory()}} && docker compose down

# Lint Python code
lint:
    cd {{justfile_directory()}} && uv run ruff check dags/ tests/

# Format Python code
fmt:
    cd {{justfile_directory()}} && uv run ruff format dags/ tests/
