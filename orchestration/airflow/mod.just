# Run Airflow tests
test:
    cd {{source_directory()}} && uv run pytest tests/ -v

# Start Airflow via Docker Compose
up:
    cd {{source_directory()}} && docker compose up -d

# Stop Airflow
down:
    cd {{source_directory()}} && docker compose down

# Lint Python code
lint:
    cd {{source_directory()}} && uv run ruff check dags/ tests/

# Format Python code
fmt:
    cd {{source_directory()}} && uv run ruff format dags/ tests/
