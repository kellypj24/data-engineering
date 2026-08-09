# Run Airflow tests
test:
    uv run pytest tests/ -v

# Start Airflow via Docker Compose
up:
    docker compose up -d

# Stop Airflow
down:
    docker compose down

# Lint Python code
lint:
    uv run ruff check dags/ tests/

# Format Python code
fmt:
    uv run ruff format dags/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check dags/ tests/
