# Run Temporal tests
test:
    uv run pytest tests/ -v

# Start Temporal via Docker Compose
up:
    docker compose up -d

# Stop Temporal
down:
    docker compose down

# Lint Python code
lint:
    uv run ruff check workflows/ activities/ workers/ tests/

# Format Python code
fmt:
    uv run ruff format workflows/ activities/ workers/ tests/
