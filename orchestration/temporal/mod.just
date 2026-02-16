# Run Temporal tests
test:
    cd {{source_directory()}} && uv run pytest tests/ -v

# Start Temporal via Docker Compose
up:
    cd {{source_directory()}} && docker compose up -d

# Stop Temporal
down:
    cd {{source_directory()}} && docker compose down

# Lint Python code
lint:
    cd {{source_directory()}} && uv run ruff check workflows/ activities/ workers/ tests/

# Format Python code
fmt:
    cd {{source_directory()}} && uv run ruff format workflows/ activities/ workers/ tests/
