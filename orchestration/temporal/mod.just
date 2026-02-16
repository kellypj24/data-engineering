# Run Temporal tests
test:
    cd {{justfile_directory()}} && uv run pytest tests/ -v

# Start Temporal via Docker Compose
up:
    cd {{justfile_directory()}} && docker compose up -d

# Stop Temporal
down:
    cd {{justfile_directory()}} && docker compose down

# Lint Python code
lint:
    cd {{justfile_directory()}} && uv run ruff check workflows/ activities/ workers/ tests/

# Format Python code
fmt:
    cd {{justfile_directory()}} && uv run ruff format workflows/ activities/ workers/ tests/
