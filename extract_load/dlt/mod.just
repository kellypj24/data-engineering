# Run dlt tests
test:
    uv run pytest tests/ -v

# Run the example pipeline
run:
    uv run python -m pipelines.example_pipeline

# Lint Python code
lint:
    uv run ruff check pipelines/ tests/

# Format Python code
fmt:
    uv run ruff format pipelines/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check pipelines/ tests/
