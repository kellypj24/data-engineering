# Run dlt tests
test:
    cd {{source_directory()}} && uv run pytest tests/ -v

# Run the example pipeline
run:
    cd {{source_directory()}} && uv run python -m pipelines.example_pipeline

# Lint Python code
lint:
    cd {{source_directory()}} && uv run ruff check pipelines/ tests/

# Format Python code
fmt:
    cd {{source_directory()}} && uv run ruff format pipelines/ tests/
