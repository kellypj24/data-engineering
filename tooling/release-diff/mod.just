# Run release-diff tests (fixture captures, duckdb, no network)
test:
    uv run pytest tests/ -v

# Diff the example fixture captures into ./release-diff-report
example:
    uv run release-diff --profile profiles/example_products.toml --captures tests/fixtures/captures

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check src/ tests/
