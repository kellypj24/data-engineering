# Run claude-review tests (temporary git repos; `claude` is mocked)
test:
    uv run pytest tests/ -v

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check src/ tests/
