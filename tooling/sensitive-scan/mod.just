# Run sensitive-scan tests
test:
    uv run pytest tests/ -v

# Scan every git-tracked file in the current repo
scan:
    uv run sensitive-scan --all

# Lint Python code
lint:
    uv run ruff check src/ tests/

# Format Python code
fmt:
    uv run ruff format src/ tests/

# Check formatting without rewriting files
fmt-check:
    uv run ruff format --check src/ tests/
