# dlt Extract & Load

## Role
Code-first Python EL pipelines. Lightweight alternative to Airbyte for custom sources.

## Key Files

- `pipelines/example_pipeline.py` — JSONPlaceholder REST API → DuckDB pipeline
- `pyproject.toml` — Dependencies: dlt[duckdb,snowflake,filesystem], requests

## Testing

```bash
just dlt::test         # or: cd extract_load/dlt && uv run pytest
```

- Tests mock `requests.get`, run pipeline into DuckDB, verify tables + row counts
- Use `dlt.pipeline(destination="duckdb")` for test isolation

## Patterns

- `@dlt.resource` for individual data sources (with write_disposition)
- `@dlt.source` to group resources
- `dlt.pipeline()` to configure destination and dataset
- Pipeline names should be descriptive: `{source}_pipeline`
- Dataset names use `{source}_raw` convention
