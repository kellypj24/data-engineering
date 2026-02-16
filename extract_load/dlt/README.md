# dlt Pipelines

Data ingestion pipelines built with [dlt](https://dlthub.com/docs/intro) (data load tool).

## When to Use dlt vs Airbyte

| Consideration | dlt | Airbyte |
|---|---|---|
| Custom sources | Code-first; define any Python generator as a source | Requires building a connector in the CDK |
| Managed connectors | Fewer pre-built connectors | 300+ managed connectors out of the box |
| Deployment | Runs anywhere Python runs (scripts, orchestrators, lambdas) | Needs Airbyte server or Airbyte Cloud |
| Schema evolution | Built-in schema inference and evolution | Managed via connector config |
| Best for | Custom APIs, niche sources, embedding into existing Python codebases | Standard SaaS sources where a connector already exists |

**Rule of thumb:** If a reliable Airbyte connector exists for your source, use Airbyte. If you need to write custom extraction logic or want tight integration with your Python codebase, use dlt.

## Pipeline Patterns

- **`@dlt.resource`** — Defines a single data stream (e.g. one API endpoint). Use `write_disposition` to control append/replace/merge behavior.
- **`@dlt.source`** — Groups related resources into a logical source.
- **`dlt.pipeline()`** — Wires a source to a destination with a pipeline name and dataset.
- **Incremental loading** — Use `dlt.sources.incremental` to track state and only fetch new/changed records.

## Setup

```bash
# Install dependencies
pip install -e ".[dev]"

# Run the example pipeline
python pipelines/example_pipeline.py

# Inspect the loaded data (DuckDB)
python -c "
import duckdb
conn = duckdb.connect('jsonplaceholder_pipeline.duckdb')
print(conn.sql('SELECT * FROM jsonplaceholder_raw.posts LIMIT 5'))
"
```

## Links

- [dlt documentation](https://dlthub.com/docs/intro)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/)
- [dlt GitHub](https://github.com/dlt-hub/dlt)
