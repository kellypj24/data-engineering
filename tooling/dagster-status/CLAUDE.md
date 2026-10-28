# dagster-status

## Role
Tooling: read-only CLI over the Dagster GraphQL API (OSS `/graphql`, Dagster+
with `DAGSTER_CLOUD_API_TOKEN`). See README.md for commands and adoption.

## Key Files

- `src/dagster_status/queries.py` — every GraphQL document the tool sends; queries only
- `src/dagster_status/cli.py` — `execute` (refuses non-queries), one function per command, table/CSV `render`. **Stdlib only**
- `tests/test_cli.py` — each command against mocked responses (urlopen patched), both formats; `tests/test_queries.py` — no mutation anywhere

## Patterns

- **Queries only.** A new document goes in `queries.py` and `queries.ALL`; never a mutation. `test_queries.py` enforces both
- **State from the instance, never from code**: `default_status` and schedule definitions do not say what will run
- Validate a changed query against the real schema from the Dagster venv:
  `cd orchestration/dagster && uv run python -c "from dagster_graphql.schema import create_schema; ..."` (graphql-core `validate`)
- Tests need no network; the one unreachable-server test uses port 9 on localhost
