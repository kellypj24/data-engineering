# dagster-status

Read-only Dagster status from the command line, over the GraphQL API. Stdlib
only, so it runs without the project's venv.

"Is this job going to run tonight?" is a question about the **instance**, not
the code. `default_status` applies only the first time a definition loads;
after that, whoever last toggled it in the UI decides. A stopped schedule's
job can still be launched by a running sensor. This tool asks the instance.

## Commands

| Command | Shows |
|---------|-------|
| `status` | Required daemons' health and the number of in-progress runs |
| `recent [-n 20]` | Latest runs: id, job, status, start, end (UTC) |
| `errors [-n 20]` | Latest failed runs |
| `schedules` | Every schedule: location, job, cron, timezone, live state |
| `sensors` | Every sensor: location, type, target jobs, live state |
| `triggers <job>` | Every schedule and sensor that can launch `<job>`, with live state |

`--csv` prints CSV instead of an aligned table.

```bash
uv run dagster-status triggers extract_job
# kind      name                  location  state
# schedule  daily_asset_schedule  src       STOPPED
# sensor    retry_extract         src       RUNNING
```

## Endpoint

| | URL (`--url` or `DAGSTER_URL`) | Auth |
|-|-----|------|
| OSS | `http://localhost:3000/graphql` (default) | none |
| Dagster+ | `https://<org>.dagster.cloud/<deployment>/graphql` | `DAGSTER_CLOUD_API_TOKEN` (a read-only user token is enough) |

## What is sent

Only GraphQL **queries** from `src/dagster_status/queries.py`, their variables
(a row limit and run statuses), and the token header when the variable is set,
to the one URL given. `execute` refuses any document that is not a query, and
a test fails if `queries.py` gains a mutation. Nothing else leaves the machine.

## Failure

Not a gate: it exits `2` with a message on stderr when the server is
unreachable, returns GraphQL errors, or reports a `PythonError`, and `0`
otherwise. It never changes instance state, so a failure has no side effect.

## GitHub Actions

A nightly check that the job you care about has a running trigger:

```yaml
- uses: astral-sh/setup-uv@v7
- name: Who can launch transform_job tonight?
  working-directory: tooling/dagster-status
  env:
    DAGSTER_URL: https://my-org.dagster.cloud/prod/graphql
    DAGSTER_CLOUD_API_TOKEN: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
  run: |
    uv run dagster-status --csv triggers transform_job | tee triggers.csv
    grep -q ',RUNNING$' triggers.csv || { echo "::error::nothing running can launch transform_job"; exit 1; }
```

## Development

```bash
just dagster-status::test
just dagster-status::lint
```
