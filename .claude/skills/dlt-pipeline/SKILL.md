---
name: dlt-pipeline
description: Use when adding a dlt extract-load pipeline — "add a dlt pipeline for X", "load this API into the warehouse", "new dlt source or resource", "write a dlt pipeline test". Scaffolds the source, resources, and pipeline with a mocked test that needs no network.
argument-hint: "<pipeline-name>   e.g. stripe"
---

# Add a dlt pipeline

Runs **in the downstream project**. Find the dlt tool root — the directory whose
`pyproject.toml` depends on `dlt`:

```bash
grep -rl '"dlt' --include=pyproject.toml . | grep -v node_modules
```

Pipelines live in `pipelines/`, tests in `tests/`, one module per source.

## The three layers

dlt separates these, and conflating them is what makes pipelines hard to test:

| Layer | Decorator | Responsibility |
|---|---|---|
| **Resource** | `@dlt.resource(name=…, write_disposition=…)` | Yields rows for **one** table. |
| **Source** | `@dlt.source(name=…)` | Groups related resources; returns a list of them. |
| **Pipeline** | `dlt.pipeline(...)` in a plain function | Binds a source to a destination and dataset. |

Keep the pipeline in its own `run_pipeline()` function with an
`if __name__ == "__main__":` guard, as `pipelines/example_pipeline.py` does.
Tests import the source and resources directly and never call `run_pipeline()` —
that is what keeps them offline.

```python
import dlt
import requests


@dlt.resource(name="charges", write_disposition="replace")
def stripe_charges(base_url: str = "https://api.stripe.com/v1"):
    """Fetch charges from the Stripe API."""
    response = requests.get(f"{base_url}/charges", timeout=30)
    response.raise_for_status()
    yield response.json()


@dlt.source(name="stripe")
def stripe_source():
    """A dlt source that groups the Stripe resources."""
    return [stripe_charges()]


def run_pipeline() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="stripe_pipeline",
        destination="duckdb",
        dataset_name="stripe_raw",
    )
    load_info = pipeline.run(stripe_source())
    print(f"Pipeline completed: {load_info}")


if __name__ == "__main__":
    run_pipeline()
```

Take the base URL as a **parameter with a default** rather than a module
constant — that is what lets a test point the resource somewhere else without
patching. Always pass `timeout=` to `requests`; a hung EL job blocks the whole
orchestration graph behind it.

`write_disposition` is the decision worth thinking about: `replace` for small
reference tables, `append` for immutable events, `merge` (with `primary_key`)
for mutable records. Getting it wrong is silent — you get duplicates or lost
history, not an error.

## Testing — mock the module, not the library

The existing tests patch the **module's** `requests` attribute:

```python
@patch("pipelines.stripe_pipeline.requests")
def test_charges_resource_yields_data(self, mock_requests, mock_charges):
    mock_response = MagicMock()
    mock_response.json.return_value = mock_charges
    mock_response.raise_for_status = MagicMock()
    mock_requests.get.return_value = mock_response

    results = list(stripe_charges())
    assert len(results) == 2
```

Patching `requests.get` globally instead leaks across tests and, worse,
sometimes still hits the network depending on import order.

For the full-pipeline test, run into DuckDB under `tmp_path` and assert on real
rows — this is the test that proves the schema actually materialised:

```python
pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    destination="duckdb",
    dataset_name="test_raw",
    pipelines_dir=str(tmp_path),
)
load_info = pipeline.run(stripe_source())
assert load_info is not None

with pipeline.sql_client() as client:
    assert client.execute_sql("SELECT COUNT(*) FROM charges")[0][0] == 2
```

`pipelines_dir=str(tmp_path)` is **not optional**. Without it dlt writes state
to a shared default directory, and the second run of the suite resumes from the
first one's state — producing a pass that means nothing and a failure that
disappears when you rerun it.

When one mock must serve several endpoints, use `side_effect` switching on the
URL rather than a queue of `return_value`s; resource execution order is not
guaranteed.

## Wire it into the task runner

`mod.just` hardcodes the example module:

```just
run:
    uv run python -m pipelines.example_pipeline
```

A second pipeline needs its own recipe or a parameterised one. Adding the file
alone means `just dlt::run` still runs the example — a genuinely confusing
five minutes.

## Verify

From the dlt tool root:

```bash
uv run pytest tests/ -v          # offline; must pass with no network
uv run ruff check pipelines/ tests/
uv run python -m pipelines.<name>_pipeline   # real run against the real source
```

The pytest run is the gate. The real run is the proof — check the row counts and
the inferred schema, because dlt will happily create a table of all-null columns
from a response shape you did not expect.

## Common mistakes

- **Omitting `pipelines_dir=str(tmp_path)` in tests.** State leaks between runs;
  results stop being reproducible.
- **Patching `requests` globally** instead of `pipelines.<module>.requests`.
- **Calling `run_pipeline()` from a test.** It targets the real destination and
  the real API.
- **A module-level `BASE_URL` constant.** Make it a parameter with a default so
  tests can redirect it without patching.
- **`requests.get` with no `timeout`.** A hung extract stalls everything
  downstream of it.
- **Defaulting to `write_disposition="replace"` for event data.** It silently
  discards history; nothing errors.
- **Adding the pipeline but not a `just` recipe.** `just dlt::run` keeps running
  the example.
