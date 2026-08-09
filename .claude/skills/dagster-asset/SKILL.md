---
name: dagster-asset
description: Use when adding a Dagster asset, asset check, sensor, or schedule — "add an asset for X", "wire this into Dagster", "my asset isn't showing up in the UI", "add a resource to Dagster", "new dagster asset". Scaffolds the definition, registers it, and adds the resource keys it needs.
argument-hint: "<asset-name>   e.g. stripe_charges"
---

# Add a Dagster asset

Runs **in the downstream project**, not the toolkit. Find the code location:

```bash
find . -name workspace.yaml -o -name dagster.yaml | grep -v node_modules
```

Its directory is the **Dagster root**. The module holding `Definitions` is
`src/__init__.py` in the toolkit's layout — confirm by grepping rather than
assuming, since downstream projects rename the package:

```bash
grep -rn "Definitions(" --include=*.py .
```

## The registration chain

An asset is invisible until it reaches `Definitions`. Three links, and a break
in any one fails **silently** — the asset simply does not appear in the UI, with
no error:

```
src/assets/<name>.py          definition
        ↓
src/assets/__init__.py        appended to all_assets
        ↓
src/__init__.py               Definitions(assets=all_assets, ...)
```

`src/__init__.py` is the entry point and takes `assets`, `asset_checks`,
`sensors`, `schedules`, and `resources`. It should not need editing to add an
asset — `all_assets` is the seam. Edit it only when adding a *new category*.

## Step 1 — define the asset

Put it in `src/assets/<domain>.py`, one module per source or domain. Resources
arrive as **typed parameters named for their resource key**, not via
`required_resource_keys`:

```python
from dagster import AssetExecutionContext, asset
from dagster_snowflake import SnowflakeResource


@asset(
    group_name="stripe",
    description="Charges pulled from the Stripe API into the raw schema.",
)
def stripe_charges(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """One row per charge."""
    with snowflake.get_connection() as conn:
        ...
```

The parameter name `snowflake` must match a key in the `RESOURCES` dict. A
mismatch is a load-time error naming the *asset*, not the resource, which sends
people looking in the wrong file.

For a repetitive family of Airbyte-backed assets, use the existing factory
rather than hand-rolling — `src/utils/factories.py` exposes
`build_source_assets(name, connection_id, tables, key_prefix=None,
group_name=None)`.

## Step 2 — register it

In `src/assets/__init__.py`, import and append. Note the existing splat pattern
and keep it:

```python
from src.assets.stripe import stripe_assets

all_assets = [
    *airbyte_assets,
    *([dbt_project_assets] if dbt_project_assets is not None else []),
    *stripe_assets,
]
```

That conditional is not defensive noise. `src/assets/dbt.py` sets
`dbt_project_assets = None` when `target/manifest.json` is absent, so the dbt
assets vanish rather than crashing the code location. If your asset can be
absent for a similar reason, follow the same shape; if it cannot, splat it
plainly.

## Step 3 — add any new resource

Resources live in `src/resources/connections.py`, in the `RESOURCES` dict.
Secrets come from `EnvVar`, never literals:

```python
RESOURCES: dict = {
    ...,
    "stripe": StripeResource(api_key=EnvVar("STRIPE_API_KEY")),
}
```

`EnvVar` is resolved at **run** time, not import time, so a missing variable
surfaces as a failed run rather than a code location that will not load. That is
deliberate — do not "fix" it by reading `os.environ` at module scope.

## Step 4 — test it

Tests go in `tests/test_assets.py`. `tests/conftest.py` already provides
`mock_airbyte_resource`, `mock_dbt_resource`, `mock_s3_client`, and
`minimal_dbt_manifest` — use them instead of building new mocks. The existing
tests assert on structure (asset keys cover the destination tables), which is
the right altitude for a scaffold: it catches a broken registration chain
without needing a warehouse.

## Verify

From the Dagster root:

```bash
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run dagster definitions validate    # loads Definitions the way the daemon does
```

`dagster definitions validate` emits a `SupersessionWarning` pointing at
`dg check defs`. That is the successor CLI and is **not installed here** —
`dg` ships in the `dagster-dg-cli` package, which this project does not depend
on. The warning is expected; the command still validates. Ignore it, or add the
dependency deliberately rather than chasing the warning mid-task.

Then confirm it actually appears, which the tests above do not prove:

```bash
uv run dagster dev            # asset should be in the graph, in its group
```

If the asset is missing from the UI but tests pass, the registration chain is
broken — walk the three links in order.

## Common mistakes

- **Defining an asset and never appending it to `all_assets`.** The most common
  failure by a wide margin, and it produces no error at all.
- **A resource parameter name that does not match its `RESOURCES` key.** The
  error names the asset, not the resource.
- **Expecting dbt assets without a manifest.** `src/assets/dbt.py` reads
  `target/manifest.json` at *import* time. Run `dbt parse` in the dbt project
  first, or `dbt_project_assets` is `None` and every dbt asset disappears.
  `DAGSTER_DBT_SKIP_MANIFEST` forces that path on purpose for tests.
- **Hardcoding secrets instead of `EnvVar`.** Also: reading `os.environ` at
  module scope, which turns a missing secret into a code location that will not
  load for anyone.
- **Editing `src/__init__.py` to add an asset.** `all_assets` is the seam;
  touching `Definitions` for a routine asset means the next person has two
  places to look.
- **Testing by materialising against a real warehouse.** Assert on structure and
  use the conftest mocks; save live runs for `dagster dev`.
