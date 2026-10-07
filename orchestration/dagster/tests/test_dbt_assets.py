"""Tests that the real dbt project loads as Dagster assets.

`src.assets.dbt` builds `@dbt_assets` from `transformation/dbt/target/manifest.json`
and silently skips when that file is missing. CI (and `just dagster::test`) runs
`dbt parse` first and sets DAGSTER_REQUIRE_DBT_MANIFEST=1, so a missing manifest
fails here instead of skipping. A dbt project that `@dbt_assets` rejects (e.g.
two dbt resources mapping to one asset key) fails at import.
"""

import os

import pytest
from dagster import Definitions

from src.assets.dbt import DBT_MANIFEST_PATH, dbt_project_assets

REQUIRED = os.environ.get("DAGSTER_REQUIRE_DBT_MANIFEST") == "1"


@pytest.fixture
def dbt_assets_def():
    if dbt_project_assets is None:
        if REQUIRED:
            pytest.fail(f"{DBT_MANIFEST_PATH} missing -- run `dbt parse` first")
        pytest.skip("no dbt manifest; run `dbt parse` in transformation/dbt")
    return dbt_project_assets


def test_dbt_models_become_assets(dbt_assets_def):
    names = {key.path[-1] for key in dbt_assets_def.keys}
    assert "stg_example" in names


def test_definitions_load(dbt_assets_def):
    from src import defs

    Definitions.validate_loadable(defs)


def test_dbt_runs_from_the_dagster_environment(tmp_path):
    """Dagster runs dbt with its own venv's dbt, not the dbt tool's. Parsing
    on the default (duckdb) target fails here if that adapter is missing."""
    from dagster_dbt import DbtCliResource

    from src.assets.dbt import DBT_PROJECT_DIR

    dbt = DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR), profiles_dir=str(DBT_PROJECT_DIR)
    )
    invocation = dbt.cli(["parse"], target_path=tmp_path, raise_on_error=False).wait()
    assert invocation.is_successful(), invocation.get_error()
