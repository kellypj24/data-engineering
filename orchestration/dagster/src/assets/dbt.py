"""dbt project assets.

Wraps your dbt project as a set of Dagster assets using the ``@dbt_assets``
decorator and ``DbtCliResource``.

Customisation
-------------
* Set ``DBT_PROJECT_DIR`` to the path of your dbt project (absolute or
  relative to the repository root).
* The manifest is loaded at module import time so Dagster can introspect
  the dbt DAG.  Run ``dbt parse`` or ``dbt compile`` at least once before
  launching ``dagster dev``.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# ---- Configuration ---------------------------------------------------------
# Relative path from the repo root to the dbt project.
DBT_PROJECT_DIR = Path(__file__).resolve().parents[4] / "transformation" / "dbt"

# The manifest.json produced by ``dbt parse`` / ``dbt compile``.
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


class CustomDbtTranslator(DagsterDbtTranslator):
    """Override this class to customise how dbt nodes map to Dagster assets.

    For example, you can change the asset key prefix, group name, or
    description for every model.
    """


# ---- Asset definition -------------------------------------------------------
# Only define dbt assets when the manifest exists (skip during tests without
# a compiled dbt project).
if DBT_MANIFEST_PATH.exists() and not os.environ.get("DAGSTER_DBT_SKIP_MANIFEST"):

    @dbt_assets(
        manifest=DBT_MANIFEST_PATH,
        dagster_dbt_translator=CustomDbtTranslator(),
    )
    def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        """Materialise all dbt models as Dagster assets."""
        yield from dbt.cli(["build"], context=context).stream()

else:
    dbt_project_assets = None
