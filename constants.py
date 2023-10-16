import os
from pathlib import Path

from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).joinpath("../../data-warehouse").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

dbt_parse_invocation = dbt.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
