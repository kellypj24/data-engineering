"""The stage refresh job runs dbt's refresh_environment for stage, executing."""

import json
from pathlib import Path

import dagster as dg

from src.jobs.environment_refresh import refresh_args, refresh_stage_environment_job


def test_refresh_runs_the_dbt_macro_for_stage_not_dry():
    args = refresh_args("stage")
    assert args[:2] == ["run-operation", "refresh_environment"]
    assert json.loads(args[3]) == {"env": "stage", "dry_run": False}


CALLS: list = []


class FakeDbt(dg.ConfigurableResource):
    project_dir: str = "unused"

    def cli(self, args):
        CALLS.append(args)
        return type("Invocation", (), {"wait": lambda self: self})()


def test_job_invokes_dbt_with_those_args():
    CALLS.clear()
    result = refresh_stage_environment_job.execute_in_process(
        resources={"dbt": FakeDbt()}
    )
    assert result.success
    assert CALLS == [refresh_args("stage")]


def test_dbt_project_defines_stage_and_prod():
    """The job names an environment the dbt var must map."""
    config = (
        Path(__file__).resolve().parents[3] / "transformation/dbt/dbt_project.yml"
    ).read_text()
    assert "stage: ANALYTICS_STAGE" in config
    assert "prod: ANALYTICS_PROD" in config
