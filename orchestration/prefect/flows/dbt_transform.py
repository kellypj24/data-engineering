"""dbt transformation flow.

Runs `dbt deps` then `dbt build` (models + tests) against the configured
dbt project. Mirrors the Dagster dbt asset pattern.
"""

import os
import subprocess

from prefect import flow, get_run_logger, task

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "./dbt_project")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")


@task(name="dbt-deps", retries=1, retry_delay_seconds=10)
def dbt_deps(project_dir: str) -> None:
    """Install dbt package dependencies."""
    logger = get_run_logger()
    logger.info(f"Installing dbt packages in {project_dir}")

    result = subprocess.run(
        ["dbt", "deps", "--project-dir", project_dir],
        capture_output=True,
        text=True,
        check=False,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt deps failed with exit code {result.returncode}")


@task(name="dbt-build", retries=2, retry_delay_seconds=30)
def dbt_build(project_dir: str, target: str = "dev") -> None:
    """Run dbt build (compile models + run tests)."""
    logger = get_run_logger()
    logger.info(f"Running dbt build in {project_dir} with target={target}")

    result = subprocess.run(
        ["dbt", "build", "--project-dir", project_dir, "--target", target],
        capture_output=True,
        text=True,
        check=False,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")

    logger.info("dbt build completed successfully")


@flow(name="dbt-transform", log_prints=True)
def dbt_transform(
    project_dir: str = DBT_PROJECT_DIR,
    target: str = DBT_TARGET,
) -> None:
    """Run dbt deps then dbt build."""
    dbt_deps(project_dir=project_dir)
    dbt_build(project_dir=project_dir, target=target)
    print("dbt transformation completed successfully")


if __name__ == "__main__":
    dbt_transform()
