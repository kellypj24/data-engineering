"""Example Prefect flow that orchestrates dbt run and dbt test."""

import subprocess
from pathlib import Path

from prefect import flow, get_run_logger, task


@task(name="dbt-run", retries=2, retry_delay_seconds=30)
def dbt_run(project_dir: str, target: str = "dev") -> None:
    """Execute `dbt run` against the specified project directory.

    Args:
        project_dir: Path to the dbt project.
        target: dbt target/profile to use.
    """
    logger = get_run_logger()
    logger.info(f"Running dbt run in {project_dir} with target={target}")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", project_dir, "--target", target],
        capture_output=True,
        text=True,
        check=False,
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")

    logger.info("dbt run completed successfully")


@task(name="dbt-test", retries=1, retry_delay_seconds=10)
def dbt_test(project_dir: str, target: str = "dev") -> None:
    """Execute `dbt test` against the specified project directory.

    Args:
        project_dir: Path to the dbt project.
        target: dbt target/profile to use.
    """
    logger = get_run_logger()
    logger.info(f"Running dbt test in {project_dir} with target={target}")

    result = subprocess.run(
        ["dbt", "test", "--project-dir", project_dir, "--target", target],
        capture_output=True,
        text=True,
        check=False,
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed with exit code {result.returncode}")

    logger.info("dbt test completed successfully")


@flow(name="dbt-pipeline", log_prints=True)
def dbt_pipeline(
    project_dir: str = "./dbt_project",
    target: str = "dev",
) -> None:
    """Orchestrate a dbt pipeline: run models then run tests.

    Args:
        project_dir: Path to the dbt project directory.
        target: dbt target/profile to use.
    """
    project_path = Path(project_dir).resolve()
    print(f"Starting dbt pipeline for project: {project_path}")

    # Run models first, then tests
    dbt_run(project_dir=str(project_path), target=target)
    dbt_test(project_dir=str(project_path), target=target)

    print("dbt pipeline completed successfully")


if __name__ == "__main__":
    dbt_pipeline()
