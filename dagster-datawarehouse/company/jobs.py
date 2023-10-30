from dagster import job

from company.ops.sample import hello_redshift


@job
def hello_redshift_job() -> None:
    """Example of a simple Dagster job."""
    hello_redshift()
