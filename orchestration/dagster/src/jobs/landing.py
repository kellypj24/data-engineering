"""Landing-file job, launched by ``s3_file_arrival_sensor`` once per new object.

Customisation
-------------
* Replace the body of ``process_landing_file`` with the load for your file
  format (copy into a raw table, trigger a dlt pipeline, ...).
"""

from dagster import Config, OpExecutionContext, job, op


class LandingFileConfig(Config):
    s3_bucket: str
    s3_key: str


@op
def process_landing_file(context: OpExecutionContext, config: LandingFileConfig):
    """Handle one newly-landed S3 object."""
    context.log.info(f"Processing s3://{config.s3_bucket}/{config.s3_key}")


@job(description="Processes one landed S3 object; launched by s3_file_arrival_sensor.")
def process_landing_file_job():
    process_landing_file()
