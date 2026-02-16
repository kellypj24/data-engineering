"""Example Temporal workflow for a data pipeline: extract -> transform -> load."""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.example_activity import extract_data, load_data, transform_data


@workflow.defn
class DataPipelineWorkflow:
    """A data pipeline workflow that runs extract, transform, and load in sequence."""

    @workflow.run
    async def run(self, source_name: str) -> str:
        """Execute the ETL pipeline.

        Args:
            source_name: Identifier for the data source to process.

        Returns:
            A summary string describing the pipeline result.
        """
        workflow.logger.info(f"Starting data pipeline for source: {source_name}")

        # Step 1: Extract
        raw_data = await workflow.execute_activity(
            extract_data,
            source_name,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=workflow.RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=5),
            ),
        )
        workflow.logger.info(f"Extraction complete: {len(raw_data)} records")

        # Step 2: Transform
        transformed_data = await workflow.execute_activity(
            transform_data,
            raw_data,
            start_to_close_timeout=timedelta(minutes=10),
        )
        workflow.logger.info(f"Transformation complete: {len(transformed_data)} records")

        # Step 3: Load
        load_result = await workflow.execute_activity(
            load_data,
            transformed_data,
            start_to_close_timeout=timedelta(minutes=10),
        )
        workflow.logger.info(f"Load complete: {load_result}")

        return f"Pipeline complete for '{source_name}': {load_result}"
