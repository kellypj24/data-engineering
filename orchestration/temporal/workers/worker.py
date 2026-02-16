"""Temporal worker that connects to the server and registers workflows + activities."""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities.example_activity import extract_data, load_data, transform_data
from workflows.example_workflow import DataPipelineWorkflow

TASK_QUEUE = "data-pipeline-queue"
TEMPORAL_ADDRESS = "localhost:7233"


async def main() -> None:
    """Start the Temporal worker."""
    client = await Client.connect(TEMPORAL_ADDRESS)
    print(f"Connected to Temporal server at {TEMPORAL_ADDRESS}")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DataPipelineWorkflow],
        activities=[extract_data, transform_data, load_data],
    )

    print(f"Worker listening on task queue: {TASK_QUEUE}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
