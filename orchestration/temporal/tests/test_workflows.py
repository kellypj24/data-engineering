"""Tests for Temporal workflow definitions.

Uses mock activities to test workflow logic in isolation.
"""

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities.example_activity import extract_data, load_data, transform_data
from workflows.example_workflow import DataPipelineWorkflow


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_data_pipeline_workflow():
    """Test the full data pipeline workflow with time-skipping."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DataPipelineWorkflow],
            activities=[extract_data, transform_data, load_data],
        ):
            result = await env.client.execute_workflow(
                DataPipelineWorkflow.run,
                "test-source",
                id="test-workflow-id",
                task_queue="test-queue",
            )

            assert "Pipeline complete" in result
            assert "test-source" in result
            assert "3 records" in result


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_data_pipeline_workflow_returns_source_name():
    """Result should include the source name."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DataPipelineWorkflow],
            activities=[extract_data, transform_data, load_data],
        ):
            result = await env.client.execute_workflow(
                DataPipelineWorkflow.run,
                "my-api",
                id="test-workflow-id-2",
                task_queue="test-queue",
            )

            assert "my-api" in result
