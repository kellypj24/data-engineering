"""Tests for Prefect task definitions via direct .fn() invocation."""

from unittest.mock import MagicMock, patch

import pytest


class TestAirbyteTasks:
    """Tests for Airbyte-related tasks."""

    @patch("flows.airbyte_sync.requests")
    def test_trigger_sync_calls_correct_endpoint(self, mock_requests):
        """trigger_sync should POST to the /jobs endpoint."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"jobId": "123"}
        mock_response.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_response

        from flows.airbyte_sync import trigger_sync

        trigger_sync.fn(server_url="http://test:8006/api/public/v1", connection_id="abc")

        mock_requests.post.assert_called_once_with(
            "http://test:8006/api/public/v1/jobs",
            json={"connectionId": "abc", "jobType": "sync"},
            timeout=30,
        )


class TestDbtTasks:
    """Tests for dbt-related tasks."""

    @patch("flows.dbt_transform.subprocess")
    def test_dbt_build_passes_target(self, mock_subprocess):
        """dbt_build should pass the target argument to dbt CLI."""
        mock_subprocess.run.return_value = MagicMock(
            returncode=0, stdout="", stderr=""
        )

        from flows.dbt_transform import dbt_build

        dbt_build.fn(project_dir="/tmp/dbt", target="staging")

        cmd = mock_subprocess.run.call_args[0][0]
        assert "--target" in cmd
        assert "staging" in cmd
