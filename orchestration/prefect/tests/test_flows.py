"""Tests for Prefect flow definitions.

All flows are tested with mocked external services (HTTP, subprocess, S3).
The prefect_test_harness from conftest.py is used automatically.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestAirbyteSyncFlow:
    """Tests for the Airbyte sync flow."""

    @patch("flows.airbyte_sync.requests")
    def test_trigger_sync_returns_job_id(self, mock_requests):
        """trigger_sync task should return the job ID from the API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"jobId": "test-job-123"}
        mock_response.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_response

        from flows.airbyte_sync import trigger_sync

        result = trigger_sync.fn(
            server_url="http://localhost:8006/api/public/v1",
            connection_id="test-connection-id",
        )
        assert result == "test-job-123"

    @patch("flows.airbyte_sync.requests")
    def test_poll_sync_returns_on_success(self, mock_requests):
        """poll_sync should return 'succeeded' when the job completes."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "succeeded"}
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        from flows.airbyte_sync import poll_sync

        result = poll_sync.fn(
            server_url="http://localhost:8006/api/public/v1",
            job_id="test-job-123",
            poll_interval=0,
        )
        assert result == "succeeded"

    @patch("flows.airbyte_sync.requests")
    def test_poll_sync_raises_on_failure(self, mock_requests):
        """poll_sync should raise RuntimeError when the job fails."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "failed"}
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        from flows.airbyte_sync import poll_sync

        with pytest.raises(RuntimeError, match="failed"):
            poll_sync.fn(
                server_url="http://localhost:8006/api/public/v1",
                job_id="test-job-123",
                poll_interval=0,
            )


class TestDbtTransformFlow:
    """Tests for the dbt transform flow."""

    @patch("flows.dbt_transform.subprocess")
    def test_dbt_deps_success(self, mock_subprocess):
        """dbt_deps task should complete without error on success."""
        mock_subprocess.run.return_value = MagicMock(
            returncode=0, stdout="Success", stderr=""
        )

        from flows.dbt_transform import dbt_deps

        # Should not raise
        dbt_deps.fn(project_dir="/tmp/test-dbt")

        mock_subprocess.run.assert_called_once()
        args = mock_subprocess.run.call_args
        assert "deps" in args[0][0]

    @patch("flows.dbt_transform.subprocess")
    def test_dbt_build_raises_on_failure(self, mock_subprocess):
        """dbt_build task should raise RuntimeError on non-zero exit."""
        mock_subprocess.run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Compilation Error"
        )

        from flows.dbt_transform import dbt_build

        with pytest.raises(RuntimeError, match="dbt build failed"):
            dbt_build.fn(project_dir="/tmp/test-dbt", target="dev")


class TestFullPipelineFlow:
    """Tests for the full pipeline flow."""

    def test_freshness_check_passes_within_threshold(self):
        """freshness_check should pass when elapsed time is within threshold."""
        from datetime import datetime, timezone

        from flows.full_pipeline import freshness_check

        recent_start = datetime.now(timezone.utc)
        result = freshness_check.fn(pipeline_start=recent_start, threshold_hours=25)

        assert result["passed"] is True

    def test_freshness_check_fails_when_exceeded(self):
        """freshness_check should raise ValueError when threshold exceeded."""
        from datetime import datetime, timedelta, timezone

        from flows.full_pipeline import freshness_check

        old_start = datetime.now(timezone.utc) - timedelta(hours=30)
        with pytest.raises(ValueError, match="exceeded"):
            freshness_check.fn(pipeline_start=old_start, threshold_hours=25)


class TestS3SensorFlow:
    """Tests for the S3 file sensor flow."""

    @patch("flows.sensors.s3_sensor.boto3")
    def test_check_s3_returns_new_keys(self, mock_boto3):
        """check_s3_for_new_files should return keys from S3 listing."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "inbound/file1.csv"},
                {"Key": "inbound/file2.csv"},
            ]
        }
        mock_boto3.client.return_value = mock_client

        from flows.sensors.s3_sensor import check_s3_for_new_files

        result = check_s3_for_new_files.fn(
            bucket="test-bucket", prefix="inbound/", last_key=""
        )

        assert len(result) == 2
        assert result[0] == "inbound/file1.csv"

    @patch("flows.sensors.s3_sensor.boto3")
    def test_check_s3_filters_by_cursor(self, mock_boto3):
        """Files at or before the cursor should be excluded."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "inbound/file1.csv"},
                {"Key": "inbound/file2.csv"},
            ]
        }
        mock_boto3.client.return_value = mock_client

        from flows.sensors.s3_sensor import check_s3_for_new_files

        result = check_s3_for_new_files.fn(
            bucket="test-bucket", prefix="inbound/", last_key="inbound/file1.csv"
        )

        assert len(result) == 1
        assert result[0] == "inbound/file2.csv"

    def test_process_s3_file_returns_status(self):
        """process_s3_file should return a dict with processing status."""
        from flows.sensors.s3_sensor import process_s3_file

        result = process_s3_file.fn(bucket="test-bucket", key="inbound/test.csv")
        assert result["status"] == "processed"
        assert result["bucket"] == "test-bucket"
        assert result["key"] == "inbound/test.csv"
