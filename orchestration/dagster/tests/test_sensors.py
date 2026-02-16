"""Tests for Dagster sensor definitions."""

from unittest.mock import MagicMock, patch

from dagster import build_sensor_context

from src.sensors.s3_sensor import s3_file_arrival_sensor, S3_BUCKET, S3_PREFIX


class TestS3FileArrivalSensor:
    """Tests for the S3 file arrival sensor."""

    @patch("src.sensors.s3_sensor.boto3")
    def test_no_new_files_yields_nothing(self, mock_boto3):
        """When S3 has no new files, sensor should yield no RunRequests."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {"Contents": []}
        mock_boto3.client.return_value = mock_client

        context = build_sensor_context()
        result = list(s3_file_arrival_sensor(context))

        assert len(result) == 0

    @patch("src.sensors.s3_sensor.boto3")
    def test_new_files_yield_run_requests(self, mock_boto3):
        """When S3 has new files, sensor should yield a RunRequest per file."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "inbound/file1.csv"},
                {"Key": "inbound/file2.csv"},
            ]
        }
        mock_boto3.client.return_value = mock_client

        context = build_sensor_context()
        result = list(s3_file_arrival_sensor(context))

        assert len(result) == 2
        assert result[0].run_key == "inbound/file1.csv"
        assert result[1].run_key == "inbound/file2.csv"

    @patch("src.sensors.s3_sensor.boto3")
    def test_cursor_filters_seen_files(self, mock_boto3):
        """Files at or before the cursor should be skipped."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "inbound/file1.csv"},
                {"Key": "inbound/file2.csv"},
            ]
        }
        mock_boto3.client.return_value = mock_client

        context = build_sensor_context(cursor="inbound/file1.csv")
        result = list(s3_file_arrival_sensor(context))

        assert len(result) == 1
        assert result[0].run_key == "inbound/file2.csv"

    def test_sensor_config(self):
        """Verify sensor configuration constants."""
        assert S3_BUCKET == "my-data-lake-landing"
        assert S3_PREFIX == "inbound/"
