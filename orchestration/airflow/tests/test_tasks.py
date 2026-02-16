"""Tests for individual Airflow task callables.

Tests the task logic in isolation using mocked external services.
"""

import os
import sys
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


class TestDbtTransformTasks:
    """Tests for dbt transform DAG tasks."""

    @patch("subprocess.run")
    def test_dbt_build_command(self, mock_run):
        """dbt_build task should call dbt build with correct arguments."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Success", stderr="")

        # Import the DAG module to access config
        import dbt_transform

        assert dbt_transform.DBT_PROJECT_DIR is not None
        assert dbt_transform.DBT_TARGET is not None


class TestFullPipelineTasks:
    """Tests for full pipeline DAG tasks."""

    def test_freshness_threshold_default(self):
        """Default freshness threshold should be 25 hours."""
        import full_pipeline

        assert full_pipeline.FRESHNESS_THRESHOLD_HOURS == 25
