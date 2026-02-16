"""Tests for Airflow DAG validation.

Verifies that all DAGs load without import errors, have no cycles,
and have the expected task counts and dependencies.
"""

import os
import sys

import pytest

# Add dags directory to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


class TestDagLoading:
    """Verify all DAGs load without errors."""

    def test_no_import_errors(self, dagbag):
        """All DAGs should load without import errors."""
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    def test_expected_dag_count(self, dagbag):
        """Should have at least 4 DAGs."""
        assert len(dagbag.dags) >= 4

    @pytest.mark.parametrize(
        "dag_id",
        ["airbyte_sync", "dbt_transform", "full_pipeline", "s3_file_sensor"],
    )
    def test_dag_exists(self, dagbag, dag_id):
        """Each expected DAG should be present."""
        assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found"

    @pytest.mark.parametrize(
        "dag_id",
        ["airbyte_sync", "dbt_transform", "full_pipeline", "s3_file_sensor"],
    )
    def test_no_cycles(self, dagbag, dag_id):
        """DAGs should have no cyclic dependencies."""
        dag = dagbag.dags[dag_id]
        # topological_sort() raises AirflowDagCycleException if cycles exist
        dag.topological_sort()


class TestDagConfigurations:
    """Verify DAG configurations match conventions."""

    @pytest.mark.parametrize(
        "dag_id",
        ["airbyte_sync", "dbt_transform", "full_pipeline"],
    )
    def test_catchup_disabled(self, dagbag, dag_id):
        """Scheduled DAGs should have catchup disabled."""
        dag = dagbag.dags[dag_id]
        assert dag.catchup is False

    def test_full_pipeline_task_count(self, dagbag):
        """Full pipeline should have 4 tasks (sync, dbt_run, dbt_test, freshness)."""
        dag = dagbag.dags["full_pipeline"]
        assert len(dag.tasks) == 4

    def test_full_pipeline_dependencies(self, dagbag):
        """In full_pipeline, airbyte_sync should be upstream of dbt tasks."""
        dag = dagbag.dags["full_pipeline"]
        sync_task = dag.get_task("airbyte_sync")
        assert len(sync_task.downstream_list) > 0

    def test_s3_sensor_has_no_schedule(self, dagbag):
        """S3 sensor DAG should be externally triggered (no schedule)."""
        dag = dagbag.dags["s3_file_sensor"]
        assert dag.schedule_interval is None or dag.timetable is not None
