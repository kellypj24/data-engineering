"""Shared fixtures for Airflow tests.

Sets AIRFLOW__CORE__UNIT_TEST_MODE to True before any DAGs are loaded,
which makes Airflow use an in-memory SQLite database.
"""

import os

import pytest

# Must be set before importing anything from airflow
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"


@pytest.fixture(scope="session")
def dagbag():
    """Load all DAGs via DagBag for validation tests."""
    from airflow.models import DagBag

    dag_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)
