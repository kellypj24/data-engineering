"""Identity probe and write-boundary proof (E13), against a scripted stand-in
for Snowflake."""

from contextlib import contextmanager

import dagster as dg
import pytest
from dagster_snowflake import SnowflakeResource
from snowflake.connector.errors import ProgrammingError

from src import defs
from src.jobs.boundary import (
    active_secondary_roles,
    default_forbidden_database,
    identity_probe_job,
    write_boundary_job,
)
from src.utils.invariants import MANUAL_ONLY_TAG

STATEMENTS: list[str] = []
SCRIPT: dict = {}


class FakeCursor:
    def execute(self, sql):
        STATEMENTS.append(sql)
        if sql.startswith("CREATE TABLE") and SCRIPT.get("create_error"):
            raise SCRIPT["create_error"]

    def fetchone(self):
        return (
            "SVC_STAGE",
            "ANALYTICS_STAGE_WRITE",
            SCRIPT.get("secondary", '{"roles":"","value":""}'),
            "ANALYTICS_STAGE",
            "TRANSFORMING",
        )


class FakeSnowflake(SnowflakeResource):
    @contextmanager
    def get_connection(self, raw_conn: bool = True):
        yield type("Connection", (), {"cursor": lambda self: FakeCursor()})()


def run(job, **script):
    STATEMENTS.clear()
    SCRIPT.clear()
    SCRIPT.update(script)
    return job.execute_in_process(
        resources={"snowflake": FakeSnowflake(account="a", user="u", password="p")},
        raise_on_error=False,
    )


def failure_message(result):
    [event] = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    return event.event_specific_data.error.message


def test_refused_write_passes():
    refusal = ProgrammingError(
        msg="Insufficient privileges to operate on schema 'PUBLIC'", errno=3001
    )
    result = run(write_boundary_job, create_error=refusal)
    assert result.success
    assert any(
        s.startswith("CREATE TABLE ANALYTICS_PROD.PUBLIC.WRITE_BOUNDARY_PROBE_")
        for s in STATEMENTS
    )


def test_not_authorized_counts_as_refused():
    refusal = ProgrammingError(
        msg="Database 'ANALYTICS_PROD' does not exist or not authorized.", errno=2003
    )
    assert run(write_boundary_job, create_error=refusal).success


def test_created_table_is_dropped_and_fails():
    result = run(write_boundary_job)
    assert not result.success
    assert "BOUNDARY BREACHED" in failure_message(result)
    created = next(s for s in STATEMENTS if s.startswith("CREATE TABLE"))
    table = created.split()[2]
    assert f"DROP TABLE IF EXISTS {table}" in STATEMENTS


def test_active_secondary_role_fails_before_writing():
    result = run(
        write_boundary_job, secondary='{"roles":"ANALYTICS_PROD_WRITE","value":""}'
    )
    assert not result.success
    assert "secondary roles" in failure_message(result)
    assert not any(s.startswith("CREATE") for s in STATEMENTS)


def test_other_errors_are_inconclusive():
    error = ProgrammingError(
        msg="Warehouse 'TRANSFORMING' cannot be resumed", errno=606
    )
    result = run(write_boundary_job, create_error=error)
    assert not result.success
    assert "inconclusive" in failure_message(result)


def test_identity_probe_only_reads():
    assert run(identity_probe_job).success
    assert len(STATEMENTS) == 1 and STATEMENTS[0].startswith("SELECT CURRENT_USER()")


def test_secondary_roles_parsing():
    assert active_secondary_roles('{"roles":"","value":""}') == []
    assert active_secondary_roles(None) == []
    assert active_secondary_roles('{"roles":"A,B","value":""}') == ["A", "B"]


def test_prod_deployment_needs_an_explicit_forbidden_database():
    assert default_forbidden_database("stage") == "ANALYTICS_PROD"
    assert default_forbidden_database(None) == "ANALYTICS_PROD"
    with pytest.raises(dg.Failure):
        default_forbidden_database("prod")


def test_both_jobs_are_manual_only_and_unscheduled():
    repo = defs.get_repository_def()
    scheduled = {s.job_name for s in repo.schedule_defs}
    for job in (identity_probe_job, write_boundary_job):
        assert job.tags[MANUAL_ONLY_TAG] == "true"
        assert job.name not in scheduled
