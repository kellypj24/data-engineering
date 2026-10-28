"""Every command against mocked GraphQL responses, in both output formats.
No network: urlopen is replaced."""

import io
import json
from unittest.mock import patch

import pytest

from dagster_status import cli

RUNS = {
    "runsOrError": {
        "__typename": "Runs",
        "results": [
            {
                "runId": "r1",
                "jobName": "extract_job",
                "status": "SUCCESS",
                "startTime": 0.0,
                "endTime": 60.0,
            },
            {
                "runId": "r2",
                "jobName": "transform_job",
                "status": "FAILURE",
                "startTime": 120.0,
                "endTime": None,
            },
        ],
    }
}
HEALTH = {
    "instance": {
        "daemonHealth": {
            "allDaemonStatuses": [
                {"daemonType": "SCHEDULER", "required": True, "healthy": True},
                {"daemonType": "SENSOR", "required": True, "healthy": False},
                {"daemonType": "BACKFILL", "required": False, "healthy": None},
            ]
        }
    }
}
REPOS = {
    "repositoriesOrError": {
        "__typename": "RepositoryConnection",
        "nodes": [
            {
                "name": "__repository__",
                "location": {"name": "src"},
                "schedules": [
                    {
                        "name": "daily_asset_schedule",
                        "cronSchedule": "0 6 * * *",
                        "executionTimezone": "UTC",
                        "pipelineName": "extract_job",
                        "scheduleState": {"status": "STOPPED"},
                    }
                ],
                "sensors": [
                    {
                        "name": "retry_extract",
                        "sensorType": "STANDARD",
                        "targets": [{"pipelineName": "extract_job"}],
                        "sensorState": {"status": "RUNNING"},
                    },
                    {
                        "name": "freshness_watch",
                        "sensorType": "STANDARD",
                        "targets": [],
                        "sensorState": {"status": "RUNNING"},
                    },
                ],
            }
        ],
    }
}


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def serve(*payloads):
    """Patch urlopen to answer with each payload in turn; record requests."""
    sent = []
    replies = iter(payloads)

    def urlopen(request, timeout):
        sent.append(request)
        return FakeResponse(json.dumps({"data": next(replies)}).encode())

    return patch("dagster_status.cli.urllib.request.urlopen", urlopen), sent


def run(argv, *payloads, capsys):
    patcher, sent = serve(*payloads)
    with patcher:
        code = cli.main(argv)
    out = capsys.readouterr()
    return code, out.out, out.err, sent


def test_status_reports_required_daemons_and_in_progress_runs(capsys):
    code, out, _, sent = run(["status"], HEALTH, RUNS, capsys=capsys)
    assert code == 0
    assert "daemon SCHEDULER  healthy" in out
    assert "daemon SENSOR     UNHEALTHY" in out
    assert "BACKFILL" not in out
    assert "runs in progress  2" in out
    assert json.loads(sent[1].data)["variables"]["statuses"] == cli.IN_PROGRESS


def test_recent_table(capsys):
    code, out, _, _ = run(["recent", "-n", "2"], RUNS, capsys=capsys)
    lines = out.splitlines()
    assert code == 0
    assert lines[0].split() == ["run_id", "job", "status", "started", "ended"]
    assert lines[1].split()[:3] == ["r1", "extract_job", "SUCCESS"]
    assert "1970-01-01 00:01:00Z" in lines[1]


def test_recent_csv(capsys):
    _, out, _, _ = run(["--csv", "recent"], RUNS, capsys=capsys)
    assert out.splitlines() == [
        "run_id,job,status,started,ended",
        "r1,extract_job,SUCCESS,1970-01-01 00:00:00Z,1970-01-01 00:01:00Z",
        "r2,transform_job,FAILURE,1970-01-01 00:02:00Z,",
    ]


def test_errors_filters_to_failures(capsys):
    _, _, _, sent = run(["errors"], RUNS, capsys=capsys)
    assert json.loads(sent[0].data)["variables"] == {
        "limit": 20,
        "statuses": ["FAILURE"],
    }


@pytest.mark.parametrize("as_csv", [False, True])
def test_schedules(capsys, as_csv):
    _, out, _, _ = run(
        [*(["--csv"] if as_csv else []), "schedules"], REPOS, capsys=capsys
    )
    if as_csv:
        assert (
            out.splitlines()[1]
            == "src,daily_asset_schedule,extract_job,0 6 * * *,UTC,STOPPED"
        )
    else:
        assert out.splitlines()[1].split()[-1] == "STOPPED"


@pytest.mark.parametrize("as_csv", [False, True])
def test_sensors(capsys, as_csv):
    _, out, _, _ = run(
        [*(["--csv"] if as_csv else []), "sensors"], REPOS, capsys=capsys
    )
    if as_csv:
        assert "src,retry_extract,STANDARD,extract_job,RUNNING" in out.splitlines()
        assert "src,freshness_watch,STANDARD,,RUNNING" in out.splitlines()
    else:
        assert "retry_extract" in out


def test_triggers_lists_a_running_sensor_for_a_job_whose_schedule_is_stopped(capsys):
    _, out, _, _ = run(["--csv", "triggers", "extract_job"], REPOS, capsys=capsys)
    assert out.splitlines() == [
        "kind,name,location,state",
        "schedule,daily_asset_schedule,src,STOPPED",
        "sensor,retry_extract,src,RUNNING",
    ]


def test_triggers_for_an_untriggered_job(capsys):
    _, out, _, _ = run(["triggers", "manual_job"], REPOS, capsys=capsys)
    assert out == "(none)\n"


def test_token_is_sent_only_when_set(capsys, monkeypatch):
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "t0k")
    _, _, _, sent = run(["schedules"], REPOS, capsys=capsys)
    assert sent[0].get_header("Dagster-cloud-api-token") == "t0k"
    monkeypatch.delenv("DAGSTER_CLOUD_API_TOKEN")
    _, _, _, sent = run(["schedules"], REPOS, capsys=capsys)
    assert sent[0].get_header("Dagster-cloud-api-token") is None


def test_graphql_errors_exit_2(capsys):
    patcher = patch(
        "dagster_status.cli.urllib.request.urlopen",
        lambda request, timeout: FakeResponse(
            json.dumps({"errors": [{"message": "boom"}]}).encode()
        ),
    )
    with patcher:
        assert cli.main(["recent"]) == 2
    assert "boom" in capsys.readouterr().err


def test_python_error_result_exits_2(capsys):
    error = {"runsOrError": {"__typename": "PythonError", "message": "storage down"}}
    code, _, err, _ = run(["recent"], error, capsys=capsys)
    assert code == 2
    assert "storage down" in err


def test_unreachable_server_exits_2(capsys):
    code = cli.main(["--url", "http://127.0.0.1:9/graphql", "recent"])
    assert code == 2
    assert "127.0.0.1:9" in capsys.readouterr().err
