"""Notifier: each severity renders to a Block Kit snapshot; routing lives in
channel_for; the failure hook builds a Notification from a real failed run.

Snapshots are in tests/snapshots/. After an intended rendering change, rewrite
them with `UPDATE_SNAPSHOTS=1 uv run pytest tests/test_notifier.py` and review
the diff."""

import json
import os
from pathlib import Path

import dagster as dg
import pytest

from src.utils.notifier import (
    Notification,
    Severity,
    channel_for,
    make_slack_on_failure_hook,
    render,
    send,
)

SNAPSHOTS = Path(__file__).parent / "snapshots"

EXAMPLES = {
    Severity.SUCCESS: Notification(
        job="daily_build",
        severity=Severity.SUCCESS,
        summary="built 42 models in 6m 10s",
        run_url="https://dagster.example.com/runs/abc123",
    ),
    Severity.WARNING: Notification(
        job="orders_export",
        severity=Severity.WARNING,
        summary="2 of 5 recipients received 0 rows.",
        fields={"Recipients": "5", "Empty": "2", "Window": "2026-01-05"},
        links={"Run log": "https://dagster.example.com/runs/def456/logs"},
        run_url="https://dagster.example.com/runs/def456",
    ),
    Severity.FAILURE: Notification(
        job="daily_build",
        severity=Severity.FAILURE,
        summary="Op `dbt_project_assets` failed.",
        fields={"Job": "`daily_build`", "Op": "`dbt_project_assets`"},
        detail="Database Error in model fct_orders: division by zero",
        run_url="https://dagster.example.com/runs/ghi789",
        mention="<!here>",
    ),
}


@pytest.mark.parametrize("severity", list(Severity))
def test_render_matches_snapshot(severity):
    rendered = json.dumps(render(EXAMPLES[severity]), indent=2, sort_keys=True) + "\n"
    path = SNAPSHOTS / f"notifier_{severity.value}.json"
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        path.write_text(rendered)
    assert rendered == path.read_text()


def test_success_is_one_line():
    assert len(render(EXAMPLES[Severity.SUCCESS])["blocks"]) == 1


def test_fields_split_into_sections_of_ten():
    many = Notification(
        job="j",
        severity=Severity.WARNING,
        summary="s",
        fields={f"k{i}": str(i) for i in range(12)},
    )
    sections = [b for b in render(many)["blocks"] if "fields" in b]
    assert [len(s["fields"]) for s in sections] == [10, 2]


def test_send_routes_by_severity():
    sent = []
    for severity, notification in EXAMPLES.items():
        send(
            notification,
            "https://hooks.example.com/x",
            post=lambda u, p: sent.append(p),
        )
        assert sent[-1]["channel"] == channel_for(severity)


def test_failure_hook_sends_a_failure_notification():
    sent = []
    hook = make_slack_on_failure_hook(
        "https://hooks.example.com/x",
        run_url_template="https://dagster.example.com/runs/{run_id}",
        post=lambda url, payload: sent.append(payload),
    )

    @dg.op
    def explode():
        raise RuntimeError("upstream exploded")

    @dg.job(hooks={hook})
    def fragile_job():
        explode()

    result = fragile_job.execute_in_process(raise_on_error=False)

    [payload] = sent
    assert payload["channel"] == channel_for(Severity.FAILURE)
    text = json.dumps(payload)
    assert "fragile_job" in text
    assert "upstream exploded" in text
    assert f"/runs/{result.run_id}" in text
