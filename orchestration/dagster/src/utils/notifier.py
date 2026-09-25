"""One notification model, rendered to Slack Block Kit by severity.

Every alert is a ``Notification``. Severity decides the layout:

* ``success`` renders as one scannable line;
* ``warning`` and ``failure`` expand to a header, the summary, fields, a
  detail block, and links.

Which channel each severity goes to is decided in ``channel_for`` and nowhere
else. This module only renders and sends; persisting run events is
``src/telemetry/``.

Usage:
    from src.utils.notifier import Notification, Severity, send

    send(
        Notification(
            job="orders_export",
            severity=Severity.WARNING,
            summary="3 recipients received 0 rows",
            fields={"Recipients": "3", "Window": "2026-01-05"},
        ),
        webhook_url=...,
    )

For op failures, attach ``make_slack_on_failure_hook(webhook_url)`` to a job.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.request import Request, urlopen

from dagster import HookContext, failure_hook

MAX_DETAIL = 2500  # Slack caps a text object at 3000 characters


class Severity(StrEnum):
    SUCCESS = "success"
    WARNING = "warning"
    FAILURE = "failure"


ICONS = {
    Severity.SUCCESS: ":white_check_mark:",
    Severity.WARNING: ":warning:",
    Severity.FAILURE: ":red_circle:",
}

CHANNELS = {
    Severity.SUCCESS: "#data-runs",
    Severity.WARNING: "#data-alerts",
    Severity.FAILURE: "#data-alerts",
}


def channel_for(severity: Severity) -> str:
    """The single place severity maps to a channel."""
    return CHANNELS[severity]


@dataclass(frozen=True)
class Notification:
    job: str
    severity: Severity
    summary: str
    fields: dict[str, str] = field(default_factory=dict)
    detail: str | None = None
    links: dict[str, str] = field(default_factory=dict)  # label -> URL
    run_url: str | None = None
    mention: str = ""  # e.g. "<!here>"; used on warning and failure only


def render(notification: Notification) -> dict:
    """Block Kit payload (without the channel) for one notification."""
    n = notification
    icon = ICONS[n.severity]
    if n.severity is Severity.SUCCESS:
        line = f"{icon} *{n.job}* {n.summary}"
        if n.run_url:
            line += f" <{n.run_url}|run>"
        return {
            "text": f"{n.job}: {n.summary}",
            "blocks": [{"type": "section", "text": _mrkdwn(line)}],
        }

    title = f"{n.job} {n.severity.value}"
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{icon} {title}", "emoji": True},
        },
        {
            "type": "section",
            "text": _mrkdwn(" ".join(filter(None, [n.mention, n.summary]))),
        },
    ]
    items = list(n.fields.items())
    for start in range(0, len(items), 10):  # Slack allows 10 fields per section
        blocks.append(
            {
                "type": "section",
                "fields": [
                    _mrkdwn(f"*{k}*\n{v}") for k, v in items[start : start + 10]
                ],
            }
        )
    if n.detail:
        blocks.append(
            {"type": "section", "text": _mrkdwn(f"```{n.detail[:MAX_DETAIL]}```")}
        )
    links = {**({"Run": n.run_url} if n.run_url else {}), **n.links}
    if links:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    _mrkdwn(" | ".join(f"<{u}|{label}>" for label, u in links.items()))
                ],
            }
        )
    return {"text": f"{title}: {n.summary}", "blocks": blocks}


def _mrkdwn(text: str) -> dict:
    return {"type": "mrkdwn", "text": text}


def _post(url: str, payload: dict) -> None:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urlopen(request, timeout=10)


def send(
    notification: Notification,
    webhook_url: str,
    *,
    post: Callable[[str, dict], None] = _post,
) -> dict:
    """Render, route, and post. Returns the payload sent."""
    payload = {**render(notification), "channel": channel_for(notification.severity)}
    post(webhook_url, payload)
    return payload


def make_slack_on_failure_hook(
    webhook_url: str,
    mention: str = "",
    run_url_template: str | None = None,
    *,
    post: Callable[[str, dict], None] = _post,
):
    """A Dagster ``@failure_hook`` that sends a failure ``Notification``.

    Args:
        webhook_url: Slack incoming webhook URL.
        mention: Optional user/group mention (e.g. "<!here>").
        run_url_template: e.g. "https://dagster.example.com/runs/{run_id}".
    """

    @failure_hook
    def slack_on_failure(context: HookContext):
        op_name = context.op.name if context.op else "unknown"
        job_name = context.job_name or "unknown"
        exception = context.op_exception
        send(
            Notification(
                job=job_name,
                severity=Severity.FAILURE,
                summary=f"Op `{op_name}` failed.",
                fields={"Job": f"`{job_name}`", "Op": f"`{op_name}`"},
                detail=str(exception) if exception else None,
                run_url=(
                    run_url_template.format(run_id=context.run_id)
                    if run_url_template
                    else None
                ),
                mention=mention,
            ),
            webhook_url,
            post=post,
        )
        context.log.info(f"Slack notification sent for {job_name}/{op_name}")

    return slack_on_failure
