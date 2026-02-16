"""Slack alerting utilities.

Provides a factory function to create Dagster hooks that send Slack
notifications on pipeline failures.

Usage:
    from src.utils.alerts import make_slack_on_failure_hook

    slack_hook = make_slack_on_failure_hook(
        webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
        channel="#data-alerts",
    )

    @job(hooks={slack_hook})
    def my_job():
        ...
"""

import json
from urllib.request import Request, urlopen

from dagster import HookContext, failure_hook


def make_slack_on_failure_hook(
    webhook_url: str,
    channel: str | None = None,
    mention: str = "",
):
    """Create a Dagster failure hook that posts to Slack.

    Args:
        webhook_url: Slack incoming webhook URL.
        channel: Optional channel override (uses webhook default if None).
        mention: Optional user/group mention (e.g. "<!here>" or "<@U123>").

    Returns:
        A Dagster ``@failure_hook`` function.
    """

    @failure_hook
    def slack_on_failure(context: HookContext):
        op_name = context.op.name if context.op else "unknown"
        job_name = context.job_name or "unknown"

        text_parts = []
        if mention:
            text_parts.append(mention)
        text_parts.append(":red_circle: *Pipeline failure*")
        text_parts.append(f"*Job:* `{job_name}`")
        text_parts.append(f"*Op:* `{op_name}`")

        if context.op_exception:
            error_msg = str(context.op_exception)[:500]
            text_parts.append(f"*Error:* ```{error_msg}```")

        payload = {"text": "\n".join(text_parts)}
        if channel:
            payload["channel"] = channel

        req = Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urlopen(req, timeout=10)

        context.log.info(f"Slack notification sent for {job_name}/{op_name}")

    return slack_on_failure
