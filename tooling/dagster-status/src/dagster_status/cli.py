"""Read-only Dagster status over the GraphQL API.

    dagster-status status              daemon health and in-progress runs
    dagster-status recent [-n 20]      latest runs
    dagster-status errors [-n 20]      latest failed runs
    dagster-status schedules           every schedule and its live state
    dagster-status sensors             every sensor and its live state
    dagster-status triggers <job>      schedules and sensors that can launch <job>

``--csv`` prints CSV instead of a table. The endpoint is ``--url`` or
``DAGSTER_URL`` (default ``http://localhost:3000/graphql``, OSS); for Dagster+
use ``https://<org>.dagster.cloud/<deployment>/graphql`` and set
``DAGSTER_CLOUD_API_TOKEN``.

State comes from the instance, never from code: ``default_status`` applies
only on a definition's first load, and a stopped schedule's job can still be
launched by a running sensor.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request

from dagster_status import queries

DEFAULT_URL = "http://localhost:3000/graphql"
TIMEOUT_SECONDS = 30
IN_PROGRESS = ["QUEUED", "STARTING", "STARTED", "CANCELING"]


class StatusError(Exception):
    pass


def execute(
    url: str, token: str | None, document: str, variables: dict | None = None
) -> dict:
    """POST one GraphQL query. Sends the query, its variables, and the token
    header if set; nothing else."""
    if not document.lstrip().startswith("query"):
        raise StatusError("refusing to send anything but a query")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Dagster-Cloud-Api-Token"] = token
    body = json.dumps({"query": document, "variables": variables or {}}).encode()
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise StatusError(f"{url}: {exc}") from exc
    if payload.get("errors"):
        raise StatusError(
            "; ".join(e.get("message", str(e)) for e in payload["errors"])
        )
    return payload["data"]


def _unwrap(result: dict, expected: str) -> dict:
    if result.get("__typename") != expected:
        raise StatusError(
            result.get("message") or f"unexpected {result.get('__typename')}"
        )
    return result


def _time(epoch: float | None) -> str:
    if epoch is None:
        return ""
    return dt.datetime.fromtimestamp(epoch, dt.UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def _runs(url, token, limit, statuses=None) -> list[dict]:
    data = execute(url, token, queries.RUNS, {"limit": limit, "statuses": statuses})
    return _unwrap(data["runsOrError"], "Runs")["results"]


def _run_rows(runs) -> list[dict]:
    return [
        {
            "run_id": r["runId"],
            "job": r["jobName"],
            "status": r["status"],
            "started": _time(r["startTime"]),
            "ended": _time(r["endTime"]),
        }
        for r in runs
    ]


def _repositories(url, token) -> list[dict]:
    data = execute(url, token, queries.INSTIGATORS)
    return _unwrap(data["repositoriesOrError"], "RepositoryConnection")["nodes"]


def schedule_rows(repositories) -> list[dict]:
    return [
        {
            "location": repo["location"]["name"],
            "schedule": s["name"],
            "job": s["pipelineName"],
            "cron": s["cronSchedule"],
            "timezone": s["executionTimezone"] or "",
            "state": s["scheduleState"]["status"],
        }
        for repo in repositories
        for s in repo["schedules"]
    ]


def sensor_rows(repositories) -> list[dict]:
    return [
        {
            "location": repo["location"]["name"],
            "sensor": s["name"],
            "type": s["sensorType"],
            "jobs": ",".join(t["pipelineName"] for t in s["targets"] or []),
            "state": s["sensorState"]["status"],
        }
        for repo in repositories
        for s in repo["sensors"]
    ]


def trigger_rows(repositories, job: str) -> list[dict]:
    rows = [
        {
            "kind": "schedule",
            "name": r["schedule"],
            "location": r["location"],
            "state": r["state"],
        }
        for r in schedule_rows(repositories)
        if r["job"] == job
    ]
    rows += [
        {
            "kind": "sensor",
            "name": r["sensor"],
            "location": r["location"],
            "state": r["state"],
        }
        for r in sensor_rows(repositories)
        if job in r["jobs"].split(",")
    ]
    return rows


def run_command(args) -> list[dict]:
    url, token = args.url, args.token
    if args.command == "status":
        health = execute(url, token, queries.DAEMON_HEALTH)["instance"]["daemonHealth"]
        rows = [
            {
                "item": f"daemon {d['daemonType']}",
                "value": "healthy" if d["healthy"] else "UNHEALTHY",
            }
            for d in health["allDaemonStatuses"]
            if d["required"]
        ]
        running = _runs(url, token, 1000, IN_PROGRESS)
        return [*rows, {"item": "runs in progress", "value": str(len(running))}]
    if args.command == "recent":
        return _run_rows(_runs(url, token, args.limit))
    if args.command == "errors":
        return _run_rows(_runs(url, token, args.limit, ["FAILURE"]))
    if args.command == "schedules":
        return schedule_rows(_repositories(url, token))
    if args.command == "sensors":
        return sensor_rows(_repositories(url, token))
    if args.command == "triggers":
        return trigger_rows(_repositories(url, token), args.job)
    raise StatusError(f"unknown command {args.command}")


def render(rows: list[dict], as_csv: bool, out=None) -> None:
    out = out or sys.stdout
    if not rows:
        if not as_csv:
            print("(none)", file=out)
        return
    columns = list(rows[0])
    if as_csv:
        writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        return
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in columns}
    print("  ".join(c.ljust(widths[c]) for c in columns).rstrip(), file=out)
    for row in rows:
        print(
            "  ".join(str(row[c]).ljust(widths[c]) for c in columns).rstrip(), file=out
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="dagster-status", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument("--url", default=os.environ.get("DAGSTER_URL", DEFAULT_URL))
    parser.add_argument("--csv", action="store_true", help="CSV instead of a table")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="daemon health and in-progress runs")
    for name in ("recent", "errors"):
        sub = commands.add_parser(
            name, help=f"latest {'failed ' if name == 'errors' else ''}runs"
        )
        sub.add_argument("-n", "--limit", type=int, default=20)
    commands.add_parser("schedules", help="schedules and their live state")
    commands.add_parser("sensors", help="sensors and their live state")
    triggers = commands.add_parser("triggers", help="what can launch a job")
    triggers.add_argument("job")
    args = parser.parse_args(argv)
    args.token = os.environ.get("DAGSTER_CLOUD_API_TOKEN") or None
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        render(run_command(args), args.csv)
    except StatusError as exc:
        print(f"dagster-status: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
