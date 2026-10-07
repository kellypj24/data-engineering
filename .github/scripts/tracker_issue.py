"""Keep ONE living tracker issue per marker: create it when a problem appears,
update it while the problem persists, close it when it clears.

    python .github/scripts/tracker_issue.py --marker "[security-audit]" \
        --label security --title "Dependency vulnerabilities" \
        --body-file body.md --state open      # or --state closed

Used by every report-only workflow that raises an alarm (security audit, the
dbt compile gate, ...), so each has one issue to watch instead of one per run.
The marker is a fixed string in the title; the label scopes the search and
tells the stale bot to leave the issue alone.

Stdlib only. Needs GITHUB_TOKEN (issues: write) and GITHUB_REPOSITORY.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from collections.abc import Callable
from pathlib import Path

Http = Callable[[str, str, dict | None], object]


def github_http(token: str) -> Http:
    def call(method: str, path: str, payload: dict | None = None):
        request = urllib.request.Request(
            f"https://api.github.com{path}",
            method=method,
            data=json.dumps(payload).encode() if payload is not None else None,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            return json.loads(body) if body else None

    return call


def find_open(http: Http, repo: str, marker: str, label: str) -> dict | None:
    issues = http(
        "GET", f"/repos/{repo}/issues?state=open&labels={label}&per_page=100", None
    )
    for issue in issues or []:
        if marker in issue["title"] and "pull_request" not in issue:
            return issue
    return None


def sync(
    http: Http, repo: str, marker: str, label: str, title: str, body: str, state: str
) -> str:
    """Make the tracker match `state`. Returns what it did."""
    full_title = f"{marker} {title}"
    existing = find_open(http, repo, marker, label)
    if state == "open":
        if existing:
            http(
                "PATCH",
                f"/repos/{repo}/issues/{existing['number']}",
                {"title": full_title, "body": body},
            )
            return f"updated #{existing['number']}"
        created = http(
            "POST",
            f"/repos/{repo}/issues",
            {"title": full_title, "body": body, "labels": [label]},
        )
        return f"opened #{created['number']}"
    if existing:
        http(
            "POST",
            f"/repos/{repo}/issues/{existing['number']}/comments",
            {"body": body or "Resolved."},
        )
        http(
            "PATCH",
            f"/repos/{repo}/issues/{existing['number']}",
            {"state": "closed", "state_reason": "completed"},
        )
        return f"closed #{existing['number']}"
    return "nothing to close"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Open, update, or close one tracker issue per marker."
    )
    parser.add_argument("--marker", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--body-file", type=Path)
    parser.add_argument("--state", choices=["open", "closed"], required=True)
    args = parser.parse_args(argv)
    body = args.body_file.read_text() if args.body_file else ""
    http = github_http(os.environ["GITHUB_TOKEN"])
    print(
        sync(
            http,
            os.environ["GITHUB_REPOSITORY"],
            args.marker,
            args.label,
            args.title,
            body,
            args.state,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
