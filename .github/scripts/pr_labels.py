"""PR labels that actions/labeler cannot do itself.

    python .github/scripts/pr_labels.py ensure            # before the labeler
    python .github/scripts/pr_labels.py title --pr 123    # after it

`ensure` creates every label in .github/labeler.yml, TITLE_LABELS, and the
stale bot's EXEMPT list (.github/workflows/stale.yml) that does not exist yet:
the labeler errors on a missing label, and the stale bot tells people to add
`pinned`, which must exist. A 404 means missing; any other error fails the
job rather than being swallowed.

`title` adds the label for a Conventional-Commit title prefix (`feat:`,
`fix(scope):`, `docs!:` ...). Additive only: it never removes a label.

Stdlib + PyYAML. Needs GITHUB_TOKEN (issues: write, pull-requests: write),
GITHUB_REPOSITORY, and for `title` the PR title in PR_TITLE (from `env:`,
never interpolated into a shell).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import urllib.error
from pathlib import Path
from urllib.parse import quote

import yaml
from tracker_issue import Http, github_http

LABELER_CONFIG = Path(__file__).resolve().parents[1] / "labeler.yml"
STALE_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/stale.yml"
TITLE_LABELS = ("feat", "fix", "docs", "ci")
TITLE_PREFIX = re.compile(r"^(\w+)(\([^)]*\))?!?:\s")
COLOR = "ededed"


def wanted_labels(
    config: Path = LABELER_CONFIG, stale: Path = STALE_WORKFLOW
) -> list[str]:
    names = list(yaml.safe_load(config.read_text()))
    names += TITLE_LABELS
    names += yaml.safe_load(stale.read_text())["env"]["EXEMPT"].split(",")
    return list(dict.fromkeys(names))


def ensure(http: Http, repo: str, names: list[str]) -> list[str]:
    created = []
    for name in names:
        try:
            http("GET", f"/repos/{repo}/labels/{quote(name, safe='')}", None)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            http("POST", f"/repos/{repo}/labels", {"name": name, "color": COLOR})
            created.append(name)
    return created


def title_label(title: str) -> str | None:
    match = TITLE_PREFIX.match(title)
    if match and match.group(1) in TITLE_LABELS:
        return match.group(1)
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ensure")
    title = sub.add_parser("title")
    title.add_argument("--pr", type=int, required=True)
    args = parser.parse_args(argv)

    http = github_http(os.environ["GITHUB_TOKEN"])
    repo = os.environ["GITHUB_REPOSITORY"]
    if args.command == "ensure":
        created = ensure(http, repo, wanted_labels())
        print(f"created: {', '.join(created)}" if created else "all labels exist")
        return 0
    label = title_label(os.environ.get("PR_TITLE", ""))
    if label is None:
        print("no recognised title prefix: nothing to add")
        return 0
    http("POST", f"/repos/{repo}/issues/{args.pr}/labels", {"labels": [label]})
    print(f"added {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
