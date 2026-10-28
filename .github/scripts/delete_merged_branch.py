"""After a merge: retarget every open PR stacked on the merged branch, then
delete the branch.

    python .github/scripts/delete_merged_branch.py

GitHub closes any open PR whose base branch disappears. It retargets them
only when the branch is deleted through its own post-merge button or
auto-delete, not via `DELETE /git/refs` from a workflow, and a PR closed that
way cannot be reopened or retargeted. So dependents move first.

Fails closed: if listing or retargeting any dependent fails, or one is still
based on the branch afterwards, the branch is kept and the job fails. A stale
branch is cheap; a closed stack is not. Skips the default branch and
protected branches. Fork PRs are filtered out by the workflow.

Stdlib only. Needs GITHUB_TOKEN (contents: write, pull-requests: write),
GITHUB_REPOSITORY, and HEAD_BRANCH / BASE_BRANCH / DEFAULT_BRANCH from the
pull_request event (passed through `env:`, never interpolated into a shell).
"""

from __future__ import annotations

import os
import sys
import urllib.error
from urllib.parse import quote

from tracker_issue import Http, github_http


class KeepBranch(RuntimeError):
    """Something failed before the delete; the branch must stay."""


def open_prs_based_on(http: Http, repo: str, branch: str) -> list[dict]:
    prs, page = [], 1
    while True:
        batch = http(
            "GET",
            f"/repos/{repo}/pulls?state=open&base={quote(branch, safe='')}"
            f"&per_page=100&page={page}",
            None,
        )
        prs += batch or []
        if not batch or len(batch) < 100:
            return prs
        page += 1


def retarget_then_delete(
    http: Http, repo: str, head: str, base: str, default: str
) -> list[str]:
    """Returns a log of what was done. Raises KeepBranch on any failure
    before the delete."""
    if head == default:
        return [f"{head} is the default branch: kept"]
    ref = quote(head, safe="")
    try:
        branch = http("GET", f"/repos/{repo}/branches/{ref}", None)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return [f"{head} no longer exists: nothing to do"]
        raise KeepBranch(f"{error.code} {error.reason}: {head} kept") from error
    if branch.get("protected"):
        return [f"{head} is protected: kept"]

    log = []
    try:
        for pr in open_prs_based_on(http, repo, head):
            http("PATCH", f"/repos/{repo}/pulls/{pr['number']}", {"base": base})
            log.append(f"retargeted #{pr['number']} {head} -> {base}")
        left = open_prs_based_on(http, repo, head)
    except (urllib.error.URLError, TimeoutError) as error:  # HTTPError included
        raise KeepBranch(f"{error}: {head} kept") from error
    if left:
        numbers = ", ".join(f"#{pr['number']}" for pr in left)
        raise KeepBranch(f"still based on {head} after retarget: {numbers}; kept")

    try:
        http("DELETE", f"/repos/{repo}/git/refs/heads/{ref}", None)
    except urllib.error.HTTPError as error:
        if error.code != 422:  # 422: the ref is already gone
            raise
        log.append(f"{head} already deleted")
        return log
    log.append(f"deleted {head}")
    return log


def main() -> int:
    env = os.environ
    try:
        log = retarget_then_delete(
            github_http(env["GITHUB_TOKEN"]),
            env["GITHUB_REPOSITORY"],
            env["HEAD_BRANCH"],
            env["BASE_BRANCH"],
            env["DEFAULT_BRANCH"],
        )
    except KeepBranch as error:
        print(f"::error::{error}")
        return 1
    print("\n".join(log))
    return 0


if __name__ == "__main__":
    sys.exit(main())
