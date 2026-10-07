"""One living issue per marker, against a fake GitHub API."""

import pytest
from tracker_issue import sync

REPO = "org/repo"


class FakeGitHub:
    def __init__(self, issues=()):
        self.issues = {i["number"]: dict(i) for i in issues}
        self.calls = []

    def __call__(self, method, path, payload):
        self.calls.append((method, path, payload))
        if method == "GET":
            return [i for i in self.issues.values() if i["state"] == "open"]
        if method == "POST" and path.endswith("/issues"):
            number = max(self.issues, default=0) + 1
            self.issues[number] = {"number": number, "state": "open", **payload}
            return {"number": number}
        if method == "PATCH":
            self.issues[int(path.rsplit("/", 1)[1])].update(payload)
        return {}


def run(gh, state, body="details"):
    return sync(gh, REPO, "[audit]", "security", "Vulnerabilities", body, state)


def test_opens_once_then_updates():
    gh = FakeGitHub()
    assert run(gh, "open") == "opened #1"
    assert run(gh, "open", body="newer") == "updated #1"
    assert len(gh.issues) == 1
    assert gh.issues[1]["body"] == "newer"
    assert gh.issues[1]["title"] == "[audit] Vulnerabilities"


def test_closes_when_clear_and_reopens_as_a_new_issue():
    gh = FakeGitHub()
    run(gh, "open")
    assert run(gh, "closed", body="All clean.") == "closed #1"
    assert gh.issues[1]["state"] == "closed"
    assert run(gh, "closed") == "nothing to close"
    assert run(gh, "open") == "opened #2"


@pytest.mark.parametrize(
    "other",
    [
        {"number": 7, "state": "open", "title": "Unrelated bug"},
        {"number": 8, "state": "open", "title": "[audit] a PR", "pull_request": {}},
    ],
)
def test_ignores_issues_without_the_marker_and_pull_requests(other):
    gh = FakeGitHub([other])
    assert run(gh, "open").startswith("opened #")  # a new issue, not the other one
    assert gh.issues[other["number"]]["title"] == other["title"]
