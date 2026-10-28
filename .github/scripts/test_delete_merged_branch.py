"""Retarget stacked PRs, then delete the merged branch, against a fake
GitHub API. Every failure before the delete must keep the branch."""

import urllib.error

import pytest
from delete_merged_branch import KeepBranch, retarget_then_delete

REPO = "org/repo"


def http_error(code):
    return urllib.error.HTTPError("url", code, "error", {}, None)


class FakeGitHub:
    def __init__(self, prs=(), branches=("feature",), protected=(), fail=None):
        self.prs = {p["number"]: dict(p) for p in prs}
        self.branches = set(branches)
        self.protected = set(protected)
        self.fail = fail or {}  # (method, path prefix) -> exception
        self.calls = []

    def __call__(self, method, path, payload):
        self.calls.append((method, path, payload))
        for (fail_method, prefix), error in self.fail.items():
            if method == fail_method and path.startswith(prefix):
                raise error
        if method == "GET" and "/branches/" in path:
            name = path.rsplit("/", 1)[1]
            if name not in self.branches:
                raise http_error(404)
            return {"name": name, "protected": name in self.protected}
        if method == "GET" and "/pulls?" in path:
            base = path.split("base=")[1].split("&")[0]
            return [p for p in self.prs.values() if p["base"] == base]
        if method == "PATCH":
            self.prs[int(path.rsplit("/", 1)[1])]["base"] = payload["base"]
            return {}
        if method == "DELETE":
            self.branches.discard(path.rsplit("/", 1)[1])
            return None
        raise AssertionError(f"unexpected call {method} {path}")

    def deleted(self):
        return [c for c in self.calls if c[0] == "DELETE"]


def run(gh, head="feature", base="main"):
    return retarget_then_delete(gh, REPO, head, base, "main")


def test_stacked_pr_is_retargeted_before_the_branch_is_deleted():
    gh = FakeGitHub(prs=[{"number": 2, "base": "feature"}])
    log = run(gh)
    assert gh.prs[2]["base"] == "main"
    assert log == ["retargeted #2 feature -> main", "deleted feature"]
    methods = [c[0] for c in gh.calls]
    assert methods.index("PATCH") < methods.index("DELETE")


def test_no_dependents_just_deletes():
    gh = FakeGitHub()
    assert run(gh) == ["deleted feature"]


def test_failed_retarget_keeps_the_branch():
    gh = FakeGitHub(
        prs=[{"number": 2, "base": "feature"}],
        fail={("PATCH", f"/repos/{REPO}/pulls/2"): http_error(422)},
    )
    with pytest.raises(KeepBranch, match="feature kept"):
        run(gh)
    assert gh.deleted() == []


def test_failed_lookup_keeps_the_branch():
    gh = FakeGitHub(fail={("GET", f"/repos/{REPO}/pulls?"): http_error(500)})
    with pytest.raises(KeepBranch):
        run(gh)
    assert gh.deleted() == []


def test_network_error_keeps_the_branch():
    gh = FakeGitHub(
        fail={("GET", f"/repos/{REPO}/pulls?"): urllib.error.URLError("timed out")}
    )
    with pytest.raises(KeepBranch):
        run(gh)
    assert gh.deleted() == []


def test_a_dependent_still_on_the_branch_keeps_it():
    gh = FakeGitHub(prs=[{"number": 2, "base": "feature"}])
    real = gh.__call__

    def ignore_patch(method, path, payload):  # PATCH "succeeds" but changes nothing
        if method == "PATCH":
            gh.calls.append((method, path, payload))
            return {}
        return real(method, path, payload)

    with pytest.raises(KeepBranch, match="#2"):
        retarget_then_delete(ignore_patch, REPO, "feature", "main", "main")
    assert gh.deleted() == []


def test_default_and_protected_branches_are_kept():
    gh = FakeGitHub(branches=("main", "release"), protected=("release",))
    assert run(gh, head="main") == ["main is the default branch: kept"]
    assert run(gh, head="release") == ["release is protected: kept"]
    assert gh.deleted() == []


def test_branch_already_gone_is_not_an_error():
    gh = FakeGitHub(branches=())
    assert run(gh) == ["feature no longer exists: nothing to do"]


def test_branch_names_are_url_encoded():
    gh = FakeGitHub(branches=("fix%2Fthing",))
    run(gh, head="fix/thing")
    assert ("DELETE", f"/repos/{REPO}/git/refs/heads/fix%2Fthing", None) in gh.calls
