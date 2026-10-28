"""Label creation and title-prefix labels, against a fake GitHub API."""

import urllib.error

import pytest
from pr_labels import TITLE_LABELS, ensure, title_label, wanted_labels

REPO = "org/repo"


class FakeGitHub:
    def __init__(self, labels=(), get_error=None):
        self.labels = set(labels)
        self.get_error = get_error
        self.calls = []

    def __call__(self, method, path, payload):
        self.calls.append((method, path, payload))
        if method == "GET":
            if self.get_error:
                raise self.get_error
            name = path.rsplit("/", 1)[1]
            if name not in self.labels:
                raise urllib.error.HTTPError(path, 404, "Not Found", {}, None)
            return {"name": name}
        if method == "POST":
            self.labels.add(payload["name"])
            return {}
        raise AssertionError(f"unexpected {method} {path}")


def test_missing_labels_are_created_existing_ones_left():
    gh = FakeGitHub(labels={"dagster"})
    assert ensure(gh, REPO, ["dagster", "brand-new"]) == ["brand-new"]
    assert gh.labels == {"dagster", "brand-new"}
    assert [c[0] for c in gh.calls].count("POST") == 1


def test_errors_other_than_404_fail():
    gh = FakeGitHub(get_error=urllib.error.HTTPError("u", 500, "boom", {}, None))
    with pytest.raises(urllib.error.HTTPError):
        ensure(gh, REPO, ["dagster"])


def test_wanted_labels_cover_the_config_title_prefixes_and_stale_exemptions():
    names = wanted_labels()
    assert "dagster" in names and "stacks" in names
    assert set(TITLE_LABELS) <= set(names)
    assert {"pinned", "blocked", "on-hold", "ci-alarm"} <= set(names)
    assert len(names) == len(set(names))


@pytest.mark.parametrize(
    ("title", "label"),
    [
        ("feat: add a thing", "feat"),
        ("fix(dagster): route at run time", "fix"),
        ("docs!: rewrite", "docs"),
        ("ci: stale bot (#22)", "ci"),
        ("chore(deps): bump ruff", None),
        ("feature: not a prefix", None),
        ("fix without a colon", None),
        ("Fix: wrong case", None),
    ],
)
def test_title_label(title, label):
    assert title_label(title) == label
