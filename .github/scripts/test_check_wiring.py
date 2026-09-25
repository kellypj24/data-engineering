"""Removing any one wiring for a tool fails check_wiring with a message naming
the tool and the surface. Each case runs against a copy of the repo's wiring
files with one edit applied."""

import shutil
from pathlib import Path

import pytest
from check_wiring import check

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def repo(tmp_path):
    for name in ("justfile", "README.md", ".github/dependabot.yml"):
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / name, tmp_path / name)
    shutil.copytree(REPO / ".github/workflows", tmp_path / ".github/workflows")
    for pattern in ("*/*/mod.just", "*/*/pyproject.toml", "*/*/**/*.tf"):
        for source in REPO.glob(pattern):
            if ".terraform" in source.parts or "archive" in source.parts:
                continue
            target = tmp_path / source.relative_to(REPO)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(source, target)
    return tmp_path


def edit(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    assert old in text, f"{old!r} not in {path}"
    path.write_text(text.replace(old, new, 1))


def test_repo_is_fully_wired():
    assert check(REPO) == []


@pytest.mark.parametrize(
    ("file", "old", "new", "expected"),
    [
        (
            "justfile",
            " dbt::test",
            "",
            "dbt: not wired into the root justfile `test` recipe",
        ),
        (
            "justfile",
            " file-export::fmt-check",
            "",
            "file-export: not wired into the root justfile `fmt-check` recipe",
        ),
        (
            ".github/workflows/ci.yml",
            "- 'extract_load/dlt/**'",
            "- 'extract_load/dlt/other/**'",
            "dlt: not wired into the ci.yml paths-filter",
        ),
        (
            ".github/workflows/ci.yml",
            "  test-temporal:\n    runs-on",
            "  test-temporal-renamed:\n    runs-on",
            "temporal: not wired into ci.yml job `test-temporal`",
        ),
        (
            ".github/workflows/ci.yml",
            "path: orchestration/prefect\n",
            "path: orchestration/prefect-renamed\n",
            "prefect: not wired into the ci.yml lint matrix",
        ),
        (
            ".github/workflows/ci.yml",
            "            transformation/dbt\n",
            "",
            "dbt: not wired into the ci.yml lockfiles job",
        ),
        (
            ".github/dependabot.yml",
            "directory: /orchestration/airflow",
            "directory: /orchestration/airflow-renamed",
            "airflow: not wired into dependabot.yml (package-ecosystem: uv)",
        ),
        (
            "README.md",
            "](orchestration/dagster/)",
            "](orchestration/)",
            "dagster: not wired into the README tool table",
        ),
        (
            ".github/workflows/terraform-validate.yml",
            "run: terraform test",
            "run: terraform plan",
            "airbyte: not wired into a workflow job running `terraform test`",
        ),
        (
            ".github/dependabot.yml",
            "package-ecosystem: terraform",
            "package-ecosystem: docker",
            "airbyte: not wired into dependabot.yml (package-ecosystem: terraform)",
        ),
    ],
)
def test_missing_wiring_is_named(repo, file, old, new, expected):
    edit(repo / file, old, new)
    problems = check(repo)
    assert any(p.startswith(expected) for p in problems), problems
