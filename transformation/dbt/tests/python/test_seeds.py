"""Acceptance tests for the project-wide seed contract (E28).

Each test runs dbt in-process on a copy of the project against a throwaway
duckdb file, so the real seeds are never modified. Requires `dbt deps`.
"""

import json
import shutil
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

PROJECT_DIR = Path(__file__).resolve().parents[2]
SEED = "order_status_codes"


@pytest.fixture
def project(tmp_path, monkeypatch):
    copy = tmp_path / "project"
    shutil.copytree(
        PROJECT_DIR,
        copy,
        ignore=shutil.ignore_patterns(
            ".venv", "target", "logs", "dbt_packages", "*.duckdb", "tests"
        ),
    )
    (copy / "dbt_packages").symlink_to(PROJECT_DIR / "dbt_packages")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(copy))
    monkeypatch.setenv("DBT_TARGET", "duckdb")
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "seeds.duckdb"))
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    return copy


def dbt(project: Path, *args: str) -> bool:
    return dbtRunner().invoke([*args, "--project-dir", str(project)]).success


def add_column(project: Path) -> None:
    csv = project / "seeds" / f"{SEED}.csv"
    header, *rows = csv.read_text().splitlines()
    csv.write_text(
        "\n".join([f"{header},sort_order", *(f"{r},{i}" for i, r in enumerate(rows))])
        + "\n"
    )


def seed_columns(db_path: str) -> set[str]:
    with duckdb.connect(db_path) as con:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [SEED],
        ).fetchall()
    return {name for (name,) in rows}


def test_new_csv_column_loads_without_full_refresh(project, tmp_path):
    assert dbt(project, "seed", "--select", SEED)
    add_column(project)
    assert dbt(project, "seed", "--select", SEED)
    assert "sort_order" in seed_columns(str(tmp_path / "seeds.duckdb"))


def test_control_without_full_refresh_setting_fails(project):
    """Proves the test above exercises the failure +full_refresh prevents."""
    config = project / "dbt_project.yml"
    config.write_text(config.read_text().replace("+full_refresh: true", ""))
    assert dbt(project, "seed", "--select", SEED)
    add_column(project)
    assert not dbt(project, "seed", "--select", SEED)


def test_every_seed_has_description_owner_and_a_test(project, tmp_path):
    assert dbt(project, "parse")
    manifest = json.loads((tmp_path / "target" / "manifest.json").read_text())
    nodes = manifest["nodes"].values()
    tested = {
        dep
        for n in nodes
        if n["resource_type"] == "test"
        for dep in n["depends_on"]["nodes"]
    }

    seeds = [n for n in nodes if n["resource_type"] == "seed"]
    assert seeds, "expected at least one seed"
    problems = []
    for seed in seeds:
        if not seed["description"].strip():
            problems.append(f"{seed['name']}: no description")
        if not seed["config"].get("meta", {}).get("owner"):
            problems.append(f"{seed['name']}: no meta.owner")
        if seed["unique_id"] not in tested:
            problems.append(f"{seed['name']}: no tests")
    assert not problems, "\n".join(problems)
