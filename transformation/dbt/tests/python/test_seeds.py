"""Acceptance tests for the project-wide seed contract (E28).

Runs on a throwaway copy of the project (see conftest.py), so the real seeds
are never modified.
"""

import json
from pathlib import Path

import duckdb

SEED = "order_status_codes"


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


def test_new_csv_column_loads_without_full_refresh(project):
    assert project.dbt("seed", "--select", SEED)
    add_column(project.path)
    assert project.dbt("seed", "--select", SEED)
    assert "sort_order" in seed_columns(str(project.db_path))


def test_control_without_full_refresh_setting_fails(project):
    """Proves the test above exercises the failure +full_refresh prevents."""
    config = project.path / "dbt_project.yml"
    config.write_text(config.read_text().replace("+full_refresh: true", ""))
    assert project.dbt("seed", "--select", SEED)
    add_column(project.path)
    assert not project.dbt("seed", "--select", SEED)


def test_every_seed_has_description_owner_and_a_test(project):
    assert project.dbt("parse")
    manifest = json.loads((project.target_path / "manifest.json").read_text())
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
