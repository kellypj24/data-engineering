"""History stamped with `current_date` (E51): no_frozen_days flags a date that
equals the day before across every key, and restate_history_date repairs it
from a neighbouring day (dry run by default)."""

import json

import duckdb
from dbt.cli.main import dbtRunner

# 10-02 is frozen: identical to 10-01 although 10-03 shows the change landed.
HISTORY = """snapshot_date,customer_id,status
2026-10-01,1,active
2026-10-01,2,active
2026-10-02,1,active
2026-10-02,2,active
2026-10-03,1,active
2026-10-03,2,churned
"""
SEED_YML = """version: 2
seeds:
  - name: fx_history
    config:
      column_types: {snapshot_date: date, customer_id: integer, status: varchar}
    data_tests:
      - no_frozen_days:
          date_column: snapshot_date
          key: customer_id
          value_columns: [status]
"""


def seed(project):
    folder = project.path / "seeds" / "fx_history"
    folder.mkdir()
    (folder / "fx_history.csv").write_text(HISTORY)
    (folder / "_fx_history.yml").write_text(SEED_YML)
    assert project.dbt("seed", "--select", "fx_history")


def frozen_days(project) -> list[str]:
    result = dbtRunner().invoke(
        ["test", "--select", "fx_history", "--store-failures", "--project-dir", str(project.path)]
    )
    (test,) = [r for r in result.result.results if r.node.resource_type == "test"]
    if not test.failures:
        return []
    with duckdb.connect(str(project.db_path)) as connection:
        rows = connection.execute(f"SELECT frozen_day FROM {test.node.relation_name}").fetchall()
    return [str(r[0]) for r in rows]


def restate(project, dry_run):
    messages = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    args = {
        "relation": "main.fx_history",
        "date_column": "snapshot_date",
        "bad_date": "2026-10-02",
        "from_date": "2026-10-03",
        "dry_run": dry_run,
    }
    result = dbtRunner(callbacks=[collect]).invoke(
        ["run-operation", "restate_history_date", "--project-dir", str(project.path), "--args", json.dumps(args)]
    )
    assert result.success
    return messages


def rows_on(project, day):
    with duckdb.connect(str(project.db_path)) as connection:
        return connection.execute(
            "SELECT customer_id, status FROM main.fx_history WHERE snapshot_date = ? ORDER BY 1",
            [day],
        ).fetchall()


def test_frozen_day_is_flagged(project):
    seed(project)
    assert frozen_days(project) == ["2026-10-02"]


def test_restate_dry_run_writes_nothing_then_repairs(project):
    seed(project)
    messages = restate(project, dry_run=True)
    assert any(m.startswith("DRY RUN restate_history_date: DELETE") for m in messages)
    assert rows_on(project, "2026-10-02") == [(1, "active"), (2, "active")]

    restate(project, dry_run=False)
    assert rows_on(project, "2026-10-02") == [(1, "active"), (2, "churned")]
    assert rows_on(project, "2026-10-03") == [(1, "active"), (2, "churned")]
    # The repaired day now equals its successor, not its predecessor.
    assert frozen_days(project) == ["2026-10-03"]
