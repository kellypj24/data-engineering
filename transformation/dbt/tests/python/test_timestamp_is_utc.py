"""Acceptance tests for the timestamp_is_utc generic test (E3)."""

from dbt.cli.main import dbtRunner

UTC = [
    "2026-01-01T09:00:00Z",
    "2026-01-01T09:00:00+00:00",
    "2026-01-01T09:00:00-00:00",
    "2026-01-01 09:00:00",  # no offset: nothing to contradict
]
NON_UTC = ["2026-01-01T09:00:00+02:00", "2026-01-01T04:00:00-05:00"]


def probe(project, name, select_sql):
    """Add a model with a timestamp_is_utc test and return that test's failures."""
    models = project.path / "models" / "staging"
    (models / f"{name}.sql").write_text(select_sql)
    (models / f"{name}.yml").write_text(
        f"""version: 2

models:
  - name: {name}
    description: timestamp_is_utc probe
    columns:
      - name: ts
        description: probe column
        tests:
          - timestamp_is_utc
"""
    )
    messages = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        ["build", "--select", name, "--project-dir", str(project.path)]
    )
    (test,) = [r for r in result.result.results if r.node.resource_type == "test"]
    return test.failures, messages


def strings(values):
    rows = ", ".join(f"('{v}')" for v in values)
    return f"SELECT ts FROM (VALUES {rows}) AS t (ts)"


def test_utc_and_offsetless_strings_pass(project):
    failures, _ = probe(project, "tz_utc", strings(UTC))
    assert failures == 0


def test_non_utc_offsets_fail_one_row_each(project):
    failures, _ = probe(project, "tz_mixed", strings(UTC + NON_UTC))
    assert failures == len(NON_UTC)


def test_offsetless_type_is_a_logged_no_op(project):
    failures, messages = probe(
        project,
        "tz_native",
        "SELECT CAST('2026-01-01 09:00:00+02:00' AS TIMESTAMPTZ) AS ts",
    )
    assert failures == 0
    assert any("nothing to check (no-op)" in m for m in messages)
