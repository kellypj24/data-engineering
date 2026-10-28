"""SCD2 reconstruction from dated full extracts (E38), and the cutover to a
go-forward snapshot seeded from it.

The example is seeds/example_raw/customer_extracts.csv -> int_customers_history.
The go-forward snapshot and the calendar fixture are written into the throwaway
project copy only."""

import json

import duckdb
from dbt.cli.main import dbtRunner

PROD = ["--vars", "{dbt_env: prod}"]

SNAPSHOT = """
{% snapshot customers_history_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='check',
    check_cols=['content_hash'],
    hard_deletes='invalidate',
) }}
SELECT id, email, {{ row_content_hash(source('raw', 'customers')) }} AS content_hash
FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
"""
SNAPSHOT_YML = """version: 2
snapshots:
  - name: customers_history_snapshot
    description: Go-forward snapshot seeded from int_customers_history
    data_tests:
      - scd2_invariants:
          key: id
"""
SEED_ARGS = {
    "snapshot": "customers_history_snapshot",
    "history": "int_customers_history",
    "current": "raw.customers",
    "key": "id",
}


def sql(project, statement):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement).fetchall()


def build_history(project):
    assert project.dbt("build", "--select", "customer_extracts", "customers", *PROD)
    assert project.dbt("build", "--select", "int_customers_history", *PROD)


def versions(project, relation):
    return sql(
        project,
        f"SELECT id, email, CAST(dbt_valid_from AS DATE)::VARCHAR, CAST(dbt_valid_to AS DATE)::VARCHAR "
        f"FROM {relation} ORDER BY id, dbt_valid_from",
    )


def seed_snapshot(project, **args) -> tuple[bool, str]:
    messages = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [
            "run-operation", "seed_snapshot_from_history",
            "--args", json.dumps({**SEED_ARGS, **args}),
            "--project-dir", str(project.path), *PROD,
        ]
    )
    return result.success, "\n".join(messages)


def test_history_is_presence_aware(project):
    build_history(project)
    assert versions(project, "intermediate.int_customers_history") == [
        (1, "ada@old.example.com", "2026-01-01", "2026-01-03"),  # changed
        (1, "ada@example.com", "2026-01-03", None),
        (2, "grace@example.com", "2026-01-01", "2026-01-03"),  # missing on 01-03
        (2, "grace@example.com", "2026-01-04", None),  # back, unchanged: new version
        (3, "alan@example.com", "2026-01-01", "2026-01-03"),  # gone for good
    ]


def test_an_empty_extract_in_the_calendar_closes_every_version(project):
    assert project.dbt("build", "--select", "customer_extracts", *PROD)
    sql(
        project,
        "CREATE SCHEMA fx; CREATE TABLE fx.calendar AS "
        "SELECT DISTINCT extract_date FROM example_raw.customer_extracts "
        "UNION ALL SELECT DATE '2026-01-05'",
    )
    (project.path / "models/intermediate/fx_history.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "{{ reconstruct_scd2(source('raw', 'customer_extracts'), key='id', extract_date='extract_date', "
        "calendar=adapter.get_relation(database=target.database, schema='fx', identifier='calendar')) }}\n"
    )
    assert project.dbt("build", "--select", "fx_history", *PROD)
    open_versions = sql(project, "SELECT COUNT(*) FROM intermediate.fx_history WHERE dbt_valid_to IS NULL")
    closed_on_05 = sql(
        project, "SELECT COUNT(*) FROM intermediate.fx_history WHERE dbt_valid_to = TIMESTAMP '2026-01-05'"
    )
    assert open_versions == [(0,)] and closed_on_05 == [(2,)]


def test_cutover_continues_the_history(project):
    (project.path / "snapshots/customers_history_snapshot.sql").write_text(SNAPSHOT)
    (project.path / "snapshots/customers_history_snapshot.yml").write_text(SNAPSHOT_YML)
    build_history(project)

    ok, log = seed_snapshot(project)
    assert ok and "seam check passed" in log and "dry run -- nothing written" in log
    assert sql(project, "SELECT COUNT(*) FROM information_schema.tables "
               "WHERE table_name = 'customers_history_snapshot'") == [(0,)]

    ok, log = seed_snapshot(project, dry_run=False)
    assert ok and "seeded" in log and "5 version(s)" in log
    seeded = versions(project, "snapshots.customers_history_snapshot")

    # The first snapshot run matches at the seam: nothing re-versions.
    assert project.dbt("snapshot", "--select", "customers_history_snapshot", *PROD)
    assert versions(project, "snapshots.customers_history_snapshot") == seeded

    # From then on dbt continues it natively.
    sql(project, "UPDATE example_raw.customers SET email = 'grace@new.example.com' WHERE id = 2")
    assert project.dbt("snapshot", "--select", "customers_history_snapshot", *PROD)
    after = versions(project, "snapshots.customers_history_snapshot")
    assert after[:4] == seeded[:3] + [(2, "grace@example.com", "2026-01-04", after[3][3])]
    assert after[3][3] is not None  # the reappeared version is now closed
    assert after[4] == (2, "grace@new.example.com", after[3][3], None)
    assert after[5] == seeded[4]
    assert project.dbt("test", "--select", "customers_history_snapshot", *PROD)

    # A snapshot table is never replaced.
    ok, _ = seed_snapshot(project, dry_run=False)
    assert not ok


def test_seam_mismatch_is_refused_unless_allowed(project):
    (project.path / "snapshots/customers_history_snapshot.sql").write_text(SNAPSHOT)
    build_history(project)
    sql(project, "UPDATE example_raw.customers SET email = 'ada@changed.example.com' WHERE id = 1")

    ok, log = seed_snapshot(project)
    assert not ok
    assert "1 difference(s)" in log and "1 | content differs" in log

    ok, log = seed_snapshot(project, allow_mismatch=True)
    assert ok and "dry run" in log
