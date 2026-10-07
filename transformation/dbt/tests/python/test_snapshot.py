"""The example snapshot (E32): a changed column opens a new version, a hard
delete closes the current row, and the point-in-time predicate returns
exactly one version per customer that existed at t."""

import duckdb

PROD = ["--vars", "{dbt_env: prod}"]


def sql(project, statement):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement).fetchall()


def versions(project):
    return sql(
        project,
        "SELECT id, email, dbt_valid_to IS NULL AS is_current "
        "FROM snapshots.customers_snapshot ORDER BY id, dbt_valid_from",
    )


def as_of(project, t):
    return sql(
        project,
        "SELECT id, email FROM snapshots.customers_snapshot "
        f"WHERE dbt_valid_from <= TIMESTAMP '{t}' "
        f"AND (TIMESTAMP '{t}' < dbt_valid_to OR dbt_valid_to IS NULL) ORDER BY id",
    )


def test_change_and_hard_delete(project):
    assert project.dbt("build", "--select", "resource_type:seed", *PROD)
    assert project.dbt("snapshot", *PROD)
    first_run = sql(project, "SELECT MAX(dbt_valid_from) FROM snapshots.customers_snapshot")[0][0]

    sql(project, "UPDATE example_raw.customers SET email = 'ada@new.example.com' WHERE id = 1")
    sql(project, "DELETE FROM example_raw.customers WHERE id = 2")
    assert project.dbt("snapshot", *PROD)

    assert versions(project) == [
        (1, "ada@example.com", False),
        (1, "ada@new.example.com", True),
        (2, "grace@example.com", False),  # deleted: closed, not removed
    ]
    # Point in time: as of the first run, both customers with original emails.
    assert as_of(project, first_run) == [(1, "ada@example.com"), (2, "grace@example.com")]
    # Now: one current version, for the customer that still exists.
    now = sql(project, "SELECT CAST(MAX(dbt_valid_from) AS VARCHAR) FROM snapshots.customers_snapshot")[0][0]
    assert as_of(project, now) == [(1, "ada@new.example.com")]


def test_unchanged_rerun_adds_no_versions(project):
    assert project.dbt("build", "--select", "resource_type:seed", *PROD)
    assert project.dbt("snapshot", *PROD)
    assert project.dbt("snapshot", *PROD)
    assert len(versions(project)) == 2
