"""The shipped example project must build end to end on duckdb: every model,
data test, and unit test. Guards against SQL that lints clean but cannot run,
such as a trailing semicolon inside dbt's `create ... as (...)` wrapper."""


def test_example_project_builds(project):
    import duckdb

    with duckdb.connect(str(project.db_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute(
            "CREATE TABLE raw.orders AS SELECT * FROM (VALUES "
            "(1, ' ACTIVE ', 100.0, TIMESTAMP '2026-01-01 09:00')) "
            "AS t (id, status, amount, created_at)"
        )
        con.execute(
            "CREATE TABLE raw.customers AS SELECT * FROM (VALUES "
            "(1, 'someone@example.com')) AS t (id, email)"
        )
    assert project.dbt("build", "--vars", "{dbt_env: prod}")
