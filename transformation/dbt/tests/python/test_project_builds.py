"""The shipped example project must build end to end on duckdb from its seeded
fixtures (seeds/example_raw/): every model, data test, and unit test. Guards
against SQL that lints clean but cannot run, such as a trailing semicolon
inside dbt's `create ... as (...)` wrapper."""

PROD = ["--vars", "{dbt_env: prod}"]


def test_example_project_builds_from_its_fixtures(project):
    # Seed first: models read the fixtures via source(), which dbt does not
    # order after seeds.
    assert project.dbt("seed", *PROD)
    assert project.dbt("build", *PROD)
