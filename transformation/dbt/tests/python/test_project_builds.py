"""The shipped example project must build end to end on duckdb from its seeded
fixtures (seeds/example_raw/): every model, data test, and unit test. Guards
against SQL that lints clean but cannot run, such as a trailing semicolon
inside dbt's `create ... as (...)` wrapper."""

PROD = ["--vars", "{dbt_env: prod}"]


def test_example_project_builds_from_its_fixtures(project):
    # Seeds first, then everything else without them: models and source tests
    # read the fixtures via source(), which dbt does not order after seeds, and
    # a seed rebuilt in the same run is dropped and recreated while they read it.
    assert project.dbt("build", "--select", "resource_type:seed", *PROD)
    assert project.dbt("build", "--exclude", "resource_type:seed", *PROD)
