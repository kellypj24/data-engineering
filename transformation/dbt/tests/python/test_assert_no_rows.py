"""assert_no_rows (E50) as a pre_hook: a guard that returns rows aborts the
build before the model is written; an empty guard lets it through."""

import duckdb

MODEL = """
{{{{ config(materialized='table', pre_hook="{{{{ assert_no_rows('{guard}', 'closed period would be rewritten') }}}}") }}}}
SELECT 1 AS id
"""


def build(project, guard):
    (project.path / "models" / "guarded.sql").write_text(MODEL.format(guard=guard))
    return project.dbt("build", "--select", "guarded")


def exists(project):
    with duckdb.connect(str(project.db_path)) as connection:
        tables = connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'guarded'"
        ).fetchone()[0]
    return tables == 1


def test_guard_with_rows_aborts_before_the_model_is_written(project):
    assert not build(project, "SELECT 1 AS violation")
    assert not exists(project)


def test_empty_guard_lets_the_build_through(project):
    assert build(project, "SELECT 1 AS violation WHERE FALSE")
    assert exists(project)
