"""row_content_hash, relation form (E40): columns come from the warehouse, minus
`exclude`; array columns are found from their types and sorted unless listed
in keep_array_order; adding a column changes every hash once.

The relation is a table created directly in the throwaway duckdb file, and the
model reading it is written into the throwaway project copy, so nothing here
adds a fixture to the real project."""

import duckdb

PROD = ["--vars", "{dbt_env: prod}"]

MODEL = """
{%- set items = adapter.get_relation(database=target.database, schema='fx', identifier='items') -%}
SELECT
    id,
    {{ row_content_hash(items, exclude=['_LOADED_AT']) }} AS h,
    {{ row_content_hash(items, exclude=['_loaded_at'], keep_array_order=['ranking']) }} AS h_ranked
FROM {{ items }}
"""


def sql(project, statement):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement).fetchall()


def hashes(project):
    assert project.dbt("run", "--select", "fx_hash", *PROD)
    rows = sql(project, "SELECT id, h, h_ranked FROM staging.fx_hash ORDER BY id")
    return {row[0]: (row[1], row[2]) for row in rows}


def setup(project):
    sql(
        project,
        """
        CREATE SCHEMA fx;
        CREATE TABLE fx.items (
            id INTEGER, "Name" VARCHAR, tags VARCHAR[], ranking INTEGER[], _loaded_at TIMESTAMP
        );
        INSERT INTO fx.items VALUES
            (1, 'a', ['x', 'y'], [1, 2], TIMESTAMP '2026-01-01'),
            (2, 'b', ['z'], [3], TIMESTAMP '2026-01-01');
        """,
    )
    (project.path / "models/staging/fx_hash.sql").write_text(MODEL)


def test_excluded_column_and_array_order_are_not_changes(project):
    setup(project)
    before = hashes(project)
    sql(
        project,
        "UPDATE fx.items SET _loaded_at = TIMESTAMP '2026-02-01', tags = ['y', 'x'] WHERE id = 1",
    )
    assert hashes(project) == before


def test_content_change_and_kept_array_order_are_changes(project):
    setup(project)
    before = hashes(project)
    sql(project, "UPDATE fx.items SET ranking = [2, 1] WHERE id = 1")
    after = hashes(project)
    assert after[1][0] == before[1][0]  # ranking sorted: same content
    assert after[1][1] != before[1][1]  # ranking order kept: a change
    assert after[2] == before[2]

    sql(project, "UPDATE fx.items SET \"Name\" = 'c' WHERE id = 2")
    assert hashes(project)[2][0] != before[2][0]


def test_adding_a_column_changes_every_hash_once(project):
    setup(project)
    before = hashes(project)
    sql(project, "ALTER TABLE fx.items ADD COLUMN note VARCHAR")
    after = hashes(project)
    assert all(after[i][0] != before[i][0] for i in before)
    assert hashes(project) == after  # stable from then on


PROBE = """
{% macro probe_snowflake_content_hash() %}
    {%- for t in ['ARRAY', 'ARRAY(NUMBER)', 'VARIANT', 'OBJECT', 'VARCHAR(16777216)', 'NUMBER(38,0)'] -%}
        {%- do log('kind ' ~ t ~ ' = ' ~ snowflake___content_hash_kind(t), info=true) -%}
    {%- endfor -%}
    {%- do log('value array sorted = ' ~ snowflake___content_hash_value('tags', 'array', true), info=true) -%}
    {%- do log('value array kept = ' ~ snowflake___content_hash_value('tags', 'array', false), info=true) -%}
    {%- do log('value semi = ' ~ snowflake___content_hash_value('attrs', 'semi', false), info=true) -%}
    {%- do log('value scalar = ' ~ snowflake___content_hash_value('email', 'scalar', false), info=true) -%}
{% endmacro %}
"""


def test_snowflake_extra_renders_canonical_serialisations(project):
    """Snowflake extra: rendered on duckdb and checked, never executed."""
    from dbt.cli.main import dbtRunner

    (project.path / "macros/_probe.sql").write_text(PROBE)
    messages = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        ["run-operation", "probe_snowflake_content_hash", "--project-dir", str(project.path)]
    )
    assert result.success
    assert messages == [
        "kind ARRAY = array",
        "kind ARRAY(NUMBER) = array",
        "kind VARIANT = semi",
        "kind OBJECT = semi",
        "kind VARCHAR(16777216) = scalar",
        "kind NUMBER(38,0) = scalar",
        "value array sorted = TO_JSON(ARRAY_SORT(tags))",
        "value array kept = TO_JSON(tags)",
        "value semi = TO_JSON(attrs)",
        "value scalar = CAST(email AS VARCHAR)",
    ]
