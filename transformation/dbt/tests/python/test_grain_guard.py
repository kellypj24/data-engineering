"""grain_guard (E41) on the example project: a duplicated key upstream builds
one row and WARNS instead of passing silently; a volatile tie-break is refused
at compile; a snapshot reading the guarded model leaves _grain_rows out via
star_except. Fixtures are edits to the throwaway project copy."""

import duckdb
from dbt.cli.main import dbtRunner

PROD = ["--vars", "{dbt_env: prod}"]

SNAPSHOT = """
{% snapshot orders_snapshot %}
{{ config(target_schema='snapshots', unique_key='order_id', strategy='check', check_cols=['status', 'amount']) }}
SELECT {{ star_except(ref('stg_example'), ['_grain_rows']) }}
FROM {{ ref('stg_example') }}
{% endsnapshot %}
"""


def sql(project, statement):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement).fetchall()


def build_stg_example(project):
    """Build stg_example and its tests; {test name: status}."""
    # `seed`, not `build`: the seed's own unique test would stop a duplicated id
    # here, and the point is what reaches stg_example when one gets through.
    assert project.dbt("seed", "--select", "orders", *PROD)
    result = dbtRunner().invoke(
        ["build", "--select", "stg_example", "--project-dir", str(project.path), *PROD]
    )
    assert result.success
    return {r.node.name: str(r.status) for r in result.result.results}


def test_clean_source_is_silent(project):
    statuses = build_stg_example(project)
    assert statuses["grain_guard_is_one_stg_example__grain_rows"] == "pass"


def test_duplicated_key_builds_one_row_and_warns(project):
    seed = project.path / "seeds/example_raw/orders.csv"
    seed.write_text(seed.read_text() + "1,101,ACTIVE,100.00,2025-12-31 09:00:00\n")
    statuses = build_stg_example(project)
    assert statuses["grain_guard_is_one_stg_example__grain_rows"] == "warn"
    assert statuses["unique_stg_example_order_id"] == "pass"  # what the dedup alone would show
    assert sql(project, "SELECT COUNT(*), MAX(_grain_rows) FROM staging.stg_example WHERE order_id = 1") == [(1, 2)]


def test_volatile_tie_break_is_refused(project):
    model = project.path / "models/staging/stg_example.sql"
    model.write_text(model.read_text().replace("order_by=['created_at DESC',", "order_by=['_loaded_at DESC',"))
    messages = []

    def collect(event):
        messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        ["compile", "--select", "stg_example", "--project-dir", str(project.path), *PROD]
    )
    assert not result.success
    assert any("`_loaded_at` is volatile" in str(m) for m in messages)


def test_snapshot_of_the_guarded_model_leaves_grain_rows_out(project):
    (project.path / "snapshots/orders_snapshot.sql").write_text(SNAPSHOT)
    build_stg_example(project)
    assert project.dbt("snapshot", "--select", "orders_snapshot", *PROD)
    columns = [row[0] for row in sql(project, "DESCRIBE snapshots.orders_snapshot")]
    assert "_grain_rows" not in columns
    assert "order_id" in columns and "dbt_valid_from" in columns
