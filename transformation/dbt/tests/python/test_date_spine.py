"""dim_date and within_date_spine (E42). The example orders run 2026-01-01 to
2026-01-03. A spine ending 2026-01-03 leaves the last day's facts outside it,
because the end date is EXCLUSIVE; moving the end one day forward fixes it."""

import json

import duckdb
from dbt.cli.main import dbtRunner


def spine_vars(end):
    return json.dumps({"dbt_env": "prod", "date_spine_start": "2026-01-01", "date_spine_end": end})


def build_with_spine(project, end) -> dict:
    """Build dim_date + fct_orders and their tests; {test name: (status, failures)}."""
    assert project.dbt("build", "--select", "resource_type:seed", "--vars", spine_vars(end))
    result = dbtRunner().invoke(
        [
            "build", "--select", "dim_date", "stg_example", "fct_orders", "--store-failures",
            "--vars", spine_vars(end), "--project-dir", str(project.path),
        ]
    )
    return {r.node.name: (str(r.status), r.failures) for r in result.result.results}


def test_a_fact_date_past_the_exclusive_end_fails(project):
    results = build_with_spine(project, "2026-01-03")
    assert results["within_date_spine_fct_orders_created_at"] == ("fail", 1)
    with duckdb.connect(str(project.db_path)) as con:
        [(outside, rows)] = con.execute(
            "SELECT CAST(outside_spine AS VARCHAR), fact_rows "
            "FROM dbt_test__audit.within_date_spine_fct_orders_created_at"
        ).fetchall()
    assert (outside, rows) == ("2026-01-03", 1)
    with duckdb.connect(str(project.db_path)) as con:
        assert con.execute("SELECT MAX(date_day) FROM marts.dim_date").fetchone()[0].isoformat() == "2026-01-02"


def test_moving_the_end_forward_passes(project):
    results = build_with_spine(project, "2026-01-04")
    assert results["within_date_spine_fct_orders_created_at"] == ("pass", 0)


def test_join_form_against_a_dimension_agrees(project):
    yml = project.path / "models/marts/fct_orders.yml"
    yml.write_text(
        yml.read_text().replace(
            "          - within_date_spine\n",
            "          - within_date_spine:\n              spine: ref('dim_date')\n",
        )
    )
    def spine_test(results):
        [result] = [v for k, v in results.items() if k.startswith("within_date_spine_fct_orders_created_at")]
        return result

    assert spine_test(build_with_spine(project, "2026-01-03")) == ("fail", 1)
    assert spine_test(build_with_spine(project, "2026-01-04")) == ("pass", 0)
