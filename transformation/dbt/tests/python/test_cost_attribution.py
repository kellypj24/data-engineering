"""Cost attribution (E27) on the duckdb fixtures standing in for
SNOWFLAKE.ACCOUNT_USAGE / ORGANIZATION_USAGE. Expected amounts are computed by
hand from the fixture CSVs: compute is $3/credit both days."""

import duckdb
import pytest
from dbt.cli.main import dbtRunner

PROD = ["--vars", "{dbt_env: prod}"]
INVOICE_TOTAL = 24.74


def build(project):
    assert project.dbt("build", "--select", "resource_type:seed", *PROD)
    assert project.dbt("build", "--select", "+models/cost", "--exclude", "resource_type:seed", *PROD)


def by_team(project):
    with duckdb.connect(str(project.db_path)) as con:
        rows = con.execute(
            "SELECT team, workload, ROUND(SUM(cost), 2) FROM cost.fct_monthly_cost "
            "GROUP BY ALL ORDER BY ALL"
        ).fetchall()
    return {(team, workload): cost for team, workload, cost in rows}


def total(project):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute("SELECT ROUND(SUM(cost), 2) FROM cost.fct_monthly_cost").fetchone()[0]


def test_attribution_by_query_not_warehouse(project):
    build(project)
    costs = by_team(project)
    # REPORTING is shared; its queries belong to whoever ran them.
    assert costs[("finance", "reporting")] == pytest.approx(4.50)  # BI_SERVICE, resolved by database
    assert costs[("NEEDS_OWNER_REVIEW", "NEEDS_OWNER_REVIEW")] == pytest.approx(1.50)  # unmapped user
    assert costs[("data_platform", "transformation")] == pytest.approx(6.00)
    assert costs[("analytics", "ad_hoc")] == pytest.approx(3.00)
    # Idle and cloud services are SHARED, not spread across teams.
    assert costs[("SHARED", "IDLE")] == pytest.approx(6.00)
    assert costs[("SHARED", "CLOUD_SERVICES")] == pytest.approx(1.65)
    assert total(project) == pytest.approx(INVOICE_TOTAL)


def test_unmapped_database_storage_is_reviewed_not_guessed(project):
    build(project)
    assert by_team(project)[("NEEDS_OWNER_REVIEW", "STORAGE")] == pytest.approx(0.33)


def test_removing_a_mapping_moves_spend_to_review(project):
    mapping = project.path / "seeds" / "cost" / "cost_user_mapping.csv"
    mapping.write_text(
        "\n".join(line for line in mapping.read_text().splitlines() if not line.startswith("JDOE"))
        + "\n"
    )
    build(project)
    costs = by_team(project)
    assert ("analytics", "ad_hoc") not in costs
    assert costs[("NEEDS_OWNER_REVIEW", "NEEDS_OWNER_REVIEW")] == pytest.approx(4.50)
    assert total(project) == pytest.approx(INVOICE_TOTAL)


def test_rerun_and_full_refresh_keep_the_durable_fact(project):
    build(project)
    before = by_team(project)
    build(project)
    assert project.dbt("build", "--select", "fct_monthly_cost", "--full-refresh", *PROD)
    assert by_team(project) == before


AUCKLAND = "Pacific/Auckland"  # UTC+13 in January: 23:00 UTC on 31 Jan is 1 Feb locally
KIRITIMATI = "Pacific/Kiritimati"  # UTC+14: the fixtures' 10:00 UTC hours on 6 Jan are 7 Jan locally


def in_session_time_zone(project, tz):
    profiles = project.path / "profiles.yml"
    text = profiles.read_text()
    anchor = "      path: \"{{ env_var('DUCKDB_PATH', 'dev.duckdb') }}\"\n"
    assert anchor in text
    profiles.write_text(text.replace(anchor, anchor + f"      settings:\n        TimeZone: {tz}\n"))


def test_utc_date_trunc_ignores_the_session_time_zone(project):
    in_session_time_zone(project, AUCKLAND)
    hour = "timestamptz '2026-01-31 23:00:00+00'"
    sql = (
        f"SELECT {{{{ utc_date_trunc('month', \"{hour}\") }}}} AS month, "
        f"{{{{ utc_date_trunc('day', \"{hour}\") }}}} AS day, "
        f"{{{{ utc_date_trunc('hour', \"{hour}\") }}}} AS hour, "
        "current_setting('TimeZone') AS tz"
    )
    result = dbtRunner().invoke(["show", "--inline", sql, "--project-dir", str(project.path)])
    assert result.success, result.exception
    row = result.result.results[0].agate_table.rows[0]
    assert row["tz"] == AUCKLAND
    assert str(row["month"]).startswith("2026-01-01")
    assert str(row["day"]).startswith("2026-01-31")
    assert str(row["hour"]).startswith("2026-01-31 23:00")


def test_invoice_tie_holds_in_a_non_utc_session(project):
    in_session_time_zone(project, KIRITIMATI)
    build(project)
    costs = by_team(project)
    # A local-day cast moves 6 Jan's hours to 7 Jan, leaving 6 Jan's invoice to NEEDS_OWNER_REVIEW.
    assert costs[("NEEDS_OWNER_REVIEW", "NEEDS_OWNER_REVIEW")] == pytest.approx(1.50)
    assert costs[("SHARED", "IDLE")] == pytest.approx(6.00)
    assert total(project) == pytest.approx(INVOICE_TOTAL)


def test_snowflake_compiles_the_utc_conversion(project, monkeypatch):
    for name, value in {
        "DBT_TARGET": "snowflake",
        "SNOWFLAKE_ACCOUNT": "compile-only",
        "SNOWFLAKE_USER": "compile_only",
        "SNOWFLAKE_AUTHENTICATOR": "snowflake",  # never externalbrowser in a test
    }.items():
        monkeypatch.setenv(name, value)
    assert project.dbt(
        "compile", "--select", "stg_snowflake__warehouse_metering",
        "stg_snowflake__query_attribution", "--no-introspect", "--no-populate-cache", *PROD,
    )
    compiled = list((project.target_path / "compiled").rglob("stg_snowflake__*.sql"))
    assert len(compiled) == 2
    for path in compiled:
        assert "CONVERT_TIMEZONE('UTC'," in path.read_text(), path.name
