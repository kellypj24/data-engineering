"""Cost attribution (E27) on the duckdb fixtures standing in for
SNOWFLAKE.ACCOUNT_USAGE / ORGANIZATION_USAGE. Expected amounts are computed by
hand from the fixture CSVs: compute is $3/credit both days."""

import duckdb
import pytest

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
