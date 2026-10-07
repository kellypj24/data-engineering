"""Semantic-view generator (E22). Snowflake extra: on duckdb the DDL is
generated and checked, never executed."""

import json

import pytest
from dbt.cli.main import dbtRunner

PROD = {"dbt_env": "prod"}
CURATED = {
    "semantic_views": {
        "revenue": {
            "metrics": ["fct_orders.total_amount AS SUM(fct_orders.amount)"],
            "relationships": ["daily AS fct_orders (created_at) REFERENCES fct_daily_order_revenue"],
            "synonyms": {"fct_orders": ["orders"], "fct_orders.amount": ["order value"]},
        }
    }
}


def generate(project, args, extra_vars=None) -> tuple[bool, str]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [
            "run-operation", "generate_semantic_view",
            "--project-dir", str(project.path),
            "--args", json.dumps(args),
            "--vars", json.dumps({**PROD, **(extra_vars or {})}),
        ]
    )
    return result.success, "\n".join(messages)


@pytest.fixture
def built(project):
    assert project.dbt("build", "--select", "resource_type:seed", "--vars", json.dumps(PROD))
    assert project.dbt("run", "--select", "+fct_orders", "+fct_daily_order_revenue",
                       "--exclude", "resource_type:seed", "--vars", json.dumps(PROD))
    return project


def test_generated_tables_facts_and_dimensions(built):
    ok, ddl = generate(built, {"domain": "revenue"})
    assert ok, ddl
    assert "CREATE OR REPLACE SEMANTIC VIEW" in ddl and "revenue_semantic_view" in ddl
    assert '"fct_orders" PRIMARY KEY (order_id)' in ddl
    assert '"fct_daily_order_revenue" PRIMARY KEY (order_date)' in ddl
    facts = ddl.split("FACTS (")[1].split("\n  )")[0]
    dimensions = ddl.split("DIMENSIONS (")[1].split("\n  )")[0]
    assert "fct_orders.amount AS amount" in facts
    assert "fct_orders.customer_id" in dimensions  # keys identify, they do not measure
    assert "COMMENT = 'Order amount'" in facts
    assert "METRICS" not in ddl and "RELATIONSHIPS" not in ddl  # never guessed


def test_sensitive_columns_are_never_exposed(built):
    yml = built.path / "models" / "marts" / "fct_orders.yml"
    yml.write_text(yml.read_text().replace(
        "      - name: customer_id\n",
        "      - name: customer_id\n        meta:\n          sensitive: true\n",
    ))
    ok, ddl = generate(built, {"domain": "revenue"})
    assert ok, ddl
    assert "customer_id" not in ddl


def test_curated_parts_come_from_the_var(built):
    ok, ddl = generate(built, {"domain": "revenue"}, CURATED)
    assert ok, ddl
    assert "METRICS (\n    fct_orders.total_amount AS SUM(fct_orders.amount)" in ddl
    assert "RELATIONSHIPS (\n    daily AS fct_orders (created_at) REFERENCES fct_daily_order_revenue" in ddl
    assert "PRIMARY KEY (order_id) WITH SYNONYMS ('orders')" in ddl
    assert "fct_orders.amount WITH SYNONYMS ('order value') AS amount" in ddl


def test_refusals(built, project):
    assert not generate(built, {"domain": "no_such_domain"})[0]
    assert not generate(built, {"domain": "revenue", "dry_run": False})[0]  # duckdb: never executes


def test_unbuilt_models_are_reported(project):
    assert not generate(project, {"domain": "revenue"})[0]
