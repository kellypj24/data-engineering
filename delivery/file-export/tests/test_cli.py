"""The CLI validates the committed configs and runs a dry run."""

import duckdb

from file_export.cli import main
from tests.conftest import CONFIGS_DIR


def test_validate_committed_configs(capsys):
    assert main(["validate", str(CONFIGS_DIR)]) == 0
    assert "export config(s) valid" in capsys.readouterr().out


def test_dry_run_prints_sql(tmp_path, capsys):
    db = tmp_path / "w.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("CREATE SCHEMA marts")
        con.execute(
            "CREATE TABLE marts.fct_orders (order_id INT, created_at TIMESTAMP)"
        )
    code = main(
        [
            "run",
            str(CONFIGS_DIR / "order_audit_adhoc.yml"),
            "--recipient",
            "finance",
            "--mode",
            "dry-run",
            "--duckdb",
            str(db),
            "--output-root",
            str(tmp_path / "out"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "FROM marts.fct_orders" in out
    assert not (tmp_path / "out").exists()
