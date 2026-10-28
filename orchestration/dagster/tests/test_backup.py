"""Warehouse backup asset (E48): materialized against a local duckdb table
and a temp directory; the Snowflake extra by the SQL it sends."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import duckdb
import pytest
from dagster import materialize

from src.resources.backup import PARQUET_FILE, BackupStorage
from src.resources.snowflake_backup import SnowflakeBackupStorage
from src.utils.factories import build_backup_asset


@pytest.fixture
def warehouse(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            "CREATE TABLE raw.orders AS SELECT range AS order_id FROM range(3)"
        )
    return path


def backup(monkeypatch, warehouse, tmp_path, deployment=None):
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    monkeypatch.delenv("DEPLOYMENT", raising=False)
    if deployment:
        monkeypatch.setenv("DEPLOYMENT", deployment)
    storage = BackupStorage(root=str(tmp_path / "backups"), duckdb_path=str(warehouse))
    result = materialize(
        [build_backup_asset("raw.orders")], resources={"backup": storage}
    )
    assert result.success
    (materialization,) = result.asset_materializations_for_node("raw__orders_backup")
    return result.run_id, materialization.metadata


def test_parquet_lands_at_the_expected_path_with_matching_row_count(
    monkeypatch, warehouse, tmp_path
):
    run_id, metadata = backup(monkeypatch, warehouse, tmp_path, deployment="prod")
    path = metadata["path"].value
    assert path.startswith(f"{tmp_path}/backups/prod/raw.orders/snapshot_ts=")
    assert path.endswith(f"/run_id={run_id}")
    assert metadata["row_count"].value == 3
    assert metadata["source_relation"].value == "raw.orders"
    assert metadata["run_id"].value == run_id
    written = duckdb.sql(f"SELECT count(*) FROM '{path}/{PARQUET_FILE}'").fetchone()[0]
    assert written == 3


@pytest.mark.parametrize("deployment", [None, "Prod", "qa"])
def test_unset_or_unknown_deployment_writes_under_non_prod(
    monkeypatch, warehouse, tmp_path, deployment
):
    _, metadata = backup(monkeypatch, warehouse, tmp_path, deployment=deployment)
    assert metadata["path"].value.startswith(f"{tmp_path}/backups/non_prod/raw.orders/")


def test_snowflake_unloads_into_the_stage_and_sums_rows():
    cursor = MagicMock()
    cursor.execute.return_value.fetchall.return_value = [(2, 10, 8), (1, 5, 4)]
    connection = MagicMock()
    connection.cursor.return_value = cursor

    class FakeSnowflake:
        @contextmanager
        def get_connection(self):
            yield connection

    storage = SnowflakeBackupStorage.model_construct(
        root="@BACKUPS", snowflake=FakeSnowflake()
    )
    rows = storage.copy_to_parquet(
        "raw.orders", "@BACKUPS/prod/raw.orders/snapshot_ts=x/run_id=y"
    )
    assert rows == 3
    cursor.execute.assert_called_once_with(
        "COPY INTO @BACKUPS/prod/raw.orders/snapshot_ts=x/run_id=y/ FROM raw.orders "
        "FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE"
    )
