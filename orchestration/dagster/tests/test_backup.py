"""Warehouse backup asset (E48): materialized against a local duckdb table
and a temp directory; the Snowflake extra by the SQL it sends."""

import socket
from contextlib import contextmanager
from unittest.mock import MagicMock

import boto3
import duckdb
import pytest
from dagster import AssetKey, materialize
from moto.server import ThreadedMotoServer

from src import defs
from src.resources.backup import PARQUET_FILE, BackupStorage, s3_setup_sql
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
    ((written,),) = duckdb.sql(
        f"SELECT count(*) FROM '{path}/{PARQUET_FILE}'"
    ).fetchall()
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


@pytest.fixture
def s3_endpoint():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    ).create_bucket(Bucket="backups")
    yield f"127.0.0.1:{port}"
    server.stop()


def test_s3_root_writes_through_httpfs(warehouse, s3_endpoint):
    secret = {
        "KEY_ID": "testing",
        "SECRET": "testing",
        "REGION": "us-east-1",
        "ENDPOINT": s3_endpoint,
        "URL_STYLE": "path",
        "USE_SSL": "false",
    }
    storage = BackupStorage(
        root="s3://backups", duckdb_path=str(warehouse), s3_secret=secret
    )
    directory = "s3://backups/non_prod/raw.orders/snapshot_ts=x/run_id=y"
    assert storage.copy_to_parquet("raw.orders", directory) == 3

    with duckdb.connect() as connection:
        for statement in s3_setup_sql(secret):
            connection.execute(statement)
        count = connection.execute(f"SELECT count(*) FROM '{directory}/{PARQUET_FILE}'")
        assert count.fetchone()[0] == 3


def test_credential_chain_is_the_default_secret():
    assert s3_setup_sql(BackupStorage().s3_secret) == [
        "INSTALL httpfs",
        "LOAD httpfs",
        "INSTALL aws",
        "LOAD aws",
        "CREATE OR REPLACE TEMPORARY SECRET backup_s3 (TYPE s3, PROVIDER 'credential_chain')",
    ]


def test_example_backup_runs_after_its_sync():
    assets = defs.get_repository_def().asset_graph
    key = AssetKey("raw_orders_backup")
    assert AssetKey(["airbyte", "raw_orders"]) in assets.get(key).parent_keys
