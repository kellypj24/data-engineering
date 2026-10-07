"""Preservation manifest (E11) on duckdb: verify is read-only, restore brings
verify back to all-present, provision creates only what is missing."""

import json

import duckdb
from dbt.cli.main import dbtRunner

DATABASE = "warehouse"  # the duckdb catalog: the conftest file is warehouse.duckdb

MANIFEST = [
    {
        "name": "order_number_seq",
        "exists_sql": "SELECT 1 FROM duckdb_sequences() "
        "WHERE database_name = '{database}' AND sequence_name = 'order_number_seq'",
        "ddl": ["CREATE OR REPLACE SEQUENCE {database}.main.order_number_seq START 1000"],
    },
    {
        "name": "manual_overrides",
        "exists_sql": "SELECT 1 FROM duckdb_tables() "
        "WHERE database_name = '{database}' AND table_name = 'manual_overrides'",
        "ddl": [
            "CREATE OR REPLACE TABLE {database}.main.manual_overrides "
            "(order_id INTEGER, note VARCHAR)"
        ],
        "post_create": [
            "INSERT INTO {database}.main.manual_overrides VALUES (1, 'seeded')"
        ],
    },
]


def preserve(project, mode, dry_run=True) -> tuple[bool, list[str]]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [
            "run-operation",
            "preserve_objects",
            "--project-dir",
            str(project.path),
            "--args",
            json.dumps({"mode": mode, "database": DATABASE, "dry_run": dry_run}),
            "--vars",
            json.dumps({"preservation_manifest": MANIFEST}),
        ]
    )
    return result.success, messages


def statuses(messages):
    return sorted(m for m in messages if m.endswith((": present", ": missing")))


def query(project, sql):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(sql).fetchall()


def test_verify_is_read_only(project):
    ok, messages = preserve(project, "verify", dry_run=False)
    assert ok
    assert statuses(messages) == [
        "preserve_objects: manual_overrides: missing",
        "preserve_objects: order_number_seq: missing",
    ]
    assert query(project, "SELECT COUNT(*) FROM duckdb_tables()") == [(0,)]


def test_restore_dry_run_writes_nothing(project):
    ok, messages = preserve(project, "restore")
    assert ok
    assert any("DRY RUN restore manual_overrides" in m for m in messages)
    assert query(project, "SELECT COUNT(*) FROM duckdb_tables()") == [(0,)]


def test_restore_brings_verify_to_all_present(project):
    assert preserve(project, "restore", dry_run=False)[0]
    ok, messages = preserve(project, "verify")
    assert ok
    assert statuses(messages) == [
        "preserve_objects: manual_overrides: present",
        "preserve_objects: order_number_seq: present",
    ]
    assert query(project, "SELECT note FROM manual_overrides") == [("seeded",)]


def test_provision_creates_only_what_is_missing(project):
    assert preserve(project, "restore", dry_run=False)[0]
    query(project, "INSERT INTO manual_overrides VALUES (2, 'added later')")
    query(project, "DROP SEQUENCE order_number_seq")

    assert preserve(project, "provision", dry_run=False)[0]

    assert query(project, "SELECT COUNT(*) FROM duckdb_sequences()") == [(1,)]
    # Present, so untouched: the row added after restore survives.
    assert query(project, "SELECT COUNT(*) FROM manual_overrides") == [(2,)]


def test_rejects_an_unknown_mode(project):
    assert not preserve(project, "repair")[0]
