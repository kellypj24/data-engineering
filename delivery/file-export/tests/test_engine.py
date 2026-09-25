"""Engine acceptance tests on duckdb: modes, watermark chain, fail-closed
tenant isolation, and all-or-nothing delivery."""

import itertools

import pytest

from file_export.engine import ExportFailed, Mode
from tests.conftest import add_lines, shared_config


def files(root):
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    )


def test_dry_run_writes_nothing(engine, warehouse, write_config, tmp_path):
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"))
    config = write_config(shared_config())
    result = engine.run(config, "north", Mode.DRY_RUN)

    assert "FROM marts.order_lines" in result.sql
    assert not (tmp_path / "out").exists()
    assert engine.run_log.exists() is False


def test_select_only_counts_and_logs_nothing(engine, warehouse, write_config):
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"), (2, 201, "2026-03-01 11:00"))
    result = engine.run(write_config(shared_config()), "north", Mode.SELECT_ONLY)
    assert result.row_count == 1
    assert engine.run_log.exists() is False


def test_watermark_chain_with_a_zero_row_run_is_contiguous(
    engine, warehouse, write_config, clock
):
    config = write_config(shared_config())
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"), (2, 101, "2026-03-02 10:00"))

    first = engine.run(config, "north", Mode.EXECUTE)  # no watermark: all history
    assert (first.window.start, first.row_count) == (None, 2)

    clock.advance(days=1)
    idle = engine.run(config, "north", Mode.EXECUTE)  # nothing new
    assert idle.row_count == 0
    assert idle.status == "success"

    clock.advance(days=1)
    add_lines(warehouse, (3, 101, "2026-03-03 10:00"))
    third = engine.run(config, "north", Mode.EXECUTE)
    assert third.row_count == 1

    windows = [(r.window.start, r.window.end) for r in (first, idle, third)]
    assert windows == [
        (None, "2026-03-02 10:00:00"),
        ("2026-03-02 10:00:00", "2026-03-02 10:00:00"),
        ("2026-03-02 10:00:00", "2026-03-03 10:00:00"),
    ]
    # Contiguous: each run starts exactly where the previous successful one ended.
    for previous, current in itertools.pairwise(windows):
        assert current[0] == previous[1]


def test_row_at_the_watermark_is_not_sent_twice(engine, warehouse, write_config, clock):
    config = write_config(shared_config())
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"))
    engine.run(config, "north", Mode.EXECUTE)
    clock.advance(days=1)
    assert engine.run(config, "north", Mode.EXECUTE).row_count == 0


def test_foreign_tenant_row_fails_closed(engine, warehouse, write_config, tmp_path):
    # North is selected by region; one north-region row belongs to tenant 202.
    config = write_config(
        shared_config(
            recipients=[
                {
                    "name": "north",
                    "tenant_keys": [101],
                    "filters": [{"column": "region", "in": ["north"]}],
                }
            ],
            outputs=[
                {"path": "{recipient}/a.csv"},
                {"path": "{recipient}/b.parquet", "format": "parquet"},
            ],
        )
    )
    add_lines(
        warehouse, (1, 101, "2026-03-01 10:00"), (2, 202, "2026-03-01 11:00", "north")
    )

    with pytest.raises(ExportFailed, match="TenantIsolationError"):
        engine.run(config, "north", Mode.EXECUTE)

    assert files(tmp_path / "out") == [] if (tmp_path / "out").exists() else True
    (status, *_rest, error) = engine.run_log.entries("lines", "north")[-1]
    assert status == "failure"
    assert "outside tenant keys" in error


def test_null_tenant_fails_closed(engine, warehouse, write_config):
    config = write_config(
        shared_config(
            recipients=[
                {
                    "name": "north",
                    "tenant_keys": [101],
                    "filters": [{"column": "region", "in": ["north"]}],
                }
            ]
        )
    )
    add_lines(warehouse, (1, None, "2026-03-01 10:00"))
    with pytest.raises(ExportFailed, match="TenantIsolationError"):
        engine.run(config, "north", Mode.EXECUTE)


def test_failed_run_does_not_advance_the_watermark(
    engine, warehouse, write_config, clock
):
    config = write_config(
        shared_config(
            recipients=[
                {
                    "name": "north",
                    "tenant_keys": [101],
                    "filters": [{"column": "region", "in": ["north"]}],
                }
            ]
        )
    )
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"))
    engine.run(config, "north", Mode.EXECUTE)
    clock.advance(days=1)
    add_lines(warehouse, (2, 202, "2026-03-02 10:00"))
    with pytest.raises(ExportFailed):
        engine.run(config, "north", Mode.EXECUTE)
    assert engine.run_log.last_successful_end("lines", "north") == "2026-03-01 10:00:00"


def test_one_recipient_failing_does_not_stop_the_others(
    engine, warehouse, write_config
):
    config = write_config(
        shared_config(
            recipients=[
                {
                    "name": "north",
                    "tenant_keys": [101],
                    "filters": [{"column": "region", "in": ["north"]}],
                },
                {"name": "south", "tenant_keys": [201]},
            ]
        )
    )
    add_lines(
        warehouse,
        (1, 999, "2026-03-01 10:00", "north"),
        (2, 201, "2026-03-01 10:00", "south"),
    )
    results = {r.recipient: r.status for r in engine.run_all(config, Mode.EXECUTE)}
    assert results == {"north": "failure", "south": "success"}


def test_multiple_outputs_records_and_control_file(
    engine, warehouse, write_config, tmp_path
):
    config = write_config(
        shared_config(
            outputs=[
                {
                    "path": "{recipient}/lines_{run_date}.csv",
                    "header_record": "H|{recipient}|{run_date}",
                    "trailer_record": "T|{row_count}",
                    "control_file": True,
                },
                {"path": "{recipient}/lines_{run_date}.parquet", "format": "parquet"},
            ]
        )
    )
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"), (2, 101, "2026-03-01 11:00"))
    engine.run(config, "north", Mode.EXECUTE)

    out = tmp_path / "out"
    assert files(out) == [
        "north/lines_2026-03-04.csv",
        "north/lines_2026-03-04.csv.ctl",
        "north/lines_2026-03-04.parquet",
    ]
    lines = (out / "north/lines_2026-03-04.csv").read_text().splitlines()
    assert lines[0] == "H|north|2026-03-04"
    assert lines[1] == "order_id,customer_id"
    assert lines[-1] == "T|2"
    control = (out / "north/lines_2026-03-04.csv.ctl").read_text()
    assert "row_count=2" in control
    assert "sha256=" in control
    assert warehouse.execute(
        f"SELECT COUNT(*) FROM '{out}/north/lines_2026-03-04.parquet'"
    ).fetchone() == (2,)


def test_a_failing_output_leaves_no_partial_delivery(
    engine, warehouse, write_config, tmp_path
):
    config = write_config(
        shared_config(
            outputs=[{"path": "{recipient}/ok.csv"}, {"path": "../escape.csv"}]
        )
    )
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"))
    with pytest.raises(ExportFailed, match="escapes the output root"):
        engine.run(config, "north", Mode.EXECUTE)
    assert files(tmp_path / "out") == []
