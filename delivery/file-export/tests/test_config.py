"""The config contract: every committed config validates (a merge gate), and
unsafe or contradictory configs fail at parse time, before any SQL runs."""

import warnings

import pytest
import yaml

from file_export.config import ConfigError, discover_configs, load_config, load_configs
from tests.conftest import CONFIGS_DIR, shared_config


@pytest.mark.parametrize("path", discover_configs(CONFIGS_DIR), ids=lambda p: p.name)
def test_every_committed_config_validates(path):
    load_config(path)


def test_committed_configs_are_discovered_and_unique():
    assert len(discover_configs(CONFIGS_DIR)) >= 3
    load_configs(CONFIGS_DIR)


def write(tmp_path, data):
    path = tmp_path / "c.yml"
    path.write_text(yaml.safe_dump(data))
    return path


def rejects(tmp_path, data, message):
    with pytest.raises(ConfigError, match=message):
        load_config(write(tmp_path, data))


def test_shared_without_tenant_column_raises(tmp_path):
    data = shared_config()
    del data["tenant_column"]
    rejects(tmp_path, data, "tenant_column")


def test_shared_recipient_without_tenant_keys_raises(tmp_path):
    data = shared_config(recipients=[{"name": "north"}])
    rejects(tmp_path, data, "tenant_keys")


def test_dedicated_rejects_filters_and_needs_per_recipient_source(tmp_path):
    base = {
        "name": "d",
        "kind": "dedicated",
        "owner": {"name": "data-platform"},
        "outputs": [{"path": "x.csv"}],
        "recipients": [{"name": "a", "source": "marts.a"}],
    }
    load_config(write(tmp_path, base))
    rejects(
        tmp_path,
        {**base, "filters": [{"column": "x", "not_null": True}]},
        "delivery settings only",
    )
    rejects(tmp_path, {**base, "recipients": [{"name": "a"}]}, "needs `source`")


def test_ad_hoc_cannot_be_scheduled(tmp_path):
    data = {
        "name": "a",
        "kind": "ad_hoc",
        "owner": {"name": "data-platform"},
        "source": "marts.x",
        "outputs": [{"path": "x.csv"}],
        "recipients": [{"name": "a", "schedule": {"cron": "0 0 * * *"}}],
    }
    rejects(tmp_path, data, "never scheduled")


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"source": "marts.x; DROP TABLE y"}, "pattern"),
        ({"columns": [{"name": "a b"}]}, "pattern"),
        ({"columns": [{"expr": "1"}]}, "needs an `alias`"),
        ({"unexpected": 1}, "Extra inputs"),
        (
            {
                "window": {
                    "column": "t",
                    "start_exclusive_end_inclusive": {"start": "yesterday"},
                }
            },
            "unknown date keyword",
        ),
        (
            {
                "window": {
                    "column": "t",
                    "start_exclusive_end_inclusive": {"end": "last_run_end"},
                }
            },
            "start bound",
        ),
        ({"filters": [{"column": "x", "in": [1], "not_null": True}]}, "exactly one of"),
        (
            {
                "outputs": [
                    {"path": "x.parquet", "format": "parquet", "trailer_record": "T"}
                ]
            },
            "csv outputs only",
        ),
    ],
)
def test_unsafe_or_contradictory_configs_raise(tmp_path, override, message):
    rejects(tmp_path, shared_config(**override), message)


def test_incremental_max_value_with_row_filters_warns(tmp_path):
    data = shared_config(filters=[{"column": "status", "in": ["active"]}])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_config(write(tmp_path, data))
    assert any("ignores row filters" in str(w.message) for w in caught)


def test_duplicate_export_names_raise(tmp_path):
    for i in range(2):
        (tmp_path / f"{i}.yml").write_text(yaml.safe_dump(shared_config()))
    with pytest.raises(ConfigError, match="duplicate export names"):
        load_configs(tmp_path)
