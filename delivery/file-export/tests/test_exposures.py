"""Generated dbt exposures and the drift gate, offline against a fixture manifest."""

from pathlib import Path

import pytest
import yaml

from file_export.cli import main
from file_export.config import ExportConfig
from file_export.exposures import ExposureError, build, load_manifest, render
from tests.conftest import shared_config

MANIFEST = Path(__file__).parent / "fixtures" / "manifest.json"


def config(**overrides) -> ExportConfig:
    base = shared_config(
        source="marts.fct_orders",
        columns=[{"name": "order_id"}, {"name": "customer_id"}],
        window={
            "column": "created_at",
            "start_exclusive_end_inclusive": {
                "start": "last_run_end",
                "end": "max_value",
            },
        },
        outputs=[
            {"path": "{recipient}/o.csv"},
            {"path": "{recipient}/o.parquet", "format": "parquet"},
        ],
    )
    base.update(overrides)
    return ExportConfig.model_validate(base)


def manifest():
    return load_manifest(MANIFEST)


def test_one_exposure_per_delivered_file_with_deterministic_names():
    doc = build([config()], manifest())
    names = [e["name"] for e in doc["exposures"]]
    assert names == [
        "export__lines__north__csv",
        "export__lines__north__parquet",
        "export__lines__south__csv",
        "export__lines__south__parquet",
    ]
    first = doc["exposures"][0]
    assert first["depends_on"] == ["ref('fct_orders')"]
    assert first["owner"] == {"name": "data-platform"}
    assert first["meta"]["recipient"] == "north"
    assert build([config()], manifest()) == doc  # deterministic


def test_same_format_twice_gets_indexed_names():
    doc = build([config(outputs=[{"path": "a.csv"}, {"path": "b.csv"}])], manifest())
    assert {e["name"] for e in doc["exposures"]} >= {
        "export__lines__north__csv_0",
        "export__lines__north__csv_1",
    }


def test_alias_resolves_to_the_model_name():
    c = config(
        source="marts.orders_legacy",
        columns=[{"name": "order_id"}],
        tenant_column="order_id",
    )
    assert build([c], manifest())["exposures"][0]["depends_on"] == ["ref('orders_v2')"]


def test_ambiguous_alias_is_settled_by_schema():
    c = config(source="b.dupe", columns=[{"name": "x"}], tenant_column="x", window=None)
    assert build([c], manifest())["exposures"][0]["depends_on"] == ["ref('dupe_b')"]


def test_missing_model_fails():
    with pytest.raises(ExposureError, match="matches no dbt model"):
        build([config(source="marts.fct_nothing")], manifest())


def test_non_model_nodes_do_not_count():
    with pytest.raises(ExposureError, match="matches no dbt model"):
        build([config(source="marts.fct_undocumented")], manifest())


def test_pinned_column_the_model_lacks_fails():
    c = config(columns=[{"name": "order_id"}, {"name": "discount"}])
    with pytest.raises(
        ExposureError, match=r"\['discount'\] not documented on dbt model fct_orders"
    ):
        build([c], manifest())


def test_filter_and_window_columns_are_checked_too():
    c = config(filters=[{"column": "region", "in": ["north"]}])
    with pytest.raises(ExposureError, match="region"):
        build([c], manifest())


def test_every_problem_is_reported_at_once():
    bad = [
        config(name="a", source="marts.nope"),
        config(name="b", columns=[{"name": "zzz"}]),
    ]
    with pytest.raises(ExposureError) as exc:
        build(bad, manifest())
    assert "marts.nope" in str(exc.value)
    assert "zzz" in str(exc.value)


def test_duplicate_exposure_names_fail():
    with pytest.raises(ExposureError, match="duplicate exposure names"):
        build([config(name="a-b"), config(name="a_b")], manifest())


def test_rendered_file_is_valid_dbt_yaml():
    doc = yaml.safe_load(render(build([config()], manifest())))
    assert doc["version"] == 2
    assert len(doc["exposures"]) == 4


def write_configs(directory: Path, *configs: dict):
    directory.mkdir(exist_ok=True)
    for data in configs:
        (directory / f"{data['name']}.yml").write_text(yaml.safe_dump(data))


def exposures(tmp_path, action):
    return main(
        [
            "exposures",
            action,
            "--manifest",
            str(MANIFEST),
            "--out",
            str(tmp_path / "exp.yml"),
            "--configs",
            str(tmp_path / "configs"),
        ]
    )


def test_check_passes_after_write_and_fails_after_an_unregenerated_config(
    tmp_path, capsys
):
    data = config().model_dump(mode="json", by_alias=True, exclude_none=True)
    write_configs(tmp_path / "configs", data)
    assert exposures(tmp_path, "--write") == 0
    assert exposures(tmp_path, "--check") == 0

    write_configs(tmp_path / "configs", {**data, "name": "lines-two"})
    assert exposures(tmp_path, "--check") == 1
    assert "is stale" in capsys.readouterr().err


def test_check_fails_when_the_file_is_missing(tmp_path):
    write_configs(
        tmp_path / "configs",
        config().model_dump(mode="json", by_alias=True, exclude_none=True),
    )
    assert exposures(tmp_path, "--check") == 1


def test_check_fails_on_a_manifest_mismatch_even_if_the_file_matches(tmp_path, capsys):
    data = config().model_dump(mode="json", by_alias=True, exclude_none=True)
    write_configs(tmp_path / "configs", {**data, "source": "marts.fct_nothing"})
    assert exposures(tmp_path, "--check") == 1
    assert "matches no dbt model" in capsys.readouterr().err
