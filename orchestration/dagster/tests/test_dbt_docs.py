"""dbt docs publishing: destination routing, the live-site refusal, and the
packaged site's build stamp."""

import datetime as dt
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from src.jobs.dbt_docs import (
    LIVE_DESTINATION,
    METADATA_FILE,
    TEST_DESTINATION,
    LivePublishRefused,
    build_metadata,
    package_site,
    publish_dbt_docs_job,
    resolve_destination,
    upload,
)


def test_prod_publishes_live():
    assert resolve_destination("prod").uri == LIVE_DESTINATION


@pytest.mark.parametrize("deployment", ["stage", "branch-1234", None, ""])
def test_everything_else_publishes_to_test(deployment):
    assert resolve_destination(deployment).uri == TEST_DESTINATION


@pytest.mark.parametrize("deployment", ["stage", "branch-1234", None])
def test_live_from_non_prod_is_refused_without_the_flag(deployment):
    with pytest.raises(LivePublishRefused):
        resolve_destination(deployment, "live")
    allowed = resolve_destination(deployment, "live", allow_live_from_non_prod=True)
    assert allowed.uri == LIVE_DESTINATION


def test_prod_can_publish_to_test():
    assert resolve_destination("prod", "test").uri == TEST_DESTINATION


def test_unknown_destination_is_rejected():
    with pytest.raises(ValueError):
        resolve_destination("prod", "staging-site")


@pytest.fixture
def target_dir(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "index.html").write_text("<html></html>")
    (target / "catalog.json").write_text("{}")
    (target / "manifest.json").write_text(
        json.dumps({"metadata": {"dbt_version": "1.12.5"}})
    )
    return target


def test_packaged_site_carries_the_build_stamp(target_dir, tmp_path):
    built_at = dt.datetime(2026, 1, 5, 7, 0, tzinfo=dt.UTC)
    metadata = build_metadata(target_dir, "abc1234", "stage", built_at)
    site = package_site(target_dir, tmp_path / "site", metadata)

    assert sorted(p.name for p in site.iterdir()) == [
        METADATA_FILE,
        "catalog.json",
        "index.html",
        "manifest.json",
    ]
    assert json.loads((site / METADATA_FILE).read_text()) == {
        "git_sha": "abc1234",
        "built_at": "2026-01-05T07:00:00+00:00",
        "dbt_version": "1.12.5",
        "deployment": "stage",
    }


def test_upload_writes_an_immutable_version_and_latest(target_dir, tmp_path):
    metadata = build_metadata(target_dir, "abc1234", None, dt.datetime.now(dt.UTC))
    site = package_site(target_dir, tmp_path / "site", metadata)
    uploads = []

    class FakeS3:
        def upload_file(self, filename, bucket, key):
            uploads.append((bucket, key))

    upload(site, resolve_destination(None), "abc1234", FakeS3())

    keys = {key for _, key in uploads}
    assert {b for b, _ in uploads} == {"example-dbt-docs"}
    assert "test/versions/abc1234/_build.json" in keys
    assert "test/latest/index.html" in keys
    assert len(keys) == 8


class FakeDbt(dg.ConfigurableResource):
    target: str

    def cli(self, args):
        CALLS.append(args)
        invocation = MagicMock()
        invocation.wait.return_value.target_path = Path(self.target)
        return invocation


CALLS: list = []


def run_job(target_dir, config, monkeypatch):
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    monkeypatch.delenv("DEPLOYMENT", raising=False)
    monkeypatch.setenv("GIT_SHA", "abc1234")
    CALLS.clear()
    return publish_dbt_docs_job.execute_in_process(
        run_config={"ops": {"publish_dbt_docs": {"config": config}}},
        resources={"dbt": FakeDbt(target=str(target_dir))},
        raise_on_error=False,
    )


def test_op_refuses_live_before_running_dbt(target_dir, monkeypatch):
    result = run_job(target_dir, {"destination": "live"}, monkeypatch)
    assert not result.success
    assert CALLS == []


def test_op_generates_packages_and_uploads(target_dir, monkeypatch):
    client = MagicMock()
    with patch("boto3.client", return_value=client):
        result = run_job(target_dir, {}, monkeypatch)
    assert result.success
    assert CALLS == [["docs", "generate"]]
    keys = {c.args[2] for c in client.upload_file.call_args_list}
    assert "test/versions/abc1234/_build.json" in keys
