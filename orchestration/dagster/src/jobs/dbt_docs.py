"""Publish the dbt docs site, versioned, to a destination chosen by deployment.

``publish_dbt_docs_job`` runs ``dbt docs generate``, packages the static site
with a ``_build.json`` stamp (git SHA, build time, dbt version, deployment),
and uploads it twice: to ``versions/<sha>/`` (immutable) and ``latest/``.

Where it goes is decided like the target database (src/utils/deployment.py):
the production deployment publishes to the live site; stage, any unknown
deployment, and no deployment publish to the test destination. Asking for the
live site from anywhere but production is refused unless
``allow_live_from_non_prod`` is set, so a local run cannot overwrite the live
docs with a half-built branch.

Customisation
-------------
* Set ``LIVE_DESTINATION`` / ``TEST_DESTINATION`` to your buckets and prefixes.
* ``GIT_SHA`` should be set by the deploy; otherwise ``git rev-parse`` is tried.
"""

import datetime as dt
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dagster import Config, OpExecutionContext, job, op
from dagster_dbt import DbtCliResource

from src.utils.deployment import PROD_DEPLOYMENT, current_deployment, deployment_tags

LIVE_DESTINATION = "s3://example-dbt-docs/live"
TEST_DESTINATION = "s3://example-dbt-docs/test"
SITE_FILES = ("index.html", "manifest.json", "catalog.json")
METADATA_FILE = "_build.json"


class LivePublishRefused(RuntimeError):
    """The live docs site was requested from a non-production deployment."""


@dataclass(frozen=True)
class Destination:
    name: str  # "live" | "test"
    uri: str


def resolve_destination(
    deployment: str | None,
    requested: str | None = None,
    *,
    allow_live_from_non_prod: bool = False,
) -> Destination:
    """prod -> live; anything else -> test. ``requested="live"`` from a
    non-prod deployment raises unless explicitly allowed."""
    is_prod = deployment == PROD_DEPLOYMENT
    wants_live = requested == "live" if requested else is_prod
    if requested not in (None, "live", "test"):
        raise ValueError(
            f"unknown docs destination {requested!r}; use 'live' or 'test'"
        )
    if wants_live and not is_prod and not allow_live_from_non_prod:
        raise LivePublishRefused(
            f"refusing to publish to the live docs site from deployment "
            f"{deployment or 'unset'!r}; set allow_live_from_non_prod to override"
        )
    if wants_live:
        return Destination("live", LIVE_DESTINATION)
    return Destination("test", TEST_DESTINATION)


def build_metadata(
    target_dir: Path, git_sha: str, deployment: str | None, built_at: dt.datetime
) -> dict:
    manifest = json.loads((target_dir / "manifest.json").read_text())
    return {
        "git_sha": git_sha,
        "built_at": built_at.isoformat(),
        "dbt_version": manifest["metadata"]["dbt_version"],
        "deployment": deployment or "unset",
    }


def package_site(target_dir: Path, out_dir: Path, metadata: dict) -> Path:
    """Copy the static site out of dbt's target/ and stamp it."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in SITE_FILES:
        shutil.copy2(target_dir / name, out_dir / name)
    (out_dir / METADATA_FILE).write_text(json.dumps(metadata, indent=2) + "\n")
    return out_dir


def upload(
    site_dir: Path, destination: Destination, git_sha: str, s3_client
) -> list[str]:
    """Upload to versions/<sha>/ and latest/. Returns the object URIs written."""
    bucket, _, prefix = destination.uri.removeprefix("s3://").partition("/")
    written = []
    for folder in (f"versions/{git_sha}", "latest"):
        for path in sorted(site_dir.iterdir()):
            key = "/".join(p for p in (prefix, folder, path.name) if p)
            s3_client.upload_file(str(path), bucket, key)
            written.append(f"s3://{bucket}/{key}")
    return written


def git_sha() -> str:
    if sha := os.environ.get("GIT_SHA"):
        return sha
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()


# No `from __future__ import annotations` in this module: Dagster resolves the
# op's config and resource types from real annotations, not strings.
class PublishDocsConfig(Config):
    destination: str | None = None  # "live" | "test"; default follows deployment
    allow_live_from_non_prod: bool = False


@op
def publish_dbt_docs(
    context: OpExecutionContext, config: PublishDocsConfig, dbt: DbtCliResource
):
    deployment = current_deployment()
    # Resolve (and possibly refuse) before spending time on a build.
    destination = resolve_destination(
        deployment,
        config.destination,
        allow_live_from_non_prod=config.allow_live_from_non_prod,
    )
    invocation = dbt.cli(["docs", "generate"]).wait()
    target_dir = invocation.target_path
    sha = git_sha()
    metadata = build_metadata(target_dir, sha, deployment, dt.datetime.now(dt.UTC))
    site = package_site(target_dir, target_dir / "docs_site", metadata)

    import boto3

    written = upload(site, destination, sha, boto3.client("s3"))
    context.log.info(
        f"published dbt docs {sha} to {destination.name}: {len(written)} objects"
    )


@job(
    description="dbt docs generate, stamped and uploaded; destination follows the deployment.",
    tags=deployment_tags(current_deployment()),
)
def publish_dbt_docs_job():
    publish_dbt_docs()
