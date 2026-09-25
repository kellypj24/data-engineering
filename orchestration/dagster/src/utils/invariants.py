"""Invariants over a whole ``Definitions`` object.

Per-definition tests only protect the definitions someone remembered to test.
These assert properties of *every* schedule, sensor, job, and dbt asset
definition, so they also cover the next one added. ``tests/test_invariants.py``
runs them against this code location; a downstream project runs the same
function against its own ``Definitions``:

    from src.utils.invariants import check_definitions
    assert check_definitions(defs, observing_sensors={"freshness_watch"}) == []

Each returned string names the offending definition.

Checks
------
* Schedules default to STOPPED: activation is an explicit, reviewed act.
* Sensors that launch a job default to STOPPED. Sensors that only observe may
  be RUNNING, but must be listed in ``observing_sensors`` by name; a sensor
  with no job target that is not listed is flagged, since its RunRequests
  have nothing to launch.
* Schedules run in ``timezone`` (UTC), set explicitly rather than inherited.
* Cron strings parse.
* No duplicate job, schedule, or sensor names.
* Jobs tagged ``MANUAL_ONLY_TAG`` are never targeted by a schedule or sensor.
* ``@dbt_assets`` definitions partition the manifest's models, seeds, and
  snapshots: no node in two definitions, none in no definition. A seed in no
  definition has been excluded, which silently drops its data tests.
* Every dbt asset definition runs ``dbt build`` (not ``run`` then ``test``),
  checked by invoking it against a recording stand-in for ``DbtCliResource``.
* A job that selects a dbt node also selects that node's parent seeds, checked
  by resolving the job's asset selection, not by reading its selector string.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from unittest.mock import MagicMock

from dagster import (
    AssetsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    build_asset_context,
)
from dagster._utils.schedules import is_valid_cron_schedule

MANUAL_ONLY_TAG = "toolkit/manual_only"
DBT_RESOURCE_TYPES = {"model", "seed", "snapshot"}


def check_definitions(
    defs: Definitions,
    *,
    observing_sensors: Iterable[str] = (),
    timezone: str = "UTC",
) -> list[str]:
    problems = _duplicate_names(defs)
    if problems:
        return problems  # Dagster refuses to resolve duplicates; stop here.

    repo = defs.get_repository_def()
    jobs = {job.name: job for job in repo.get_all_jobs()}
    observing = set(observing_sensors)

    for schedule in repo.schedule_defs:
        name = f"schedule {schedule.name}"
        if schedule.default_status != DefaultScheduleStatus.STOPPED:
            problems.append(f"{name}: default_status must be STOPPED")
        if schedule.execution_timezone != timezone:
            problems.append(
                f"{name}: execution_timezone is {schedule.execution_timezone!r}, "
                f"expected {timezone!r} set explicitly"
            )
        crons = schedule.cron_schedule
        for cron in [crons] if isinstance(crons, str) else crons:
            if not is_valid_cron_schedule(cron):
                problems.append(f"{name}: invalid cron {cron!r}")

    for sensor in repo.sensor_defs:
        name = f"sensor {sensor.name}"
        launches = bool(sensor.targets)
        if launches and sensor.default_status != DefaultSensorStatus.STOPPED:
            problems.append(
                f"{name}: launches a job, so default_status must be STOPPED"
            )
        if not launches and sensor.name not in observing:
            problems.append(
                f"{name}: has no job target and is not declared in observing_sensors"
            )

    targeted = [(f"schedule {s.name}", s.job_name) for s in repo.schedule_defs]
    targeted += [
        (f"sensor {s.name}", t.job_name) for s in repo.sensor_defs for t in s.targets
    ]
    for source, job_name in targeted:
        job = jobs.get(job_name)
        if job is not None and job.tags.get(MANUAL_ONLY_TAG) == "true":
            problems.append(f"{source}: targets manual-only job {job_name}")

    problems += _dbt_problems(defs, jobs.values())
    return problems


def _duplicate_names(defs: Definitions) -> list[str]:
    problems = []
    for kind, items in (
        ("job", defs.jobs),
        ("schedule", defs.schedules),
        ("sensor", defs.sensors),
    ):
        counts = Counter(item.name for item in items or [])
        problems += [f"duplicate {kind} name {n}" for n, c in counts.items() if c > 1]
    return problems


def _dbt_problems(defs: Definitions, jobs) -> list[str]:
    dbt_defs = [
        a
        for a in defs.assets or []
        if isinstance(a, AssetsDefinition)
        and any("dagster_dbt/unique_id" in s.metadata for s in a.specs)
    ]
    if not dbt_defs:
        return []

    problems = []
    owner: dict[str, str] = {}  # unique_id -> dbt assets definition name
    key_to_id = {}
    for a in dbt_defs:
        for spec in a.specs:
            unique_id = spec.metadata["dagster_dbt/unique_id"]
            key_to_id[spec.key] = unique_id
            if unique_id in owner:
                problems.append(
                    f"dbt node {unique_id} is in both {owner[unique_id]} and {a.node_def.name}"
                )
            owner[unique_id] = a.node_def.name
        problems += _build_problems(a)

    manifest = next(iter(dbt_defs[0].specs)).metadata["dagster_dbt/manifest"].manifest
    nodes = {
        uid: node
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in DBT_RESOURCE_TYPES
    }
    for uid in sorted(set(nodes) - set(owner)):
        detail = (
            "; an excluded seed's data tests never run"
            if nodes[uid]["resource_type"] == "seed"
            else ""
        )
        problems.append(f"dbt node {uid} is in no @dbt_assets definition{detail}")

    for job in jobs:
        selected = {
            key_to_id[k] for k in job.asset_layer.selected_asset_keys if k in key_to_id
        }
        for uid in sorted(selected):
            for parent in manifest["parent_map"].get(uid, []):
                if parent.startswith("seed.") and parent not in selected:
                    problems.append(
                        f"job {job.name}: selects {uid} but not its seed {parent}, "
                        "so the seed's data tests never run"
                    )
    return problems


def _build_problems(assets_def: AssetsDefinition) -> list[str]:
    """Invoke the dbt asset function with a stand-in resource and record the
    dbt commands it issues."""
    commands: list[list[str]] = []

    class RecordingDbt:
        def cli(self, args, **_):
            commands.append(list(args))
            invocation = MagicMock()
            invocation.stream.return_value = iter([])
            return invocation

    name = assets_def.node_def.name
    try:
        result = assets_def(context=build_asset_context(), dbt=RecordingDbt())
        if result is not None:
            list(result)
    except Exception as exc:  # noqa: BLE001 -- report, don't crash the suite
        return [f"dbt assets {name}: could not invoke with a stand-in dbt: {exc}"]
    verbs = [c[0] for c in commands if c]
    if verbs != ["build"]:
        return [f"dbt assets {name}: runs dbt {verbs}; expected a single `dbt build`"]
    return []
