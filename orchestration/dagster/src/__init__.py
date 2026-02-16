"""Dagster orchestration — Definitions entry point.

This module is the top-level code location loaded by the Dagster webserver and
daemon.  It discovers and registers every asset, sensor, schedule, resource,
and asset check defined in the submodules below.
"""

from dagster import Definitions

from src.assets import all_assets
from src.checks import all_checks
from src.resources import RESOURCES
from src.schedules import all_schedules
from src.sensors import all_sensors

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    sensors=all_sensors,
    schedules=all_schedules,
    resources=RESOURCES,
)
