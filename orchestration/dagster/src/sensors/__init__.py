"""Sensor definitions — re-exported from submodules."""

from src.sensors.s3_sensor import s3_file_arrival_sensor
from src.telemetry.sensors import telemetry_sensors

all_sensors = [
    s3_file_arrival_sensor,
    *telemetry_sensors,
]
