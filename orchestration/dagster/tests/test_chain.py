"""The extract -> transform chain: only the head has a cron, and each link has
exactly one upstream sensor and no schedule."""

import dagster as dg

from src import defs
from src.jobs.chain import extract_job, transform_job
from src.sensors.chain import transform_after_extract

repo = defs.get_repository_def()
SCHEDULED = {s.job_name for s in repo.schedule_defs}
CHAINED = {
    t.job_name: s.name
    for s in repo.sensor_defs
    if isinstance(s, dg.RunStatusSensorDefinition)
    for t in s.targets
}


def test_head_has_a_cron():
    assert extract_job.name in SCHEDULED
    assert extract_job.name not in CHAINED


def test_link_has_one_upstream_sensor_and_no_schedule():
    assert transform_job.name not in SCHEDULED
    upstream = [
        s.name
        for s in repo.sensor_defs
        if any(t.job_name == transform_job.name for t in s.targets)
    ]
    assert upstream == [transform_after_extract.name]


def test_extract_success_requests_one_transform_run():
    @dg.op
    def extracted():
        pass

    @dg.job(name="extract_job")
    def stand_in_extract():
        extracted()

    with dg.instance_for_test() as instance:
        result = stand_in_extract.execute_in_process(instance=instance)
        dagster_run = instance.get_run_by_id(result.run_id)
        [record] = instance.get_records_for_run(
            result.run_id, of_type=dg.DagsterEventType.RUN_SUCCESS
        ).records
        context = dg.build_run_status_sensor_context(
            sensor_name=transform_after_extract.name,
            dagster_instance=instance,
            dagster_run=dagster_run,
            dagster_event=record.event_log_entry.dagster_event,
        )
        request = transform_after_extract(context)

    assert request.run_key == result.run_id
