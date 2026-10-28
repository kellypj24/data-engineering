"""SQS sensor (E47) against a moto queue: one run per message, deletion one
tick later, and a crash before the cursor is saved neither loses nor
duplicates a run."""

import json

import boto3
import pytest
from dagster import build_sensor_context
from moto import mock_aws

from src.jobs.landing import process_landing_file_job
from src.sensors.sqs_sensor import build_sqs_sensor, s3_event_run_config


def s3_event(key: str) -> str:
    return json.dumps(
        {"Records": [{"s3": {"bucket": {"name": "landing"}, "object": {"key": key}}}]}
    )


@pytest.fixture
def queue(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        sqs = boto3.client("sqs")
        # Visibility 0: an undeleted message is redelivered on the next receive.
        url = sqs.create_queue(
            QueueName="landing-events", Attributes={"VisibilityTimeout": "0"}
        )["QueueUrl"]
        yield sqs, url


def sensor_for(url):
    return build_sqs_sensor(
        name="test_sqs_sensor",
        queue_url=url,
        job=process_landing_file_job,
        run_config_fn=s3_event_run_config,
    )


def tick(sensor, cursor=None):
    context = build_sensor_context(cursor=cursor)
    requests = list(sensor(context))
    return requests, context.cursor


def queued(sqs, url) -> int:
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]) + int(
        attrs["ApproximateNumberOfMessagesNotVisible"]
    )


def test_a_message_produces_one_run_and_is_deleted_on_the_next_tick(queue):
    sqs, url = queue
    message_id = sqs.send_message(QueueUrl=url, MessageBody=s3_event("inbound/a.csv"))[
        "MessageId"
    ]
    sensor = sensor_for(url)

    requests, cursor = tick(sensor)
    assert [r.run_key for r in requests] == [message_id]
    config = requests[0].run_config["ops"]["process_landing_file"]["config"]
    assert config == {"s3_bucket": "landing", "s3_key": "inbound/a.csv"}
    assert queued(sqs, url) == 1  # not deleted on the tick that queued the run

    # The next tick deletes it before receiving, so nothing is requested again.
    requests, _ = tick(sensor, cursor)
    assert requests == []
    assert queued(sqs, url) == 0


def test_crash_before_cursor_save_redelivers_with_the_same_run_key(queue):
    sqs, url = queue
    message_id = sqs.send_message(QueueUrl=url, MessageBody=s3_event("inbound/b.csv"))[
        "MessageId"
    ]
    sensor = sensor_for(url)

    first, _lost_cursor = tick(sensor)  # the cursor is never saved
    second, cursor = tick(sensor)  # starts from no cursor, as after a crash
    assert [r.run_key for r in first] == [r.run_key for r in second] == [message_id]

    tick(sensor, cursor)
    assert queued(sqs, url) == 0


def test_unparseable_message_is_left_on_the_queue(queue):
    sqs, url = queue
    sqs.send_message(QueueUrl=url, MessageBody="not json")
    sensor = sensor_for(url)

    requests, cursor = tick(sensor)
    assert requests == []
    assert json.loads(cursor) == {}
    tick(sensor, cursor)
    assert queued(sqs, url) == 1


def test_empty_queue_url_raises_at_load():
    with pytest.raises(ValueError, match="queue_url is empty"):
        sensor_for("")
