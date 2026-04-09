import json
import pytest
from unittest.mock import MagicMock, call
from adapters.redis.publisher import SchedulesLoadedPublisher


def test_publish_sends_correct_payload():
    redis_client = MagicMock()
    publisher = SchedulesLoadedPublisher(redis_client, "schedules.loaded:v1")

    publisher.publish(
        event_id="abc123",
        schedule_names=["daily", "hourly"],
        manifest_versions={"service-a": "v3"},
    )

    redis_client.xadd.assert_called_once()
    call_args = redis_client.xadd.call_args
    stream = call_args[0][0]
    assert stream == "schedules.loaded:v1"

    payload = json.loads(call_args[0][1]["payload"])
    assert payload["event_id"] == "abc123"
    assert set(payload["schedule_names"]) == {"daily", "hourly"}
    assert payload["manifest_versions"] == {"service-a": "v3"}


def test_publish_empty_schedule_names():
    redis_client = MagicMock()
    publisher = SchedulesLoadedPublisher(redis_client, "schedules.loaded:v1")
    publisher.publish(event_id="abc", schedule_names=[], manifest_versions={})
    redis_client.xadd.assert_called_once()
    payload = json.loads(redis_client.xadd.call_args[0][1]["payload"])
    assert payload["schedule_names"] == []
    assert payload["manifest_versions"] == {}
