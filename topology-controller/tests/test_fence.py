from unittest.mock import MagicMock

import pytest
import redis
import redis.exceptions as redis_exceptions

from adapters.redis import fence
from adapters.redis.fence import (
    PERMISSION_PROBE_KEY,
    PermissionFenceError,
    check_permission_fence,
)
from config.config import REDIS_ACL_MODE_PER_SERVICE, REDIS_ACL_MODE_SHARED

_VIOLATION = (
    "redis login keywide can read keys outside its own: "
    "apply the rules in deploy/redis-acl-rules.acl"
)


def _client(username="keywide", answer=None, side_effect=None):
    """A redis-py client signed in as username whose probe answers with
    answer, or raises side_effect (a list raises or returns in turn)."""
    client = redis.Redis(host="redis.invalid", username=username, password="pw")
    client.execute_command = MagicMock(return_value=answer, side_effect=side_effect)
    return client


class _Clock:
    """A monotonic clock that sleeping advances."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_fence_passes_when_redis_answers_noperm():
    client = _client(side_effect=redis_exceptions.NoPermissionError(
        "User keywide has no permissions to access the 'continuo-permission-probe' key"))
    check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    client.execute_command.assert_called_once_with("XINFO", "GROUPS", PERMISSION_PROBE_KEY)


def test_fence_fails_when_the_probe_succeeds():
    client = _client(answer=[])
    with pytest.raises(PermissionFenceError) as exc_info:
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    assert str(exc_info.value) == _VIOLATION


def test_fence_fails_when_redis_answers_no_such_key():
    client = _client(side_effect=redis_exceptions.ResponseError("no such key"))
    with pytest.raises(PermissionFenceError) as exc_info:
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    assert str(exc_info.value) == _VIOLATION


def test_fence_skips_shared_mode():
    client = _client(answer=[])
    check_permission_fence(client, REDIS_ACL_MODE_SHARED)
    client.execute_command.assert_not_called()


def test_fence_refuses_an_unknown_mode():
    client = _client(answer=[])
    with pytest.raises(PermissionFenceError, match="unknown REDIS_ACL_MODE 'perservice'"):
        check_permission_fence(client, "perservice")
    client.execute_command.assert_not_called()


def test_fence_names_the_default_login_when_no_username_is_set():
    client = _client(username=None, answer=[])
    with pytest.raises(PermissionFenceError, match="^redis login default can read keys outside its own"):
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)


def test_fence_names_the_login_its_credential_provider_signs_in_as():
    provider = MagicMock()
    provider.get_credentials.return_value = ("continuo_topology_controller", "pw")
    client = redis.Redis(host="redis.invalid", credential_provider=provider)
    client.execute_command = MagicMock(return_value=[])
    with pytest.raises(PermissionFenceError, match="^redis login continuo_topology_controller "):
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)


def test_fence_waits_out_a_redis_it_cannot_reach_yet(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(fence.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(fence.time, "sleep", clock.sleep)
    client = _client(side_effect=[
        redis_exceptions.ConnectionError("Connection refused"),
        redis_exceptions.BusyLoadingError("Redis is loading the dataset in memory"),
        redis_exceptions.NoPermissionError("no permissions"),
    ])
    check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    assert client.execute_command.call_count == 3
    assert clock.sleeps == [1.0, 2.0]


def test_fence_raises_the_connection_error_once_the_wait_is_spent(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(fence.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(fence.time, "sleep", clock.sleep)
    refused = redis_exceptions.ConnectionError("Connection refused")
    client = _client(side_effect=refused)
    with pytest.raises(redis_exceptions.ConnectionError) as exc_info:
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    assert exc_info.value is refused
    assert not isinstance(exc_info.value, PermissionFenceError)
    assert clock.now <= 300.0


def test_fence_does_not_wait_out_a_refused_sign_in(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(fence.time, "sleep", clock.sleep)
    client = _client(side_effect=redis_exceptions.AuthenticationError(
        "invalid username-password pair or user is disabled."))
    with pytest.raises(redis_exceptions.AuthenticationError):
        check_permission_fence(client, REDIS_ACL_MODE_PER_SERVICE)
    assert clock.sleeps == []


def test_the_fence_acts_on_exactly_the_modes_config_accepts():
    from config import config
    assert config._REDIS_ACL_MODES == (fence.SHARED, fence.PER_SERVICE)


def _chart_file(*parts):
    import pathlib

    return pathlib.Path(__file__).resolve().parents[2].joinpath("deploy", "continuo", *parts)


def test_redis_acl_modes_match_the_chart():
    """The modes config accepts, the fence acts on and the chart's schema allows agree.

    The chart's credentials.redis.mode reaches this service as REDIS_ACL_MODE;
    a mode the chart allows but this service refuses would crash it at boot.
    """
    import json

    from config.config import _REDIS_ACL_MODES

    schema = json.loads(_chart_file("values.schema.json").read_text())
    chart = set(schema["properties"]["credentials"]["properties"]["redis"]["properties"]["mode"]["enum"])
    assert set(_REDIS_ACL_MODES) == chart
    assert {fence.SHARED, fence.PER_SERVICE} == chart


def test_permission_probe_key_is_outside_every_generated_rule():
    """No generated login's key pattern covers the probe key, or its fence would fail."""
    import fnmatch
    import json

    rules = json.loads(_chart_file("files", "redis-acl-rules.json").read_text())
    for login in rules["logins"]:
        for token in login["rule"].split():
            if "~" not in token:
                continue
            pattern = token.split("~", 1)[1]
            assert not fnmatch.fnmatchcase(fence.PERMISSION_PROBE_KEY, pattern), (login["username"], pattern)
