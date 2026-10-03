import pytest

import redis_streams


def test_entries_added_read_from_raw_xinfo():
    raw = ["length", "3", "radix-tree-keys", "1", "entries-added", "41", "first-entry", "1-0"]
    assert redis_streams.entries_added(raw) == (41, True)


def test_entries_added_falls_back_to_length_when_counter_missing():
    assert redis_streams.entries_added(["length", "3", "groups", "1"]) == (3, False)


def test_entries_added_rejects_unknown_output():
    with pytest.raises(ValueError):
        redis_streams.entries_added(["groups", "1"])


def test_redis_command_prefixes_the_configured_cli(monkeypatch):
    monkeypatch.setenv("BENCH_REDIS_CMD", '["redis-cli", "-h", "x"]')
    assert redis_streams.redis_command(["PING"]) == ["redis-cli", "-h", "x", "PING"]
