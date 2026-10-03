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


def test_snapshot_follows_the_scan_cursor(monkeypatch):
    pages = {"0": ["5", "a:v1"], "5": ["0", "b:v1"]}
    counters = {"a:v1": ["entries-added", "7"], "b:v1": ["length", "3"]}

    def fake_redis(args):
        if args[0] == "SCAN":
            assert args[2:] == ["TYPE", "stream", "COUNT", "1000"]
            return pages[args[1]]
        assert args[:2] == ["XINFO", "STREAM"]
        return counters[args[2]]

    monkeypatch.setattr(redis_streams, "_redis", fake_redis)
    assert redis_streams.snapshot() == {"a:v1": 7, "b:v1": 3}


class FakeRun:
    def __init__(self, returncode=0, stdout=""):
        self.calls = []
        self.returncode = returncode
        self.stdout = stdout

    def __call__(self, args, **kwargs):
        self.calls.append((args, kwargs.get("input")))
        return type("Done", (), {"returncode": self.returncode, "stdout": self.stdout, "stderr": "boom"})()


def test_the_password_travels_on_stdin_not_in_the_command(monkeypatch):
    fake = FakeRun(stdout="PONG\n")
    monkeypatch.setattr(redis_streams.subprocess, "run", fake)
    monkeypatch.setenv("BENCH_REDIS_CMD", '["kubectl", "exec", "--", "sh", "-c", "script", "redis-cli"]')
    monkeypatch.setenv("BENCH_REDIS_PASSWORD", "pw-secret-1")
    assert redis_streams._redis(["PING"]) == ["PONG"]
    args, stdin = fake.calls[0]
    assert "pw-secret-1" not in " ".join(args)
    assert stdin == b"pw-secret-1\n"


def test_cli_forwards_its_stdin_after_the_password(monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(redis_streams.subprocess, "run", fake)
    monkeypatch.setenv("BENCH_REDIS_CMD", '["redis-cli"]')
    monkeypatch.setenv("BENCH_REDIS_PASSWORD", "pw-secret-1")
    assert redis_streams.cli(["-x", "XADD", "s", "*", "payload"], b'{"release_id": "r"}') == 0
    assert fake.calls[0] == (["redis-cli", "-x", "XADD", "s", "*", "payload"], b'pw-secret-1\n{"release_id": "r"}')


def test_a_failed_redis_call_is_reported_without_the_command(monkeypatch):
    monkeypatch.setattr(redis_streams.subprocess, "run", FakeRun(returncode=1))
    monkeypatch.setenv("BENCH_REDIS_CMD", '["kubectl", "exec", "--", "redis-cli"]')
    monkeypatch.setenv("BENCH_REDIS_PASSWORD", "pw-secret-1")
    with pytest.raises(RuntimeError) as failure:
        redis_streams._redis(["XINFO", "STREAM", "s"])
    assert "pw-secret-1" not in str(failure.value) and "kubectl" not in str(failure.value)
