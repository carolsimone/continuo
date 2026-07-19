"""Unit tests for parse_cache_fetcher.main.

No S3 access required — boto3 is patched with a MagicMock. The fetcher never
fails the Job it runs in: every error path degrades (logs, writes a
termination message, exits 0) instead of raising or exiting nonzero.
"""
import io
from unittest import mock

import pytest

import parse_cache_fetcher  # pythonpath="s3-sidecar" in pyproject
import s3_common


def test_hydrates_dest_and_writes_termination_message(tmp_path, monkeypatch):
    dest = tmp_path / "target" / "partial_parse.msgpack"
    term = tmp_path / "term"
    monkeypatch.setenv("PARSE_CACHE_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setenv("PARSE_CACHE_DEST", str(dest))
    monkeypatch.setattr(parse_cache_fetcher, "TERMINATION_LOG", str(term))
    fake = mock.MagicMock()
    fake.get_object.return_value = {"Body": io.BytesIO(b"BYTES")}
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    parse_cache_fetcher.main()

    fake.get_object.assert_called_once_with(Bucket="continuo", Key="svc/parse-cache/team-x/partial_parse.msgpack")
    assert dest.read_bytes() == b"BYTES"
    assert term.read_text() == "hydrated"


def test_degrades_on_missing_object(tmp_path, monkeypatch):
    dest = tmp_path / "target" / "partial_parse.msgpack"
    term = tmp_path / "term"
    monkeypatch.setenv("PARSE_CACHE_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setenv("PARSE_CACHE_DEST", str(dest))
    monkeypatch.setattr(parse_cache_fetcher, "TERMINATION_LOG", str(term))
    fake = mock.MagicMock()
    fake.get_object.side_effect = Exception("NoSuchKey: continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    with pytest.raises(SystemExit) as e:
        parse_cache_fetcher.main()

    assert e.value.code == 0
    assert not dest.exists()
    assert term.read_text().startswith("degraded:")


def test_cleans_up_partial_dest_on_write_failure(tmp_path, monkeypatch):
    """A write failure after the dest file was already created (e.g. disk
    full mid-write) must not leave a partially-written cache file behind for
    the team container to mount and misread as a valid cache."""
    dest = tmp_path / "target" / "partial_parse.msgpack"
    term = tmp_path / "term"
    monkeypatch.setenv("PARSE_CACHE_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setenv("PARSE_CACHE_DEST", str(dest))
    monkeypatch.setattr(parse_cache_fetcher, "TERMINATION_LOG", str(term))
    fake = mock.MagicMock()
    fake.get_object.return_value = {"Body": io.BytesIO(b"BYTES")}
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    real_open = open

    def failing_open(path, mode="r", *args, **kwargs):
        if str(path) == str(dest) and "w" in mode:
            # Simulate a write that creates the file before failing partway through.
            real_open(path, mode, *args, **kwargs).close()
            raise OSError("disk full")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", failing_open)

    with pytest.raises(SystemExit) as e:
        parse_cache_fetcher.main()

    assert e.value.code == 0
    assert not dest.exists(), "partially-written dest file must be cleaned up"
    assert term.read_text().startswith("degraded:write ")
