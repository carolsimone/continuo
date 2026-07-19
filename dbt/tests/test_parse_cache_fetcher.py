"""Unit tests for parse_cache_fetcher.main.

No S3 access required — boto3 is patched with a MagicMock. The fetcher never
fails the Job it runs in: every error path degrades (logs, writes a
termination message, exits 0) instead of raising or exiting nonzero.
"""
import io
import os
import stat
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
    # The team container runs as its own (possibly non-65532) uid and dbt
    # rewrites this file in place ('wb') on every partial-parse invalidation;
    # a default-umask 0644 file owned by 65532 would EACCES that rewrite.
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o666
    assert stat.S_IMODE(os.stat(dest.parent).st_mode) == 0o777


def test_hydrates_when_dest_dir_preexists_and_is_not_owned(tmp_path, monkeypatch):
    """Regression test for the production layout: PARSE_CACHE_DEST's directory
    is always the "parse-cache" emptyDir's mount root (see
    parseCacheInitContainer/CreateQueryJob), which kubelet creates — and
    already leaves world-writable — before this container starts. This
    sidecar does not own that directory and runs with every Linux capability
    dropped, so a chmod attempt against it fails with EPERM (os.makedirs(...,
    exist_ok=True) against an already-existing directory does not confer
    ownership). Prior to this fix the fetcher chmod'd dest_dir unconditionally
    and that EPERM degraded every single hydrate attempt in production,
    without any local test catching it (the other tests here all use a fresh
    tmp_path subdirectory the fetcher itself creates, which it does own)."""
    dest_dir = tmp_path / "mount-root"
    dest_dir.mkdir()  # pre-exists, like the kubelet-created emptyDir mount root
    dest = dest_dir / "partial_parse.msgpack"
    term = tmp_path / "term"
    monkeypatch.setenv("PARSE_CACHE_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setenv("PARSE_CACHE_DEST", str(dest))
    monkeypatch.setattr(parse_cache_fetcher, "TERMINATION_LOG", str(term))
    fake = mock.MagicMock()
    fake.get_object.return_value = {"Body": io.BytesIO(b"BYTES")}
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    real_chmod = os.chmod

    def chmod_denying_dir(path, mode, *args, **kwargs):
        if str(path) == str(dest_dir):
            raise PermissionError(1, "Operation not permitted")
        return real_chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(os, "chmod", chmod_denying_dir)

    parse_cache_fetcher.main()

    assert dest.read_bytes() == b"BYTES"
    assert term.read_text() == "hydrated"
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o666


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
