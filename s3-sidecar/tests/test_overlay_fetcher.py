"""Unit tests for overlay_fetcher.main.

The fetcher lays a shadow release's proposed source files into OVERLAY_DEST.
Unlike parse_cache_fetcher it FAILS the Job on any problem: a shadow release
that compiled the committed source instead of the proposed fix would verify
the wrong thing.
"""
import io
import os
import stat
import tarfile
from unittest import mock

import pytest

import overlay_fetcher  # pythonpath="." (sidecar root) in pyproject
import s3_common


def _tar(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, body in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tf.addfile(info, io.BytesIO(body))
    return buf.getvalue()


def _serve(monkeypatch, body: bytes) -> mock.MagicMock:
    fake = mock.MagicMock()
    fake.get_object.return_value = {"Body": io.BytesIO(body)}
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)
    return fake


def test_extracts_files_world_readable(tmp_path, monkeypatch):
    dest = tmp_path / "overlay"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    fake = _serve(monkeypatch, _tar({"models/a.sql": b"select 1", "models/nested/b.sql": b"select 2"}))

    overlay_fetcher.main()

    fake.get_object.assert_called_once_with(Bucket="continuo", Key="svc/shadow-1/source-overlay.tar.gz")
    assert (dest / "models" / "a.sql").read_bytes() == b"select 1"
    assert (dest / "models" / "nested" / "b.sql").read_bytes() == b"select 2"
    assert stat.S_IMODE(os.stat(dest / "models" / "a.sql").st_mode) == 0o644
    assert stat.S_IMODE(os.stat(dest / "models").st_mode) == 0o755


@pytest.mark.parametrize("name", ["../escape.sql", "/abs/escape.sql", "models/../../escape.sql"])
def test_rejects_paths_outside_dest(tmp_path, monkeypatch, name):
    dest = tmp_path / "overlay"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    _serve(monkeypatch, _tar({name: b"x"}))

    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == 4
    assert not (tmp_path / "escape.sql").exists()


def test_rejects_symlink_members(tmp_path, monkeypatch):
    dest = tmp_path / "overlay"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo("models/link.sql")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tf.addfile(info)
    _serve(monkeypatch, buf.getvalue())

    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == 4


def test_missing_config_exits_2(monkeypatch):
    monkeypatch.delenv("SOURCE_OVERLAY_URI", raising=False)
    monkeypatch.delenv("OVERLAY_DEST", raising=False)
    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == 2


def test_fetch_failure_exits_3(tmp_path, monkeypatch):
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(tmp_path / "overlay"))
    fake = mock.MagicMock()
    fake.get_object.side_effect = RuntimeError("boom")
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)
    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == 3
