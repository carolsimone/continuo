"""Unit tests for overlay_fetcher.main.

The fetcher lays a verification run's proposed source files into OVERLAY_DEST
and writes the paths the overlay removes to OVERLAY_DELETE_LIST. Unlike
parse_cache_fetcher it FAILS the Job on any problem: a verification run that
built the committed source instead of the proposed fix would verify the wrong
thing.
"""
import contextlib
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


@contextlib.contextmanager
def _restrictive_umask():
    """Force a 0077 umask so a missing explicit chmod on a created path fails
    the world-readable assertions below instead of silently passing under
    whatever permissive umask happens to be set on the host/CI runner."""
    old = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(old)


def test_extracts_files_world_readable(tmp_path, monkeypatch):
    dest = tmp_path / "overlay"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    fake = _serve(monkeypatch, _tar({"models/a.sql": b"select 1", "models/nested/b.sql": b"select 2"}))

    with _restrictive_umask():
        overlay_fetcher.main()

    fake.get_object.assert_called_once_with(Bucket="continuo", Key="svc/shadow-1/source-overlay.tar.gz")
    assert (dest / "models" / "a.sql").read_bytes() == b"select 1"
    assert (dest / "models" / "nested" / "b.sql").read_bytes() == b"select 2"
    assert stat.S_IMODE(os.stat(dest / "models" / "a.sql").st_mode) == 0o644
    assert stat.S_IMODE(os.stat(dest / "models").st_mode) == 0o755
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o755


def test_extracts_single_top_level_file_dest_world_readable(tmp_path, monkeypatch):
    """OVERLAY_DEST itself must be world-readable even when the tarball has no
    directory members at all (the compile Job's OVERLAY_DEST=/shared/overlay
    is a subdirectory this script creates, with no fsGroup set on the pod)."""
    dest = tmp_path / "overlay"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    _serve(monkeypatch, _tar({"a.sql": b"select 1"}))

    with _restrictive_umask():
        overlay_fetcher.main()

    assert (dest / "a.sql").read_bytes() == b"select 1"
    assert stat.S_IMODE(os.stat(dest / "a.sql").st_mode) == 0o644
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o755


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


def _overlay_env(monkeypatch, tmp_path):
    """Point the fetcher at OVERLAY_DEST and OVERLAY_DELETE_LIST under tmp_path
    and return both paths."""
    dest = tmp_path / "overlay"
    delete_list = tmp_path / "overlay-delete-paths"
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/verify-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(dest))
    monkeypatch.setenv("OVERLAY_DELETE_LIST", str(delete_list))
    return dest, delete_list


def test_writes_the_delete_list_in_order_without_repeats_or_blank_lines(tmp_path, monkeypatch):
    dest, delete_list = _overlay_env(monkeypatch, tmp_path)
    _serve(monkeypatch, _tar({
        "models/table_b_v2.sql": b"select 2",
        overlay_fetcher.DELETE_LIST_MEMBER:
            b"models/table_b.sql\n\n   \nseeds/z.csv\n./models/table_b.sql\nmodels/with space.sql",
    }))

    with _restrictive_umask():
        overlay_fetcher.main()

    assert delete_list.read_bytes() == b"models/table_b.sql\nseeds/z.csv\nmodels/with space.sql\n"
    assert stat.S_IMODE(os.stat(delete_list).st_mode) == 0o644
    assert (dest / "models" / "table_b_v2.sql").read_bytes() == b"select 2"


def test_never_extracts_the_delete_list_member(tmp_path, monkeypatch):
    dest, _delete_list = _overlay_env(monkeypatch, tmp_path)
    _serve(monkeypatch, _tar({
        "models/table_b_v2.sql": b"select 2",
        overlay_fetcher.DELETE_LIST_MEMBER: b"models/table_b.sql\n",
    }))

    overlay_fetcher.main()

    assert not (dest / ".continuo").exists()
    assert sorted(p.relative_to(dest).as_posix() for p in dest.rglob("*") if p.is_file()) == [
        "models/table_b_v2.sql",
    ]


@pytest.mark.parametrize("listed", ["../escape.sql", "/abs/escape.sql", "models/../../escape.sql"])
def test_rejects_a_delete_path_outside_the_project(tmp_path, monkeypatch, listed):
    dest, delete_list = _overlay_env(monkeypatch, tmp_path)
    _serve(monkeypatch, _tar({
        "models/a.sql": b"select 1",
        overlay_fetcher.DELETE_LIST_MEMBER: f"models/b.sql\n{listed}\n".encode(),
    }))

    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == overlay_fetcher.EXIT_UNSAFE
    assert not delete_list.exists()
    assert not (dest / "models" / "a.sql").exists(), "nothing is extracted from a refused overlay"


@pytest.mark.parametrize("listed", ["models/a.sql", "./models//a.sql", "models", "models/nested"])
def test_rejects_a_delete_path_the_overlay_also_writes(tmp_path, monkeypatch, listed):
    """A listed path the overlay writes, as a file or as a directory holding
    its files, cannot also be absent from the staged project."""
    dest, delete_list = _overlay_env(monkeypatch, tmp_path)
    _serve(monkeypatch, _tar({
        "models/a.sql": b"select 1",
        "models/nested/b.sql": b"select 2",
        overlay_fetcher.DELETE_LIST_MEMBER: f"{listed}\n".encode(),
    }))

    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == overlay_fetcher.EXIT_UNSAFE
    assert not delete_list.exists()
    assert not (dest / "models" / "a.sql").exists(), "nothing is extracted from a refused overlay"


@pytest.mark.parametrize("value", [None, ""])
def test_a_delete_list_without_overlay_delete_list_exits_2(tmp_path, monkeypatch, value):
    dest, _delete_list = _overlay_env(monkeypatch, tmp_path)
    if value is None:
        monkeypatch.delenv("OVERLAY_DELETE_LIST")
    else:
        monkeypatch.setenv("OVERLAY_DELETE_LIST", value)
    _serve(monkeypatch, _tar({
        "models/table_b_v2.sql": b"select 2",
        overlay_fetcher.DELETE_LIST_MEMBER: b"models/table_b.sql\n",
    }))

    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == overlay_fetcher.EXIT_CONFIG
    assert not (dest / "models" / "table_b_v2.sql").exists(), "nothing is extracted when the list has nowhere to go"


@pytest.mark.parametrize("configured", [True, False])
def test_an_overlay_without_a_delete_list_writes_no_list(tmp_path, monkeypatch, configured):
    """A tarball without the reserved member needs no OVERLAY_DELETE_LIST and,
    when one is set, leaves its file unwritten."""
    dest, delete_list = _overlay_env(monkeypatch, tmp_path)
    if not configured:
        monkeypatch.delenv("OVERLAY_DELETE_LIST")
    _serve(monkeypatch, _tar({"models/a.sql": b"select 1"}))

    overlay_fetcher.main()

    assert (dest / "models" / "a.sql").read_bytes() == b"select 1"
    assert not delete_list.exists()


def test_fetch_failure_exits_3(tmp_path, monkeypatch):
    monkeypatch.setenv("SOURCE_OVERLAY_URI", "s3://continuo/svc/shadow-1/source-overlay.tar.gz")
    monkeypatch.setenv("OVERLAY_DEST", str(tmp_path / "overlay"))
    fake = mock.MagicMock()
    fake.get_object.side_effect = RuntimeError("boom")
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)
    with pytest.raises(SystemExit) as exc:
        overlay_fetcher.main()
    assert exc.value.code == 3
