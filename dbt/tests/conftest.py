import pytest

from dbt_upload import paths


@pytest.fixture
def build_root(tmp_path, monkeypatch):
    """A build root under tmp_path, standing in for the image's /app/build: each
    service's compile output goes to build_root/<service>."""
    root = tmp_path / "build"
    monkeypatch.setattr(paths, "BUILD_ROOT", str(root))
    return root
