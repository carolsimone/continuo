import os
import tempfile
import pytest
from domain.model import NodeRegistry, NodeRegistryEntry
from adapters.filesystem.registry_repository import FilesystemRegistryRepository


@pytest.fixture
def tmp_csv(tmp_path):
    return str(tmp_path / "registry.csv")


def test_save_and_load_roundtrip(tmp_csv):
    repo = FilesystemRegistryRepository(tmp_csv)
    registry = NodeRegistry(entries=[
        NodeRegistryEntry("orders", "public", "service-1", "data-platform"),
        NodeRegistryEntry("users", "public", "service-2", "core-team"),
    ])
    repo.save(registry)
    loaded = repo.load()
    assert len(loaded.entries) == 2
    names = {e.table_name for e in loaded.entries}
    assert names == {"orders", "users"}


def test_load_returns_empty_when_file_missing(tmp_csv):
    repo = FilesystemRegistryRepository(tmp_csv)
    loaded = repo.load()
    assert loaded.entries == []


def test_save_overwrites_existing(tmp_csv):
    repo = FilesystemRegistryRepository(tmp_csv)
    repo.save(NodeRegistry(entries=[
        NodeRegistryEntry("orders", "public", "service-1", "data-platform"),
    ]))
    repo.save(NodeRegistry(entries=[
        NodeRegistryEntry("users", "public", "service-2", "core-team"),
    ]))
    loaded = repo.load()
    assert len(loaded.entries) == 1
    assert loaded.entries[0].table_name == "users"


def test_save_creates_parent_directories(tmp_path):
    deep_path = str(tmp_path / "a" / "b" / "registry.csv")
    repo = FilesystemRegistryRepository(deep_path)
    registry = NodeRegistry(entries=[
        NodeRegistryEntry("orders", "public", "service-1", "data-platform"),
    ])
    repo.save(registry)
    assert os.path.exists(deep_path)
