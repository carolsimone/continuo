from pathlib import Path
from unittest.mock import MagicMock, create_autospec
import pytest
from adapters.sources import ManifestSource
from adapters.filesystem.registry_repository import FilesystemRegistryRepository
from domain.model import ManifestFile
from service.manifest_handler import ManifestHandler

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_manifest_publisher():
    return MagicMock()


@pytest.fixture
def mock_registry_repo(tmp_path):
    return FilesystemRegistryRepository(str(tmp_path / "registry.csv"))


def _make_source(*entries: tuple[str, str]) -> ManifestSource:
    """entries: (fixture_filename, version) pairs"""
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(FIXTURES / name), version=version)
        for name, version in entries
    ]
    return source


def test_handle_publishes_all_nodes_across_manifests(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_publisher = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, manifest_publisher=mock_publisher, registry_repo=repo)
    handler.handle()

    mock_publisher.publish.assert_called_once()
    published_nodes = mock_publisher.publish.call_args[0][0]
    assert len(published_nodes) == 2


def test_handle_resolves_cross_service_deps(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_publisher = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, manifest_publisher=mock_publisher, registry_repo=repo)
    handler.handle()

    published_nodes = mock_publisher.publish.call_args[0][0]
    orders_node = next(n for n in published_nodes if n["table_name"] == "orders")
    assert len(orders_node["dependencies"]) == 1
    assert orders_node["dependencies"][0]["table_name"] == "users"
    assert orders_node["dependencies"][0]["service_name"] == "service-1"


def test_handle_persists_combined_registry(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_publisher = MagicMock()
    registry_path = str(tmp_path / "registry.csv")
    repo = FilesystemRegistryRepository(registry_path)

    handler = ManifestHandler(source=source, manifest_publisher=mock_publisher, registry_repo=repo)
    handler.handle()

    loaded = repo.load()
    names = {e.table_name for e in loaded.entries}
    assert names == {"users", "orders"}


def test_handle_does_nothing_when_no_manifests(tmp_path):
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    mock_publisher = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, manifest_publisher=mock_publisher, registry_repo=repo)
    schedule_names, manifest_versions = handler.handle()

    mock_publisher.publish.assert_not_called()
    assert schedule_names == []
    assert manifest_versions == {}


def test_handle_returns_distinct_schedule_names(mock_manifest_publisher, mock_registry_repo):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v5"),
    )
    handler = ManifestHandler(
        source=source,
        manifest_publisher=mock_manifest_publisher,
        registry_repo=mock_registry_repo,
    )
    schedule_names, _ = handler.handle()
    assert isinstance(schedule_names, list)
    assert len(schedule_names) == len(set(schedule_names))
    assert all(isinstance(n, str) and n for n in schedule_names)


def test_handle_returns_manifest_versions_map(mock_manifest_publisher, mock_registry_repo):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v5"),
    )
    handler = ManifestHandler(
        source=source,
        manifest_publisher=mock_manifest_publisher,
        registry_repo=mock_registry_repo,
    )
    _, manifest_versions = handler.handle()
    # manifest_service1.json has service_name "service-1" (from fqn)
    # manifest_service2.json has service_name "service-2" (from fqn)
    assert manifest_versions.get("service-1") == "v1"
    assert manifest_versions.get("service-2") == "v5"


def test_handle_publishes_correct_node_structure(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
    )
    mock_publisher = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, manifest_publisher=mock_publisher, registry_repo=repo)
    handler.handle()

    published_nodes = mock_publisher.publish.call_args[0][0]
    node = published_nodes[0]
    expected_keys = {
        "service_name", "schema_name", "table_name", "owner",
        "schedule_name", "criticality", "node_type",
        "manifest_version", "dependencies",
    }
    assert set(node.keys()) == expected_keys
