from pathlib import Path
from unittest.mock import MagicMock, create_autospec
import pytest
from adapters.sources import ManifestSource
from adapters.filesystem.registry_repository import FilesystemRegistryRepository
from domain.model import ManifestFile
from service.manifest_handler import ManifestHandler

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_graph_client():
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


def test_handle_loads_all_nodes_across_manifests(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_graph = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, graph_client=mock_graph, registry_repo=repo)
    handler.handle()

    assert mock_graph.create_node.call_count == 2


def test_handle_resolves_cross_service_deps(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_graph = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, graph_client=mock_graph, registry_repo=repo)
    handler.handle()

    calls = mock_graph.create_node.call_args_list
    orders_call = next(c for c in calls if c[0][0].table_name == "orders")
    assert len(orders_call[0][0].upstream_deps) == 1
    assert orders_call[0][0].upstream_deps[0].table_name == "users"
    assert orders_call[0][0].upstream_deps[0].service_name == "service-1"


def test_handle_persists_combined_registry(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_graph = MagicMock()
    registry_path = str(tmp_path / "registry.csv")
    repo = FilesystemRegistryRepository(registry_path)

    handler = ManifestHandler(source=source, graph_client=mock_graph, registry_repo=repo)
    handler.handle()

    loaded = repo.load()
    names = {e.table_name for e in loaded.entries}
    assert names == {"users", "orders"}


def test_handle_does_nothing_when_no_manifests(tmp_path):
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    mock_graph = MagicMock()
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, graph_client=mock_graph, registry_repo=repo)
    schedule_names, manifest_versions = handler.handle()

    mock_graph.create_node.assert_not_called()
    assert schedule_names == []
    assert manifest_versions == {}


def test_handle_continues_on_graph_error(tmp_path):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    mock_graph = MagicMock()
    mock_graph.create_node.side_effect = [Exception("gRPC error"), None]
    repo = FilesystemRegistryRepository(str(tmp_path / "registry.csv"))

    handler = ManifestHandler(source=source, graph_client=mock_graph, registry_repo=repo)
    handler.handle()

    assert mock_graph.create_node.call_count == 2


def test_handle_returns_distinct_schedule_names(mock_graph_client, mock_registry_repo):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v5"),
    )
    handler = ManifestHandler(
        source=source,
        graph_client=mock_graph_client,
        registry_repo=mock_registry_repo,
    )
    schedule_names, _ = handler.handle()
    assert isinstance(schedule_names, list)
    assert len(schedule_names) == len(set(schedule_names))
    assert all(isinstance(n, str) and n for n in schedule_names)


def test_handle_returns_manifest_versions_map(mock_graph_client, mock_registry_repo):
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v5"),
    )
    handler = ManifestHandler(
        source=source,
        graph_client=mock_graph_client,
        registry_repo=mock_registry_repo,
    )
    _, manifest_versions = handler.handle()
    # manifest_service1.json has service_name "service-1" (from fqn)
    # manifest_service2.json has service_name "service-2" (from fqn)
    assert manifest_versions.get("service-1") == "v1"
    assert manifest_versions.get("service-2") == "v5"
