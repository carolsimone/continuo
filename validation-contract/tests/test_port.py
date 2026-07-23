"""Unit tests for the WarehouseAdapter port and entry-point discovery."""
import pytest

from continuo_validation_contract import port


class _GoodAdapter(port.WarehouseAdapter):
    """Minimal concrete adapter used to exercise discovery."""

    @classmethod
    def required_env(cls) -> list[str]:
        return []

    @classmethod
    def from_env(cls) -> "port.WarehouseAdapter":
        return cls()

    def ensure_schema(self, schema: str) -> None:
        pass

    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None:
        pass

    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None:
        pass

    def close(self) -> None:
        pass


class _FakeEntryPoint:
    """Stands in for importlib.metadata.EntryPoint: has .name and .load()."""

    def __init__(self, name: str, obj: object) -> None:
        self.name = name
        self._obj = obj

    def load(self) -> object:
        return self._obj


def test_discover_zero_adapters_raises(monkeypatch):
    """Test that discovery raises when no adapters are installed."""
    monkeypatch.setattr(port, "entry_points", lambda group: [])
    with pytest.raises(port.AdapterDiscoveryError, match="no warehouse adapter installed"):
        port.discover_adapter()


def test_discover_multiple_adapters_raises_with_names(monkeypatch):
    """Test that discovery raises when multiple adapters are installed, listing names."""
    eps = [_FakeEntryPoint("postgres", _GoodAdapter), _FakeEntryPoint("snowflake", _GoodAdapter)]
    monkeypatch.setattr(port, "entry_points", lambda group: eps)
    with pytest.raises(port.AdapterDiscoveryError, match="postgres, snowflake"):
        port.discover_adapter()


def test_discover_single_adapter_returns_name_and_class(monkeypatch):
    """Test that discovery returns engine name and adapter class for a single adapter."""
    monkeypatch.setattr(
        port, "entry_points", lambda group: [_FakeEntryPoint("postgres", _GoodAdapter)]
    )
    engine, cls = port.discover_adapter()
    assert engine == "postgres"
    assert cls is _GoodAdapter


def test_discover_rejects_non_adapter_entry_point(monkeypatch):
    """Test that discovery raises when entry point is not a WarehouseAdapter subclass."""
    monkeypatch.setattr(
        port, "entry_points", lambda group: [_FakeEntryPoint("broken", object)]
    )
    with pytest.raises(port.AdapterDiscoveryError, match="WarehouseAdapter subclass"):
        port.discover_adapter()
