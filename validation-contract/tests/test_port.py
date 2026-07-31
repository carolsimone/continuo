"""Discovery tests for the WarehouseAdapter port.

These install REAL adapter packages (with genuine ``continuo_validation.adapters``
entry points) into throwaway virtualenvs and run ``discover_adapter`` against the
real ``importlib.metadata`` machinery — no monkeypatching of ``entry_points``, so the
tests exercise actual entry-point loading rather than an assumed return shape.
"""
import subprocess
import textwrap
from pathlib import Path

import pytest

from continuo_validation_contract import port
from continuo_validation_contract.port import WarehouseAdapter

CONTRACT_ROOT = Path(__file__).resolve().parent.parent  # validation-contract/


def test_warehouse_adapter_cannot_be_instantiated():
    """The port is abstract — a concrete adapter must implement every method."""
    with pytest.raises(TypeError):
        WarehouseAdapter()  # type: ignore[abstract]


def test_warehouse_adapter_requires_drop_schema():
    """drop_schema is part of the contract: omitting it blocks instantiation.

    A concrete adapter that implements every method except the teardown one must
    still be abstract, so the executor can rely on drop_schema existing on every
    installed engine.
    """

    class MissingDrop(WarehouseAdapter):
        @classmethod
        def required_env(cls):
            return []

        @classmethod
        def from_env(cls):
            return cls()

        def ensure_schema(self, schema):
            ...

        def build_empty_from_sql(self, schema, table, compiled_sql):
            ...

        def clone_empty_from_prod(self, candidate_schema, prod_schema, table):
            ...

        def close(self):
            ...

    with pytest.raises(TypeError, match="drop_schema"):
        MissingDrop()  # type: ignore[abstract]


def test_discover_adapter_raises_when_none_installed():
    """With no adapter package installed, discovery raises cleanly.

    The contract's own environment installs no ``continuo_validation.adapters``
    entry point, so this exercises the real zero-adapter path (no fakes).
    """
    with pytest.raises(port.AdapterDiscoveryError, match="no warehouse adapter installed"):
        port.discover_adapter()


# --- Real installed-adapter discovery via throwaway venvs --------------------

_FIXTURE_MODULE = '''\
from continuo_validation_contract.port import WarehouseAdapter


class OneAdapter(WarehouseAdapter):
    @classmethod
    def required_env(cls):
        return []

    @classmethod
    def from_env(cls):
        return cls()

    def ensure_schema(self, schema):
        ...

    def drop_schema(self, schema):
        ...

    def build_empty_from_sql(self, schema, table, compiled_sql):
        ...

    def clone_empty_from_prod(self, candidate_schema, prod_schema, table):
        ...

    def close(self):
        ...


class TwoAdapter(OneAdapter):
    ...


not_an_adapter = object()
'''

_DISCOVER_SCRIPT = textwrap.dedent(
    """
    from continuo_validation_contract.port import discover_adapter, AdapterDiscoveryError
    try:
        name, cls = discover_adapter()
        print("OK", name, cls.__name__)
    except AdapterDiscoveryError as exc:
        print("ERR", exc)
    """
)


def _run_discovery(tmp_path: Path, entry_points: dict, module_body: str = _FIXTURE_MODULE) -> str:
    """Run ``discover_adapter`` in a fresh venv holding the contract + a fixture adapter.

    Installs a generated fixture package declaring *entry_points* (an
    ``continuo_validation.adapters`` group) with *module_body* as its module source,
    and returns the discovery script's stdout.
    """
    fixture = tmp_path / "fixture"
    (fixture / "cvfixtureadapters").mkdir(parents=True)
    (fixture / "cvfixtureadapters" / "__init__.py").write_text(module_body)
    eps = "\n".join(f'{name} = "cvfixtureadapters:{target}"' for name, target in entry_points.items())
    (fixture / "pyproject.toml").write_text(
        textwrap.dedent(
            f"""
            [project]
            name = "cvfixtureadapters"
            version = "0.0.0"
            requires-python = ">= 3.14"

            [project.entry-points."continuo_validation.adapters"]
            {eps}

            [build-system]
            requires = ["hatchling"]
            build-backend = "hatchling.build"

            [tool.hatch.build.targets.wheel]
            packages = ["cvfixtureadapters"]
            """
        )
    )
    venv = tmp_path / "venv"
    subprocess.run(["uv", "venv", str(venv), "-p", "3.14"], check=True, capture_output=True)
    subprocess.run(
        ["uv", "pip", "install", "--python", str(venv), str(CONTRACT_ROOT), str(fixture)],
        check=True,
        capture_output=True,
    )
    py = venv / "bin" / "python"
    proc = subprocess.run([str(py), "-c", _DISCOVER_SCRIPT], capture_output=True, text=True)
    return proc.stdout


def test_discover_single_installed_adapter_is_found(tmp_path):
    """One installed adapter package → discovery returns its name and class."""
    out = _run_discovery(tmp_path, {"one": "OneAdapter"})
    assert out.strip() == "OK one OneAdapter"


def test_discover_multiple_installed_adapters_raises_naming_them(tmp_path):
    """Two installed adapters → discovery refuses and names both."""
    out = _run_discovery(tmp_path, {"one": "OneAdapter", "two": "TwoAdapter"})
    assert out.startswith("ERR")
    assert "one, two" in out


def test_discover_non_adapter_entry_point_raises(tmp_path):
    """An entry point that is not a WarehouseAdapter subclass is rejected."""
    out = _run_discovery(tmp_path, {"broken": "not_an_adapter"})
    assert out.startswith("ERR")
    assert "WarehouseAdapter subclass" in out


def test_discover_wraps_adapter_import_failure(tmp_path):
    """A failing adapter import surfaces as AdapterDiscoveryError, not a raw ImportError.

    That lets the runner still emit a structured result block for a malformed engine
    image rather than crashing with a traceback.
    """
    out = _run_discovery(
        tmp_path,
        {"one": "OneAdapter"},
        module_body="import definitely_missing_pkg_zzz  # noqa: F401\n",
    )
    assert out.startswith("ERR")
    assert "failed to load adapter entry point" in out


# --- RuntimeAdapter tests -------------------------------------------------------

from continuo_validation_contract.port import (
    RUNTIME_ENTRY_POINT_GROUP,
    RuntimeAdapter,
    discover_runtime_adapter,
)


def test_runtime_adapter_is_abstract():
    """The port is abstract — a concrete adapter must implement every method."""
    with pytest.raises(TypeError):
        RuntimeAdapter()  # type: ignore[abstract]


def test_runtime_entry_point_group_name():
    """The runtime entry point group constant has the correct value."""
    assert RUNTIME_ENTRY_POINT_GROUP == "continuo_runtime.adapters"


def test_discover_runtime_adapter_empty_group_raises():
    """With no runtime adapter package installed, discovery raises cleanly."""
    with pytest.raises(port.AdapterDiscoveryError, match="no runtime adapter installed"):
        discover_runtime_adapter()
