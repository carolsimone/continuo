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


def _run_discovery(tmp_path: Path, entry_points: dict) -> str:
    """Run ``discover_adapter`` in a fresh venv holding the contract + a fixture adapter.

    Installs a generated fixture package declaring *entry_points* (an
    ``continuo_validation.adapters`` group) and returns the discovery script's stdout.
    """
    fixture = tmp_path / "fixture"
    (fixture / "cvfixtureadapters").mkdir(parents=True)
    (fixture / "cvfixtureadapters" / "__init__.py").write_text(_FIXTURE_MODULE)
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
