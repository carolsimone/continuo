import importlib.metadata
from pathlib import Path

SERVICE_ROOT = Path(__file__).parent.parent


def test_continuo_contracts_is_pinned_exactly():
    assert '"continuo-contracts==0.1.0"' in (SERVICE_ROOT / "pyproject.toml").read_text()


def test_installed_library_is_the_pinned_version():
    assert importlib.metadata.version("continuo-contracts") == "0.1.0"
