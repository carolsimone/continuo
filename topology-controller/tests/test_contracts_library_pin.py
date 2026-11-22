import importlib.metadata
import runpy
from pathlib import Path

import pytest
from continuo_contracts import dbt

from domain.model import SkipReason
from service import contracts_library

SERVICE_ROOT = Path(__file__).parent.parent


def test_continuo_contracts_is_pinned_exactly():
    assert '"continuo-contracts==0.4.1"' in (SERVICE_ROOT / "pyproject.toml").read_text()


def test_installed_library_is_the_pinned_version():
    assert importlib.metadata.version("continuo-contracts") == "0.4.1"


def test_the_domain_skip_reasons_are_the_librarys():
    assert tuple(reason.value for reason in SkipReason) == dbt.SKIP_REASONS


@pytest.mark.parametrize("text, triple", [
    ("0.2.0", (0, 2, 0)),
    ("10.20.30", (10, 20, 30)),
    ("0.2", None),
    ("0.2.0rc1", None),
    ("0.2.0.dev1", None),
    ("v0.2.0", None),
    ("0.-2.0", None),
    ("٠.٢.٠", None),
    ("1234567890.0.0", None),
    ("", None),
])
def test_a_version_is_three_whole_numbers_or_nothing(text, triple):
    assert contracts_library.version_triple(text) == triple


@pytest.mark.parametrize("installed", ["0.2", "0.2.0rc1", "0.2.0.dev1", "unknown", ""])
def test_an_install_whose_library_version_is_not_x_y_z_refuses_to_boot(monkeypatch, installed):
    monkeypatch.setattr(importlib.metadata, "version", lambda name: installed)
    with pytest.raises(RuntimeError, match=f"continuo-contracts version {installed!r} is not a version X.Y.Z"):
        runpy.run_path(contracts_library.__file__)


def test_an_install_whose_library_version_is_x_y_z_boots(monkeypatch):
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "0.4.1")
    assert runpy.run_path(contracts_library.__file__)["LIBRARY_VERSION"] == "0.4.1"
