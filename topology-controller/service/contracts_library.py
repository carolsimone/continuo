"""The continuo-contracts library this process runs on: its version.

The version is the one installed, which `pyproject.toml` pins exactly. A producer's bundle
names the version it was written with, and the parse refuses one newer than this. The
installed version is read once, when this module is imported, and an install whose version
is not three whole numbers `X.Y.Z` cannot be compared with a bundle's, so the import fails
and the process does not boot.
"""
import importlib.metadata
import re
from typing import Final


_VERSION = re.compile(r"[0-9]{1,9}\.[0-9]{1,9}\.[0-9]{1,9}")


def version_triple(text: str) -> tuple[int, int, int] | None:
    """The numbers of a version written X.Y.Z (ASCII digits, one to nine per part), or None
    for any other text."""
    if _VERSION.fullmatch(text) is None:
        return None
    major, minor, patch = (int(part) for part in text.split("."))
    return major, minor, patch


def _installed_version() -> str:
    version = importlib.metadata.version("continuo-contracts")
    if version_triple(version) is None:
        raise RuntimeError(
            f"the installed continuo-contracts version {version!r} is not a version X.Y.Z,"
            " so a producer's bundle cannot be compared with it"
        )
    return version


LIBRARY_VERSION: Final = _installed_version()
