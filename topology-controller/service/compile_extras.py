"""Loads and validates the compile-extras.json a dbt compile writes beside its manifest.

The object records what the manifest does not carry about a service: each seed's file
digest, CSV header and configured column types, the dbt_project.yml flags and the
versions of the installed dbt distributions. A document that is unreadable, speaks
another version or holds a value of the wrong shape is refused whole.
"""
import hashlib
import json
import re

from continuo_contracts.canonical import canonical_json

from domain.exceptions import InvalidCompileExtrasError
from domain.model import CompileExtras, SeedExtras

SCHEMA_VERSION = 1
_SHA256 = re.compile(r"[0-9a-f]{64}")


def load_compile_extras(path: str, expected_service: str) -> CompileExtras:
    """Read the extras object at `path`. When `expected_service` is non-empty the
    object must name that service. Raises InvalidCompileExtrasError otherwise."""
    try:
        with open(path, encoding="utf-8") as f:
            document = json.load(f)
    except (OSError, ValueError) as exc:
        raise InvalidCompileExtrasError(f"unreadable: {exc}") from exc

    if not isinstance(document, dict):
        raise InvalidCompileExtrasError(f"must be a JSON object, got {type(document).__name__}")
    version = document.get("version")
    if type(version) is not int or version != SCHEMA_VERSION:
        raise InvalidCompileExtrasError(f"unsupported version {version!r}, this parser reads version {SCHEMA_VERSION}")

    service = _string(document, "service")
    if expected_service and service != expected_service:
        raise InvalidCompileExtrasError(f"service {service!r} is not the declared service {expected_service!r}")
    return CompileExtras(
        service=service,
        release_id=_string(document, "release_id"),
        seeds=_seeds(_field(document, "seeds", dict)),
        project_flags=_field(document, "project_flags", dict),
        packages=_string_map(_field(document, "packages", dict), "packages"),
    )


def seed_columns_hash(seed: SeedExtras) -> str:
    """The fingerprint of a seed's columns: the SHA-256 of the canonical JSON (RFC 8785)
    of its header and column types, "sha256:"-prefixed. The file digest is left out."""
    columns = canonical_json({"column_types": seed.column_types, "header": list(seed.header)})
    return "sha256:" + hashlib.sha256(columns).hexdigest()


def _field(container: dict, key: str, kind: type):
    if key not in container:
        raise InvalidCompileExtrasError(f"missing {key!r}")
    value = container[key]
    if not isinstance(value, kind):
        raise InvalidCompileExtrasError(f"{key!r} must be a {kind.__name__}, got {type(value).__name__}")
    return value


def _string(container: dict, key: str) -> str:
    return _field(container, key, str)


def _string_map(value: dict, where: str) -> dict[str, str]:
    for key, item in value.items():
        if not isinstance(item, str):
            raise InvalidCompileExtrasError(f"{where}[{key!r}] must be a string, got {type(item).__name__}")
    return dict(value)


def _seeds(value: dict) -> dict[str, SeedExtras]:
    seeds = {}
    for unique_id, entry in value.items():
        where = f"seeds[{unique_id!r}]"
        if not isinstance(entry, dict):
            raise InvalidCompileExtrasError(f"{where} must be an object, got {type(entry).__name__}")
        sha256 = _string(entry, "sha256")
        if not _SHA256.fullmatch(sha256):
            raise InvalidCompileExtrasError(f"{where}.sha256 must be 64 lowercase hex characters")
        header = _field(entry, "header", list)
        if not all(isinstance(column, str) for column in header):
            raise InvalidCompileExtrasError(f"{where}.header must hold strings")
        seeds[unique_id] = SeedExtras(
            sha256=sha256,
            header=tuple(header),
            column_types=_string_map(_field(entry, "column_types", dict), f"{where}.column_types"),
        )
    return seeds
