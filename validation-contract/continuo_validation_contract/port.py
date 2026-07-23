"""Warehouse adapter port and entry-point discovery.

Validation builds each node as an EMPTY table in the candidate schema; the DDL is
engine-specific and lives behind the WarehouseAdapter port. Engine packages register
a ``continuo_validation.adapters`` entry point; each runner image installs exactly
one, so discovery — not configuration — selects the adapter.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from importlib.metadata import entry_points

ENTRY_POINT_GROUP = "continuo_validation.adapters"


class AdapterDiscoveryError(Exception):
    """Installed adapter plugins do not resolve to exactly one usable engine."""


class WarehouseAdapter(ABC):
    """Port for engine-specific empty-table DDL during blue/green validation.

    stdout is a parsed channel: the runner prints one sentinel-framed result block
    (see ``result.py``) as its last stdout line. Adapters must log diagnostics via
    the stdlib ``logging`` module (captured from the pod log) rather than printing
    them, must never write to stdout, and must never emit the
    ``===CONTINUO_VALIDATION_RESULT_BEGIN/END===`` marker strings themselves.
    """

    @classmethod
    @abstractmethod
    def required_env(cls) -> list[str]:
        """Names of env vars that must be non-empty before connecting."""

    @classmethod
    @abstractmethod
    def from_env(cls) -> "WarehouseAdapter":
        """Construct a connected adapter from environment variables."""

    @abstractmethod
    def ensure_schema(self, schema: str) -> None:
        """Idempotently create *schema*; safe under concurrent callers."""

    @abstractmethod
    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None:
        """Create ``schema.table`` empty, shaped by the compiled SELECT."""

    @abstractmethod
    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None:
        """Create ``candidate_schema.table`` empty, shaped like ``prod_schema.table``."""

    @abstractmethod
    def close(self) -> None:
        """Release the underlying connection."""


def discover_adapter() -> tuple[str, type[WarehouseAdapter]]:
    """Return ``(engine_name, adapter_class)`` from the single installed plugin.

    Raises
    ------
    AdapterDiscoveryError
        If zero or multiple adapters are installed, or the entry point does not
        resolve to a WarehouseAdapter subclass.
    """
    eps = list(entry_points(group=ENTRY_POINT_GROUP))
    if not eps:
        raise AdapterDiscoveryError(
            f"no warehouse adapter installed (entry-point group {ENTRY_POINT_GROUP!r} is empty); "
            "install exactly one continuo-validation-<engine> package"
        )
    if len(eps) > 1:
        names = ", ".join(sorted(ep.name for ep in eps))
        raise AdapterDiscoveryError(
            f"multiple warehouse adapters installed ({names}); "
            "a runner image must install exactly one"
        )
    ep = eps[0]
    cls = ep.load()
    if not (isinstance(cls, type) and issubclass(cls, WarehouseAdapter)):
        raise AdapterDiscoveryError(
            f"entry point {ep.name!r} does not resolve to a WarehouseAdapter subclass"
        )
    return ep.name, cls
