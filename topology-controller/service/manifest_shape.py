"""The shape of the manifest fields that the hashing and the parse read without the contracts library.

dbt writes each of these fields as text, an object or a list. The library refuses a field it reads in
another shape (`DbtNodeError`). The hashing and the parse read a few more fields themselves: the fields
of a node, and the entries of the manifest's `macros`, `functions` and `unit_tests`. A value of another
shape there fails deep in the fold with an `AttributeError`, which the handler would retry instead of
rejecting. `refusal` names the first such field in the library's words, so the release is an invalid
artifact. A null node field stands for an empty value, as everywhere the parse reads these fields.
"""
from collections.abc import Mapping
from typing import Any

from continuo_contracts import dbt

# The nodes the hashing and the parse read: the ones that become nodes, the tests attached to them, and
# the on-run hooks (operation nodes) whose code every build folds. Any other kind of node is never read.
# A tuple, not a set: a `resource_type` of another shape (a list) must compare, not raise.
_READ_RESOURCE_TYPES = (*dbt.RESOURCE_TYPES, "test", "operation")

_STRING = "a string"
_MAPPING = "a mapping"
_STRINGS = "a list of strings"

# Only the fields whose wrong shape fails the fold: the others (`config`, `columns`, `tags`, ...) are
# read tolerantly, or by the library, which refuses them as a failed contract.
_NODE_FIELDS: tuple[tuple[str, str], ...] = (
    ("raw_code", _STRING),
    ("compiled_code", _STRING),
    ("database", _STRING),
    ("depends_on", _MAPPING),
)
_DEPENDS_ON_FIELDS: tuple[str, ...] = ("macros", "nodes")

# The collections of the manifest the hashing reads entry by entry: a macro's code and its calls, the
# functions a node reaches, and the unit tests of a node. Each is an object of objects (null reads as none).
_ENTRY_COLLECTIONS: tuple[str, ...] = ("macros", "functions", "unit_tests")


def _is(kind: str, value: Any) -> bool:
    if kind == _STRING:
        return isinstance(value, str)
    if kind == _MAPPING:
        return isinstance(value, Mapping)
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _wrong(where: str, kind: str, value: Any) -> str:
    return f"manifest: {where} must be {kind}, got {type(value).__name__}"


def refusal(manifest: Mapping[str, Any]) -> str | None:
    """The reason `manifest` cannot be read, or None when everything the parse reads has the shape it
    expects: `nodes` is an object, `macros`, `functions` and `unit_tests` are objects of objects, and the
    first node the parse reads (in manifest order) has fields of the right shape. The reason starts with
    `manifest`, as the library's refusal of a manifest field does, and names the field. A manifest with
    no `nodes` key raises KeyError, as the parse's own read of it does."""
    nodes = manifest["nodes"]
    if not isinstance(nodes, Mapping):
        return _wrong("nodes", _MAPPING, nodes)
    for collection in _ENTRY_COLLECTIONS:
        entries = manifest.get(collection)
        if entries is None:
            continue
        if not isinstance(entries, Mapping):
            return _wrong(collection, _MAPPING, entries)
        for key, entry in entries.items():
            if not isinstance(entry, Mapping):
                return _wrong(f"{collection}.{key}", _MAPPING, entry)
    for node_id, node in nodes.items():
        if not isinstance(node, Mapping):
            return _wrong(f"nodes.{node_id}", _MAPPING, node)
        if node.get("resource_type") not in _READ_RESOURCE_TYPES:
            continue
        for field, kind in _NODE_FIELDS:
            value = node.get(field)
            if value is not None and not _is(kind, value):
                return _wrong(f"nodes.{node_id}.{field}", kind, value)
        depends_on = node.get("depends_on") or {}
        for field in _DEPENDS_ON_FIELDS:
            value = depends_on.get(field)
            if value is not None and not _is(_STRINGS, value):
                return _wrong(f"nodes.{node_id}.depends_on.{field}", _STRINGS, value)
    return None
