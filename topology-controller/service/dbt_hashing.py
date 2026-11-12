"""The source, compiled and config parts of a dbt node's content_hash.

A dbt node's content_hash is `fold4(source, shared, config, compiled)`:

- source: a model's or snapshot's `raw_code`, normalised (line endings, outer
  whitespace); a test's compiled SQL, masked; a seed's raw file bytes, as the
  SHA-256 the compile extras recorded, else dbt's own checksum as written.
- compiled: a model's or snapshot's compiled SQL, masked; `""` for every other node.
- config: the SHA-256 of the RFC 8785 canonical JSON of `hashed_config`.

Masking replaces the values a compile writes that change on every run, or with the
target, rather than with the code: the manifest's `invocation_id`, its
`run_started_at` in the ISO "T" form and the space form, and the node's own
`database` as a whole identifier.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from continuo_contracts.canonical import canonical_json, normalise_text
from continuo_contracts.fingerprints import hashed_config_denylist

from domain.model import CompileExtras

_INVOCATION = "__CONTINUO_INVOCATION_ID__"
_RUN_STARTED = "__CONTINUO_RUN_STARTED_AT__"
_DATABASE = "__CONTINUO_DATABASE__"

_SOURCE_FROM_RAW_CODE = frozenset({"model", "snapshot"})


@dataclass(frozen=True)
class RunMask:
    """The values of one compile that a node's rendered text may carry."""
    invocation_id: str
    run_started_at: str

    @classmethod
    def from_metadata(cls, metadata: Mapping) -> RunMask:
        """The manifest metadata's `invocation_id` and `run_started_at`, "" when absent."""
        return cls(
            invocation_id=metadata.get("invocation_id") or "",
            run_started_at=metadata.get("run_started_at") or "",
        )


@dataclass(frozen=True)
class ManifestContext:
    """What hashing a node needs from the manifest and the compile around it."""
    run: RunMask
    extras: CompileExtras | None
    denylist: frozenset[str]

    @classmethod
    def from_manifest(cls, manifest: Mapping, extras: CompileExtras | None) -> ManifestContext:
        return cls(
            run=RunMask.from_metadata(manifest.get("metadata") or {}),
            extras=extras,
            denylist=hashed_config_denylist(),
        )


def mask_text(text: str, run: RunMask, database: str) -> str:
    """Replace the invocation id, both forms of the run start time and `database`
    as a whole identifier (never inside `warehouse_stage` or `my_warehouse`)."""
    if run.invocation_id:
        text = text.replace(run.invocation_id, _INVOCATION)
    if run.run_started_at:
        text = text.replace(run.run_started_at, _RUN_STARTED)
        text = text.replace(run.run_started_at.replace("T", " ", 1), _RUN_STARTED)
    if database:
        text = re.sub(rf"(?<![A-Za-z0-9_$]){re.escape(database)}(?![A-Za-z0-9_$])", _DATABASE, text)
    return text


def mask_value(value: Any, run: RunMask, database: str) -> Any:
    """`value` with `mask_text` applied to every string inside its dicts and lists."""
    if isinstance(value, str):
        return mask_text(value, run, database)
    if isinstance(value, Mapping):
        return {key: mask_value(item, run, database) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [mask_value(item, run, database) for item in value]
    return value


def drop_empty(value: Any) -> Any:
    """`value` with every `None`, `{}` and `[]` removed, at every depth (a container
    that holds only such values is removed too). `False`, `0` and `""` stay."""
    if isinstance(value, Mapping):
        kept = {key: drop_empty(item) for key, item in value.items()}
        return {key: item for key, item in kept.items() if not _empty(item)}
    if isinstance(value, list | tuple):
        kept_items = [drop_empty(item) for item in value]
        return [item for item in kept_items if not _empty(item)]
    return value


def _empty(value: Any) -> bool:
    return value is None or value == {} or value == []


def hashed_config(node: Mapping, ctx: ManifestContext) -> dict:
    """The node's config as the config part hashes it.

    The denylisted keys are dropped and the rendered values masked; a test's
    `severity` is lower-cased; `relation` carries the schema and alias the node
    writes, in their declared spelling; a model whose contract is enforced adds
    its declared columns in order and its model-level constraints. Nulls and
    empty containers are dropped last.
    """
    database = node.get("database") or ""
    raw_config = node.get("config") or {}
    config = mask_value({k: v for k, v in raw_config.items() if k not in ctx.denylist}, ctx.run, database)
    if node.get("resource_type") == "test" and isinstance(config.get("severity"), str):
        config["severity"] = config["severity"].lower()
    config["relation"] = {"schema": node.get("schema") or "", "alias": node.get("alias") or node.get("name") or ""}
    if (raw_config.get("contract") or {}).get("enforced") is True:
        config["columns"] = [
            {"name": c.get("name"), "data_type": c.get("data_type"),
             "constraints": c.get("constraints"), "quote": c.get("quote")}
            for c in (node.get("columns") or {}).values()
        ]
        config["constraints"] = node.get("constraints")
    return drop_empty(config)


def config_part(config: Mapping) -> str:
    """The SHA-256 of the canonical JSON of a hashed config."""
    return hashlib.sha256(canonical_json(config)).hexdigest()


def source_part(node: Mapping, ctx: ManifestContext) -> tuple[str, str, bool]:
    """The node's source part, the name of the input it hashes, and whether the
    part is dbt's `path` checksum (a seed over 1 MiB with no extras entry)."""
    resource_type = node.get("resource_type")
    if resource_type in _SOURCE_FROM_RAW_CODE:
        return _sha256(normalise_text(node.get("raw_code") or "")), "raw_code", False
    if resource_type == "seed":
        seed = ctx.extras.seeds.get(node.get("unique_id", "")) if ctx.extras is not None else None
        if seed is not None:
            return seed.sha256, "seed_bytes", False
        checksum = node.get("checksum") or {}
        return checksum.get("checksum") or "", "seed_checksum", checksum.get("name") == "path"
    code = node.get("compiled_code") or node.get("raw_code") or ""
    if not code:
        return hashlib.sha256(canonical_json(node)).hexdigest(), "compiled_code", False
    return _sha256(mask_text(code, ctx.run, node.get("database") or "")), "compiled_code", False


def compiled_part(node: Mapping, ctx: ManifestContext) -> str:
    """A model's or snapshot's masked compiled SQL, hashed; "" for every other node
    and for a model with no compiled SQL."""
    compiled = node.get("compiled_code") or ""
    if node.get("resource_type") not in _SOURCE_FROM_RAW_CODE or not compiled:
        return ""
    return _sha256(mask_text(compiled, ctx.run, node.get("database") or ""))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
