"""Parses a python-service wire contract (contract_version 1) into ManifestNodes.

The artifact is one yaml document per service, produced by the domain repo's
CI: {contract_version: 1, service: <name>, nodes: [...]}. Every entry carries
CI-computed hash parts (source_hash, shared_code_hash, config_hash) and their
fold (content_hash); the fold is recomputed here and a mismatch rejects the
artifact. Any malformed entry rejects the WHOLE artifact: the producing CI
validates before upload, so a bad entry here means a broken pipeline, and a
silently dropped node would retire it from production on promote. Schema
evolution goes through contract_version — incompatible schema changes require
a contract_version bump, so an unknown field is an error.
"""
import json
import logging

import yaml

from domain.exceptions import MalformedContractError
from domain.model import ManifestNode, NodeType, Runtime
from service.content_hash import content_hash_fold

logger = logging.getLogger(__name__)

CONTRACT_VERSION = 1
CRITICALITIES = {"REGULATORY", "CORE", "SECONDARY"}
EXTRA_COLUMNS_POLICIES = {"raise", "warn"}

_TOP_LEVEL_KEYS = {"contract_version", "service", "nodes"}
_REQUIRED_ENTRY_KEYS = {
    "schema", "table", "owner", "schedule", "criticality", "script",
    "reads", "output_columns",
    "source_hash", "shared_code_hash", "config_hash", "content_hash",
}
_OPTIONAL_ENTRY_KEYS = {"description", "extra_columns", "config", "kind"}
_COLUMN_REQUIRED_KEYS = {"name", "type"}
_COLUMN_ALLOWED_KEYS = {"name", "type", "nullable"}
KINDS = {"python-model", "python-csv"}


def _fail(detail: str) -> None:
    raise MalformedContractError(detail)


def _validate_csv_uri(uri: str, label: str) -> None:
    """Mirrors continuo_python_runtime.csv_source.parse_csv_uri's grammar
    exactly, so a contract this loader accepts is one the pinned runner's
    validation header-fetch can actually parse. A prefix-only check would
    accept shapes the runner rejects (a bucket-less ``s3://bucket`` with no
    object key, a host-less ``https://``), letting topology-controller wave
    a malformed csv contract through a release that only fails once the
    validation Job actually runs. Accepts exactly ``s3://bucket/key`` (both
    non-empty) or ``https://<non-empty-host>[/...]``; every other shape —
    including ``http://`` — is rejected here, at parse time.
    """
    if uri.startswith("s3://"):
        bucket, _, key = uri[len("s3://"):].partition("/")
        if bucket and key:
            return
        _fail(f"{label}: invalid s3 csv uri (missing bucket or key): {uri!r}")
    elif uri.startswith("https://"):
        host, _, _ = uri[len("https://"):].partition("/")
        if host:
            return
        _fail(f"{label}: invalid https csv uri (missing host): {uri!r}")
    else:
        _fail(
            f"{label}: reads['csv'] must be an s3://bucket/key or"
            f" https://<host>/... uri, got {uri!r}"
        )


def _non_empty_str(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} must be a non-empty string, got {value!r}")
    return value


def _parse_reads(raw, label: str) -> dict[str, str]:
    if not isinstance(raw, dict):
        _fail(f"{label}: reads must be a mapping of read name -> SQL")
    reads: dict[str, str] = {}
    for name, sql in raw.items():
        _non_empty_str(name, f"{label}: read name")
        _non_empty_str(sql, f"{label}: reads[{name!r}]")
        reads[name] = sql
    return reads


def _parse_columns(raw, label: str) -> list[dict]:
    if not isinstance(raw, list) or not raw:
        _fail(f"{label}: output_columns must be a non-empty list")
    columns = []
    seen_names: set[str] = set()
    for i, col in enumerate(raw):
        col_label = f"{label}: output_columns[{i}]"
        if not isinstance(col, dict):
            _fail(f"{col_label} must be a mapping")
        unknown = set(col) - _COLUMN_ALLOWED_KEYS
        if unknown:
            _fail(f"{col_label} has unknown keys {sorted(unknown, key=str)}")
        missing = _COLUMN_REQUIRED_KEYS - set(col)
        if missing:
            _fail(f"{col_label} is missing {sorted(missing)}")
        name = _non_empty_str(col["name"], f"{col_label}.name")
        col_type = _non_empty_str(col["type"], f"{col_label}.type")
        nullable = col.get("nullable", True)
        if not isinstance(nullable, bool):
            _fail(f"{col_label}.nullable must be a bool, got {nullable!r}")
        name_key = name.lower()
        if name_key in seen_names:
            _fail(f"{label}: duplicate column name {name!r}")
        seen_names.add(name_key)
        columns.append({"name": name, "type": col_type, "nullable": nullable})
    return columns


def _parse_entry(
    entry, service: str, manifest_version: str, image_tag: str
) -> ManifestNode:
    if not isinstance(entry, dict):
        _fail(f"node entry must be a mapping, got {type(entry).__name__}")
    label = f"{entry.get('schema', '?')}.{entry.get('table', '?')}"

    unknown = set(entry) - _REQUIRED_ENTRY_KEYS - _OPTIONAL_ENTRY_KEYS
    if unknown:
        _fail(
            f"{label}: unknown fields {sorted(unknown, key=str)}"
            " — incompatible schema changes require a contract_version bump"
        )

    kind = entry.get("kind", "python-model")
    if not isinstance(kind, str) or kind not in KINDS:
        _fail(f"{label}: kind must be one of {sorted(KINDS)}, got {kind!r}")

    if kind == "python-csv":
        if entry.get("script"):
            _fail(f"{label}: 'script' is forbidden for kind python-csv")
        required = _REQUIRED_ENTRY_KEYS - {"script"}
    else:
        required = _REQUIRED_ENTRY_KEYS
    missing = required - set(entry)
    if missing:
        _fail(f"{label}: missing required fields {sorted(missing)}")

    schema = _non_empty_str(entry["schema"], f"{label}: schema")
    table = _non_empty_str(entry["table"], f"{label}: table")
    owner = _non_empty_str(entry["owner"], f"{label}: owner")
    schedule = _non_empty_str(entry["schedule"], f"{label}: schedule")
    script = _non_empty_str(entry["script"], f"{label}: script") if kind != "python-csv" else ""

    criticality = entry["criticality"]
    if not isinstance(criticality, str) or criticality not in CRITICALITIES:
        _fail(
            f"{label}: criticality must be one of {sorted(CRITICALITIES)},"
            f" got {criticality!r}"
        )

    extra_columns = entry.get("extra_columns", "raise")
    if not isinstance(extra_columns, str) or extra_columns not in EXTRA_COLUMNS_POLICIES:
        _fail(
            f"{label}: extra_columns must be one of"
            f" {sorted(EXTRA_COLUMNS_POLICIES)}, got {extra_columns!r}"
        )

    config = entry.get("config", {})
    if not isinstance(config, dict):
        _fail(f"{label}: config must be a mapping")

    description = entry.get("description", "")
    if not isinstance(description, str):
        _fail(f"{label}: description must be a string")

    reads = _parse_reads(entry["reads"], label)
    if kind == "python-csv":
        if set(reads) != {"csv"}:
            _fail(f"{label}: a python-csv node's reads must be exactly {{csv: <uri>}}")
        csv_source = reads["csv"]
        _validate_csv_uri(csv_source, label)
    else:
        csv_source = ""
    columns = _parse_columns(entry["output_columns"], label)

    source_hash = _non_empty_str(entry["source_hash"], f"{label}: source_hash")
    config_hash = _non_empty_str(entry["config_hash"], f"{label}: config_hash")
    content_hash = _non_empty_str(entry["content_hash"], f"{label}: content_hash")
    shared_code_hash = entry["shared_code_hash"]
    if not isinstance(shared_code_hash, str):
        _fail(
            f"{label}: shared_code_hash must be a string"
            ' ("" when the script has no in-repo imports)'
        )

    expected = content_hash_fold(source_hash, shared_code_hash, config_hash)
    if content_hash != expected:
        _fail(
            f"{label}: content_hash does not equal the fold of its parts"
            " — the producing CI's hasher is broken or the artifact was altered"
        )

    # The node's source as the control plane sees it (the script itself never
    # reaches topology-controller): the normalized entry, hash fields excluded,
    # serialized deterministically for the code bundle and the remediation LLM.
    raw_entry = {
        "schema": schema,
        "table": table,
        "owner": owner,
        "schedule": schedule,
        "criticality": criticality,
        "kind": kind,
        "description": description,
        "extra_columns": extra_columns,
        "reads": dict(reads),
        "output_columns": columns,
        "config": config,
    }
    if kind != "python-csv":
        raw_entry["script"] = script

    try:
        raw_code = json.dumps(raw_entry, sort_keys=True, indent=2)
    except (TypeError, ValueError):
        _fail(f"{label}: config is not JSON-serializable")

    node_type = NodeType.PYTHON_CSV if kind == "python-csv" else NodeType.PYTHON_MODEL
    dependency_sqls = [] if kind == "python-csv" else [reads[name] for name in sorted(reads)]

    return ManifestNode(
        table_name=table,
        schema_name=schema,
        # A python contract has no alias concept: the relation it writes is
        # always the declared table.
        resolved_relation=table,
        service_name=service,
        owner=owner,
        schedule_name=schedule,
        criticality=criticality,
        dependency_sqls=dependency_sqls,
        candidate_sql="",
        node_type=node_type,
        content_hash=content_hash,
        manifest_version=manifest_version,
        image_tag=image_tag,
        original_file_path=script,
        raw_code=raw_code,
        config=config,
        source_hash=source_hash,
        shared_code_hash=shared_code_hash,
        config_hash=config_hash,
        code_unit_ids=[],
        output_columns=columns,
        runtime=Runtime.PYTHON,
        csv_source=csv_source,
    )


def parse_python_contract(
    contract_path: str, manifest_version: str, image_tag: str = ""
) -> tuple[list[ManifestNode], dict]:
    with open(contract_path) as f:
        try:
            doc = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise MalformedContractError(f"invalid yaml: {exc}") from exc

    if not isinstance(doc, dict):
        _fail(f"contract document must be a mapping, got {type(doc).__name__}")
    if "contract_version" in doc and (
        type(doc["contract_version"]) is not int
        or doc["contract_version"] != CONTRACT_VERSION
    ):
        _fail(
            f"unsupported contract_version {doc['contract_version']!r}"
            f" — this parser speaks version {CONTRACT_VERSION}"
        )
    unknown = set(doc) - _TOP_LEVEL_KEYS
    if unknown:
        _fail(f"unknown top-level fields {sorted(unknown, key=str)}")
    missing = _TOP_LEVEL_KEYS - set(doc)
    if missing:
        _fail(f"missing top-level fields {sorted(missing)}")
    service = _non_empty_str(doc["service"], "service")
    if not isinstance(doc["nodes"], list):
        _fail("nodes must be a list")

    nodes: list[ManifestNode] = []
    seen: set[tuple[str, str]] = set()
    for entry in doc["nodes"]:
        node = _parse_entry(entry, service, manifest_version, image_tag)
        key = (node.schema_name.lower(), node.table_name.lower())
        if key in seen:
            _fail(f"duplicate node {node.schema_name}.{node.table_name}")
        seen.add(key)
        nodes.append(node)
    return nodes, {}
