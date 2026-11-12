"""Parses a python-service wire contract (contract_version 1) into ManifestNodes.

The artifact is one yaml document per service, produced by the domain repo's
CI: {contract_version: 1, service: <name>, nodes: [...]}. Every entry carries
CI-computed hash parts (source_hash, shared_code_hash, config_hash) and their
fold (content_hash). The parser keeps the CI source_hash and shared_code_hash,
which hash files that never reach topology-controller, and computes the config
part itself: the node's config_hash is the `projection_hash` of its code
projection (`continuo_contracts.python`), and its content_hash folds the two CI
parts with it. The CI config_hash and content_hash must be present but are not
trusted; a CI content_hash that differs from the computed one is logged.

Any malformed entry rejects the WHOLE artifact: the producing CI validates
before upload, so a bad entry here means a broken pipeline, and a silently
dropped node would retire it from production on promote. Schema evolution goes
through contract_version — incompatible schema changes require a
contract_version bump, so an unknown field is an error.
"""
import json
import logging

import yaml
from continuo_contracts.errors import ContractsError
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonNode, code_projection, projection_hash

from domain.exceptions import MalformedContractError
from domain.contract_vocabulary import NodeType
from domain.model import ManifestNode, ParsedArtifact, Runtime, SkippedNode
from domain.node_timeout import timeout_seconds
from domain.secret_ref import validate_secret_ref
from service.python_kind_rules import RULES, KindRules, resolve_kind

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
_OPTIONAL_ENTRY_KEYS = {
    "description", "extra_columns", "config", "kind", "secret_ref", "timeout_minutes",
    "mode_type", "primary_key", "assets",
}
_COLUMN_REQUIRED_KEYS = {"name", "type"}
_COLUMN_ALLOWED_KEYS = {"name", "type", "nullable"}


def _fail(detail: str) -> None:
    raise MalformedContractError(detail)


def _non_empty_str(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} must be a non-empty string, got {value!r}")
    return value


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


def _parse_mode_type(entry: dict, rules: KindRules, node_type: NodeType, label: str) -> str:
    mode_type = entry.get("mode_type", "replace")
    if not isinstance(mode_type, str) or mode_type not in rules.mode_types:
        _fail(
            f"{label}: mode_type must be one of {sorted(rules.mode_types)}"
            f" for kind {node_type}, got {mode_type!r}"
        )
    return mode_type


def _parse_primary_key(entry: dict, mode_type: str, columns: list[dict], label: str) -> list[str]:
    """The upsert key: required for upsert and refused for every other mode.

    Each name is spelled exactly as one of the output columns, and no name is
    repeated when names are compared case-insensitively, as the runtime's
    loader checks it.
    """
    if mode_type != "upsert":
        if "primary_key" in entry:
            _fail(f"{label}: primary_key is only allowed when mode_type is upsert (got {mode_type})")
        return []
    if "primary_key" not in entry:
        _fail(f"{label}: primary_key is required when mode_type is upsert")
    raw_key = entry["primary_key"]
    if not isinstance(raw_key, list) or not raw_key:
        _fail(f"{label}: primary_key must be a non-empty list of output column names")
    column_names = {column["name"] for column in columns}
    seen: set[str] = set()
    for name in raw_key:
        _non_empty_str(name, f"{label}: primary_key entry")
        if name.lower() in seen:
            _fail(
                f"{label}: primary_key names {name!r} more than once"
                " (names are compared case-insensitively)"
            )
        seen.add(name.lower())
        if name not in column_names:
            _fail(f"{label}: primary_key column {name!r} is not in output_columns")
    return list(raw_key)


def _parse_assets(entry: dict, rules: KindRules, node_type: NodeType, label: str) -> list[str]:
    """The files the node's script reads: distinct non-empty paths."""
    if "assets" not in entry:
        return []
    if not rules.assets_allowed:
        _fail(f"{label}: assets is not allowed for kind {node_type}")
    raw_assets = entry["assets"]
    if not isinstance(raw_assets, list):
        _fail(f"{label}: assets must be a list of paths")
    for path in raw_assets:
        _non_empty_str(path, f"{label}: assets entry")
    if len(set(raw_assets)) != len(raw_assets):
        _fail(f"{label}: assets must not list a path twice")
    return list(raw_assets)


def _project(projection_input: dict) -> tuple[str, str]:
    """The code projection of a validated entry as pretty JSON text, and its hash.

    A node the library refuses (a type outside its grammar, a config value
    canonical JSON cannot hold) is a malformed contract.
    """
    try:
        node = PythonNode.from_mapping(projection_input)
        projection = code_projection(node)
        config_hash = projection_hash(node)
    except ContractsError as error:
        raise MalformedContractError(str(error)) from error
    return json.dumps(projection, sort_keys=True, indent=2), config_hash


def _parse_entry(
    entry, service: str, image_tag: str
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

    node_type = resolve_kind(entry.get("kind"), label)
    rules = RULES[node_type]

    if rules.script_required:
        required = _REQUIRED_ENTRY_KEYS
    else:
        if entry.get("script"):
            _fail(f"{label}: 'script' is forbidden for kind {node_type}")
        required = _REQUIRED_ENTRY_KEYS - {"script"}
    missing = required - set(entry)
    if missing:
        _fail(f"{label}: missing required fields {sorted(missing)}")

    schema = _non_empty_str(entry["schema"], f"{label}: schema")
    table = _non_empty_str(entry["table"], f"{label}: table")
    owner = _non_empty_str(entry["owner"], f"{label}: owner")
    schedule = _non_empty_str(entry["schedule"], f"{label}: schedule")
    script = _non_empty_str(entry["script"], f"{label}: script") if rules.script_required else ""

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

    secret_ref = ""
    if "secret_ref" in entry:
        if not rules.secret_allowed:
            _fail(f"{label}: secret_ref is not allowed for kind {node_type}")
        secret_ref = validate_secret_ref(entry["secret_ref"], label)

    try:
        task_timeout_seconds = timeout_seconds(entry.get("timeout_minutes"))
    except ValueError as exc:
        _fail(f"{label}: timeout_minutes {exc}")

    parsed = rules.reads.parse(entry["reads"], label)
    columns = _parse_columns(entry["output_columns"], label)
    mode_type = _parse_mode_type(entry, rules, node_type, label)
    primary_key = _parse_primary_key(entry, mode_type, columns, label)
    assets = _parse_assets(entry, rules, node_type, label)

    source_hash = _non_empty_str(entry["source_hash"], f"{label}: source_hash")
    _non_empty_str(entry["config_hash"], f"{label}: config_hash")
    ci_content_hash = _non_empty_str(entry["content_hash"], f"{label}: content_hash")
    shared_code_hash = entry["shared_code_hash"]
    if not isinstance(shared_code_hash, str):
        _fail(
            f"{label}: shared_code_hash must be a string"
            ' ("" when the script has no in-repo imports)'
        )

    projection, config_hash = _project({
        "schema": schema,
        "table": table,
        "kind": str(node_type),
        "script": script,
        "reads": dict(parsed.reads),
        "output_columns": columns,
        "extra_columns": extra_columns,
        "config": config,
        "secret_ref": secret_ref,
        "mode_type": mode_type,
        "primary_key": primary_key,
        "assets": assets,
    })
    content_hash = fold3(source_hash, shared_code_hash, config_hash)
    if ci_content_hash != content_hash:
        logger.warning(
            "python contract: the producer CI's content_hash differs from the one"
            " topology-controller computes; expected until the service's rebaseline"
            " release on continuo-python-runtime 0.12.0",
            extra={
                "node_id": f"{schema}.{table}".lower(),
                "service": service,
                "ci_content_hash": ci_content_hash,
                "content_hash": content_hash,
            },
        )

    # The contract entry the code bundle's read set names (docs/read-contract.md):
    # the normalized entry, hash fields excluded, serialized deterministically for
    # the code bundle and the remediation LLM. The hashed code projection rides
    # beside it in `projection`.
    raw_entry = {
        "schema": schema,
        "table": table,
        "owner": owner,
        "schedule": schedule,
        "criticality": criticality,
        "kind": str(node_type),
        "description": description,
        "extra_columns": extra_columns,
        "reads": dict(parsed.reads),
        "output_columns": columns,
        "config": config,
    }
    if rules.script_required:
        raw_entry["script"] = script
    if secret_ref:
        raw_entry["secret_ref"] = secret_ref

    raw_code = json.dumps(raw_entry, sort_keys=True, indent=2)

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
        dependency_sqls=parsed.dependency_sqls,
        candidate_sql="",
        node_type=node_type,
        content_hash=content_hash,
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
        csv_source=parsed.csv_source,
        secret_ref=secret_ref,
        timeout_seconds=task_timeout_seconds,
        declared_columns=len(columns),
        projection=projection,
    )


def parse_python_contract(
    contract_path: str, image_tag: str = "", *, skipped: list[SkippedNode] | None = None
) -> ParsedArtifact:
    """Parse a python contract into its nodes. A python service has no shared code.

    `skipped` is accepted so every parse function takes the same arguments and
    is never appended to: a python contract skips no node, a malformed entry
    fails the whole artifact.
    """
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
        node = _parse_entry(entry, service, image_tag)
        key = (node.schema_name.lower(), node.table_name.lower())
        if key in seen:
            _fail(f"duplicate node {node.schema_name}.{node.table_name}")
        seen.add(key)
        nodes.append(node)
    return ParsedArtifact(nodes=nodes, shared_code={})
