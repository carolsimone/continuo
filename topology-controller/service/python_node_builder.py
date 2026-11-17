"""Builds the ManifestNode of a Python node from its validated fields.

Every reader of a Python contract (the wire parser of `contract.yaml`, the ODCS
parser of `odcs-bundle.json`) checks its own document and hands the node's
fields to `build_python_node`, so one rule decides how the node is projected,
hashed and described whatever document declared it:

- the code projection (`continuo_contracts.python`) is the node's hashed config
  part, and its pretty JSON text rides on the node as `projection`;
- `content_hash` folds the producer's source hash and shared-code hash with that
  config part;
- `raw_code` is the contract entry the code bundle's read set names, serialized
  deterministically for the code bundle and the remediation LLM.
"""
import json
from collections.abc import Mapping
from dataclasses import dataclass

from continuo_contracts.errors import ContractsError
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonNode, code_projection, projection_hash

from domain.contract_vocabulary import NodeType
from domain.exceptions import MalformedContractError
from domain.model import ManifestNode, Runtime
from service.python_kind_rules import RULES

DEFAULT_MODE_TYPE = "replace"
"""The write mode a Python node declares when it names none."""


@dataclass(frozen=True)
class PythonNodeFields:
    """What a Python node declares, after its document was checked.

    `reads` maps a read name to its SQL (a python-csv node's sole read is
    `{"csv": <uri>}`), `dependency_sqls` is the upstream SQL of the reads in
    sorted read-name order and `csv_source` the csv uri, empty for every other
    kind. `script` is empty for a kind that carries none, `secret_ref` empty
    when the node names none, `primary_key` and `assets` empty when it has none,
    and `timeout_seconds` None when it declares no task timeout.
    """
    schema: str
    table: str
    owner: str
    schedule: str
    criticality: str
    kind: NodeType
    description: str
    script: str
    reads: Mapping[str, str]
    dependency_sqls: list[str]
    csv_source: str
    output_columns: list[dict]
    extra_columns: str
    config: dict
    secret_ref: str
    mode_type: str
    primary_key: list[str]
    assets: list[str]
    timeout_seconds: int | None


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


def build_python_node(
    fields: PythonNodeFields, *, service: str, image_tag: str, source_hash: str, shared_code_hash: str
) -> ManifestNode:
    """The ManifestNode of a Python node of `service`, hashed over the producer's
    `source_hash` and `shared_code_hash` and its own code projection.

    A node the projection refuses raises MalformedContractError.
    """
    rules = RULES[fields.kind]
    reads = dict(fields.reads)
    columns = list(fields.output_columns)
    config = dict(fields.config)
    primary_key = list(fields.primary_key)
    assets = list(fields.assets)

    projection, config_hash = _project({
        "schema": fields.schema,
        "table": fields.table,
        "kind": str(fields.kind),
        "script": fields.script,
        "reads": reads,
        "output_columns": columns,
        "extra_columns": fields.extra_columns,
        "config": config,
        "secret_ref": fields.secret_ref,
        "mode_type": fields.mode_type,
        "primary_key": primary_key,
        "assets": assets,
    })
    content_hash = fold3(source_hash, shared_code_hash, config_hash)

    # The contract entry the code bundle's read set names (docs/read-contract.md):
    # the entry's parsed fields, hash fields and timeout excluded, with
    # `script`, `secret_ref`, `mode_type`, `primary_key` and `assets` only where
    # they apply or differ from the default, serialized deterministically for the
    # code bundle and the remediation LLM. The hashed code projection rides beside
    # it in `projection`.
    raw_entry = {
        "schema": fields.schema,
        "table": fields.table,
        "owner": fields.owner,
        "schedule": fields.schedule,
        "criticality": fields.criticality,
        "kind": str(fields.kind),
        "description": fields.description,
        "extra_columns": fields.extra_columns,
        "reads": reads,
        "output_columns": columns,
        "config": config,
    }
    if rules.script_required:
        raw_entry["script"] = fields.script
    if fields.secret_ref:
        raw_entry["secret_ref"] = fields.secret_ref
    if fields.mode_type != DEFAULT_MODE_TYPE:
        raw_entry["mode_type"] = fields.mode_type
    if primary_key:
        raw_entry["primary_key"] = primary_key
    if assets:
        raw_entry["assets"] = assets

    raw_code = json.dumps(raw_entry, sort_keys=True, indent=2)

    return ManifestNode(
        table_name=fields.table,
        schema_name=fields.schema,
        # A python contract has no alias concept: the relation it writes is
        # always the declared table.
        resolved_relation=fields.table,
        service_name=service,
        owner=fields.owner,
        schedule_name=fields.schedule,
        criticality=fields.criticality,
        dependency_sqls=list(fields.dependency_sqls),
        candidate_sql="",
        node_type=fields.kind,
        content_hash=content_hash,
        image_tag=image_tag,
        original_file_path=fields.script,
        raw_code=raw_code,
        config=config,
        source_hash=source_hash,
        shared_code_hash=shared_code_hash,
        config_hash=config_hash,
        code_unit_ids=[],
        output_columns=columns,
        runtime=Runtime.PYTHON,
        csv_source=fields.csv_source,
        secret_ref=fields.secret_ref,
        timeout_seconds=fields.timeout_seconds,
        declared_columns=len(columns),
        projection=projection,
    )
