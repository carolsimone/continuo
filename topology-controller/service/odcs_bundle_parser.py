"""Reads the producer bundle of a `python-odcs` service into ManifestNodes.

A service's CI packs the ODCS contracts of its repository into one `odcs-bundle.json`
(`continuo-runtime pack`) and uploads it with the release:

    {"bundle_version": 1,
     "service": <service>,
     "producer": {"continuo_python_runtime": <version>, "continuo_contracts": <version>},
     "image_packages": {<package>: <version>},
     "nodes": [{"path": <repository path of the contract file>,
                "text": <the file's text as written>,
                "source_hash": <bare hex>,
                "shared_code_hash": <bare hex, or "" when the node has none>}]}

The bundle as a whole is read strictly: a bundle that is not that document, that names
another service, that a newer continuo-contracts wrote, or whose nodes share a path or
resolve to one node is refused whole (`InvalidOdcsBundleError`), because no node of it can
be trusted or none can be told from another. Each contract is read on its own, through
continuo-contracts: the strict YAML loader, the schema and rules check (cached, see
`ContractValidator`), then the node it declares, whose unique_id must equal the contract's
`id` (the library accepts a non-ASCII capital, which the unique_id lowers). A contract that
fails a check is not an exception but one `FailedNode` of the result, named by its id in
lower case when the id is `<schema>.<table>`, so a release reports every invalid contract
at once; a valid one becomes a ManifestNode built by `build_python_node`, hashed over the
producer's two hash parts and the node's own code projection, and keeps its authored
document. Every result comes in path order, whatever order the bundle lists its nodes in.
"""
import json
from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from continuo_contracts import yaml_strict
from continuo_contracts.errors import ContractsError, StrictYamlError
from continuo_contracts.hashing import sha256_hex
from continuo_contracts.python import KIND_ALIASES, PYTHON_KINDS, node_from_odcs
from continuo_contracts.validate import Issue
from continuo_contracts.vocabulary import continuo_values

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.exceptions import InvalidOdcsBundleError, MalformedContractError
from domain.model import ContractFamily, FailedNode, ManifestFile, ManifestNode, NodeContract, ParsedArtifact, SkippedNode
from domain.node_timeout import timeout_seconds
from service.contract_validation import ContractValidator
from service.contracts_library import LIBRARY_VERSION, version_triple
from service.python_kind_rules import RULES
from service.python_node_builder import PythonNodeFields, build_python_node
from service.release_timings import ReleaseTimings, Stage

BUNDLE_VERSION = 1
CONTRACT_FILE_SUFFIX = ".odcs.yaml"

_TOP_LEVEL_KEYS = frozenset({"bundle_version", "service", "producer", "image_packages", "nodes"})
_PRODUCER_KEYS = frozenset({"continuo_python_runtime", "continuo_contracts"})
_NODE_KEYS = frozenset({"path", "text", "source_hash", "shared_code_hash"})
_DEFAULT_KIND = NodeType.PYTHON_NODE.value


def _fail(detail: str) -> NoReturn:
    raise InvalidOdcsBundleError(detail)


def _read_json(path: str) -> object:
    with open(path, "rb") as f:
        data = f.read()
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail(f"the bundle is not UTF-8 text: {exc}")
    try:
        return json.loads(text)
    except (ValueError, RecursionError) as exc:
        _fail(f"the bundle is not valid JSON: {exc}")


def _text(value: object, label: str, *, non_empty: bool = True) -> str:
    """A string field of the bundle that UTF-8 can encode, and that has text when `non_empty`."""
    if not isinstance(value, str) or (non_empty and not value):
        _fail(f"{label} must be a {'non-empty ' if non_empty else ''}string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        _fail(f"{label} must be text UTF-8 can encode")
    return value


def _check_keys(obj: Mapping[str, Any], expected: frozenset[str], label: str) -> None:
    unknown = sorted(set(obj) - expected)
    if unknown:
        _fail(f"{label} has unknown keys {unknown}")
    missing = sorted(expected - set(obj))
    if missing:
        _fail(f"{label} is missing {missing}")


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{label} must be an object")
    return value


def _check_bundle_version(bundle: Mapping[str, Any]) -> None:
    if "bundle_version" not in bundle:
        return
    version = bundle["bundle_version"]
    if type(version) is int and version > BUNDLE_VERSION:
        _fail(f"bundle_version {version} is newer than this install reads (it reads bundle_version {BUNDLE_VERSION})")
    if type(version) is not int or version != BUNDLE_VERSION:
        _fail(f"unsupported bundle_version {version!r}: this install reads bundle_version {BUNDLE_VERSION}")


def _check_producer(producer: object, library_version: str) -> tuple[str, str]:
    """The producer's runtime version and continuo-contracts version, the latter
    refused when it is newer than `library_version`."""
    producer = _object(producer, "producer")
    _check_keys(producer, _PRODUCER_KEYS, "producer")
    runtime = _text(producer["continuo_python_runtime"], "producer.continuo_python_runtime")
    written_by = _text(producer["continuo_contracts"], "producer.continuo_contracts")
    produced = version_triple(written_by)
    if produced is None:
        _fail(f"producer.continuo_contracts {written_by!r} is not a version X.Y.Z")
    running = version_triple(library_version)
    if running is None:
        raise ValueError(f"the installed continuo-contracts version {library_version!r} is not a version X.Y.Z")
    if produced > running:
        _fail(
            f"the bundle was written by continuo-contracts {written_by} and this install runs {library_version}:"
            " upgrade continuo, or pin an older runtime"
        )
    return runtime, written_by


def _check_image_packages(packages: object) -> dict[str, str]:
    if not isinstance(packages, dict) or not all(isinstance(v, str) for v in packages.values()):
        _fail("image_packages must be an object of package name to version string")
    for name, version in packages.items():
        _text(name, "image_packages name", non_empty=False)
        _text(version, "image_packages version", non_empty=False)
    return dict(packages)


def _check_entries(raw: object) -> list[dict[str, str]]:
    """The bundle's node entries, each checked, in path order."""
    if not isinstance(raw, list):
        _fail("nodes must be a list")
    seen: set[str] = set()
    entries = []
    for index, entry in enumerate(raw):
        label = f"nodes[{index}]"
        entry = _object(entry, label)
        _check_keys(entry, _NODE_KEYS, label)
        path = _text(entry["path"], f"{label}.path")
        if not path.endswith(CONTRACT_FILE_SUFFIX):
            _fail(f"{label}.path {path!r} does not end in {CONTRACT_FILE_SUFFIX}")
        if path in seen:
            _fail(f"two nodes have the path {path}")
        seen.add(path)
        text = entry["text"]
        if not isinstance(text, str):
            _fail(f"{label}.text must be a string")
        entries.append({
            "path": path,
            "text": text,
            "source_hash": _text(entry["source_hash"], f"{label}.source_hash"),
            "shared_code_hash": _text(entry["shared_code_hash"], f"{label}.shared_code_hash", non_empty=False),
        })
    return sorted(entries, key=lambda entry: entry["path"])


def _failed_node(timings: ReleaseTimings, doc: object, path: str, service: str, issues: list[Issue]) -> FailedNode:
    """The FailedNode of a contract that did not pass its checks; each issue is counted."""
    for issue in issues:
        timings.add_contract_failure("validate", issue.reason)
    identifier = doc.get("id") if isinstance(doc, Mapping) else None
    return FailedNode(
        node_id=_failed_node_id(identifier) if isinstance(identifier, str) and identifier.strip() else path,
        kind=ParseFailureKind.INVALID_CONTRACT,
        service=service,
        file_path=path,
        node_type=_node_type(doc),
        detail="; ".join(str(issue) for issue in issues),
    )


def _failed_node_id(identifier: str) -> str:
    """The name of a failed contract from its id: in lower case, as a node's unique_id is
    minted, when the id has the shape <schema>.<table>; as written otherwise."""
    parts = identifier.split(".")
    if len(parts) == 2 and all(parts):
        return identifier.lower()
    return identifier


def _id_issue(doc: Mapping[str, Any], node: ManifestNode, path: str) -> Issue | None:
    """The issue of a contract whose id is not the unique_id of the node it declares, or
    None when the two are equal.

    The library accepts an id whose ASCII letters are in lower case, while a node's
    unique_id is the id lowered by `str.lower`, which lowers every letter. A non-ASCII
    capital (`analytics.Ölstand`) would key the node and its contract under two ids."""
    identifier = doc["id"]
    if identifier == node.unique_id:
        return None
    return Issue(
        "id", "identity",
        f"{path} declares the id {identifier!r}, and continuo names its node {node.unique_id!r}:"
        " write the id in lower case",
    )


def _node_type(doc: object) -> str:
    """The Python kind a contract declares, `python-node` when it declares none that is one."""
    if isinstance(doc, Mapping):
        declared = continuo_values(doc.get("customProperties")).get("kind", _DEFAULT_KIND)
        if isinstance(declared, str):
            kind = KIND_ALIASES.get(declared, declared)
            if kind in PYTHON_KINDS:
                return kind
    return _DEFAULT_KIND


def _purpose(doc: Mapping[str, Any]) -> str:
    description = doc.get("description")
    purpose = description.get("purpose") if isinstance(description, Mapping) else None
    return purpose if isinstance(purpose, str) else ""


def _build_node(doc: Mapping[str, Any], entry: Mapping[str, str], *, service: str, image_tag: str) -> ManifestNode:
    """The ManifestNode a checked contract declares, hashed over the entry's hash
    parts. Raises ContractsError or MalformedContractError for a contract the
    node's kind or projection refuses after the checks accepted it."""
    odcs = node_from_odcs(doc)
    kind = NodeType(odcs.kind)
    parsed = RULES[kind].reads.parse(odcs.reads, odcs.relation)
    values = continuo_values(doc.get("customProperties"))
    try:
        task_timeout = timeout_seconds(values.get("timeoutMinutes"))
    except ValueError as exc:
        raise MalformedContractError(f"{odcs.relation}: timeoutMinutes {exc}") from exc
    node = build_python_node(
        PythonNodeFields(
            schema=odcs.schema,
            table=odcs.table,
            owner=doc["team"]["name"],
            schedule=values["schedule"],
            criticality=values["criticality"],
            kind=kind,
            description=_purpose(doc),
            script=odcs.script,
            reads=parsed.reads,
            dependency_sqls=parsed.dependency_sqls,
            csv_source=parsed.csv_source,
            output_columns=[{"name": c.name, "type": c.type, "nullable": c.nullable} for c in odcs.output_columns],
            extra_columns=odcs.extra_columns,
            config=dict(odcs.config),
            secret_ref=odcs.secret_ref,
            mode_type=odcs.mode_type,
            # A key only an upsert node has: a node that does not upsert keeps the
            # `primaryKey` columns as description and writes no key.
            primary_key=list(odcs.primary_key) if odcs.mode_type == "upsert" else [],
            assets=list(odcs.assets),
            timeout_seconds=task_timeout,
        ),
        service=service,
        image_tag=image_tag,
        source_hash=entry["source_hash"],
        shared_code_hash=entry["shared_code_hash"],
    )
    node.contract = NodeContract(
        family=ContractFamily.PYTHON,
        document=doc,
        path=entry["path"],
        source_sha256=sha256_hex(entry["text"].encode("utf-8")),
    )
    return node


def _read_node(
    entry: Mapping[str, str], *, service: str, image_tag: str, validator: ContractValidator, timings: ReleaseTimings
) -> ManifestNode | FailedNode:
    """One contract of the bundle: its node, or the FailedNode that says why it is not one."""
    path = entry["path"]
    try:
        doc = yaml_strict.load(entry["text"])
    except StrictYamlError as exc:
        return _failed_node(timings, None, path, service, [Issue("$", "strict_yaml", str(exc))])
    with timings.stage(Stage.CONTRACT_VALIDATION, sample_memory=False, nodes=1):
        issues = validator.check(doc, path=path, service=service, on_cache=timings.add_validation_cache)
    if issues:
        return _failed_node(timings, doc, path, service, issues)
    try:
        node = _build_node(doc, entry, service=service, image_tag=image_tag)
    except (ContractsError, MalformedContractError) as exc:
        return _failed_node(timings, doc, path, service, [Issue("$", "value", str(exc))])
    mismatch = _id_issue(doc, node, path)
    if mismatch is not None:
        return _failed_node(timings, doc, path, service, [mismatch])
    return node


def _check_one_node_per_id(nodes: list[ManifestNode]) -> None:
    """Refuse two contracts that resolve to one node. Each passes its checks on its
    own, so neither file is wrong by itself. The ids compare in lower case, as a
    node's unique_id is minted."""
    paths_by_id: dict[str, list[str]] = {}
    for node in nodes:
        paths_by_id.setdefault(node.unique_id, []).append(node.contract.path)
    clashes = []
    for unique_id, paths in sorted(paths_by_id.items()):
        if len(paths) > 1:
            paths.sort()
            count = "two" if len(paths) == 2 else str(len(paths))
            clashes.append(f"{count} contracts declare {unique_id}: {', '.join(paths[:-1])} and {paths[-1]}")
    if clashes:
        _fail("; ".join(clashes))


def parse_odcs_bundle(
    mf: ManifestFile,
    *,
    skipped: list[SkippedNode] | None = None,
    progress: Callable[[], None],
    timings: ReleaseTimings,
    validator: ContractValidator,
    library_version: str = LIBRARY_VERSION,
) -> ParsedArtifact:
    """Parse a `python-odcs` producer bundle into the nodes of its valid contracts and
    one FailedNode for each invalid one.

    `skipped` is accepted so every parse function takes the same arguments and is
    never appended to: a contract is read or it fails, it is never left out.
    `progress` is called once after each contract. The library's check of each contract,
    and nothing else of reading it (the YAML load and the node's build stay in the parse),
    is the `contract_validation` stage of `timings`, as a dbt node's check is; `timings`
    also counts each cache lookup of `validator` and each contract failure by its reason.
    `library_version` is the continuo-contracts version of this install, which the
    producer's must not exceed.

    Raises InvalidOdcsBundleError for a bundle that cannot be read as a whole (see the
    module docstring). A bundle with no node is an empty ParsedArtifact.
    """
    bundle = _read_json(mf.path)
    if not isinstance(bundle, dict):
        _fail(f"the bundle must be a JSON object, got {type(bundle).__name__}")
    _check_bundle_version(bundle)
    _check_keys(bundle, _TOP_LEVEL_KEYS, "the bundle")
    service = _text(bundle["service"], "service")
    if mf.declared_service and service != mf.declared_service:
        _fail(f"the bundle is for service {service!r}, not {mf.declared_service!r}")
    runtime_version, _ = _check_producer(bundle["producer"], library_version)
    image_packages = _check_image_packages(bundle["image_packages"])
    entries = _check_entries(bundle["nodes"])

    nodes: list[ManifestNode] = []
    failed: list[FailedNode] = []
    for entry in entries:
        read = _read_node(entry, service=service, image_tag=mf.image_tag, validator=validator, timings=timings)
        if isinstance(read, FailedNode):
            failed.append(read)
        else:
            nodes.append(read)
        progress()
    _check_one_node_per_id(nodes)
    return ParsedArtifact(
        nodes=nodes,
        shared_code={},
        failed=tuple(failed),
        python_provenance={"continuo_python_runtime": runtime_version, "image_packages": image_packages},
    )
