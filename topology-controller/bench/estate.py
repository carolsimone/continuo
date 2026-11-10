"""A seeded synthetic estate: the S3 objects of one release.

`generate_estate` builds what release-controller would have uploaded for a
release that carries dbt services and one python service: a dbt `manifest.json`
and `compile-extras.json` per dbt service, and a python `contract.yaml`. Every
value comes from one `random.Random(seed)`, so the same arguments always give
the same bytes and two baselines parse the same estate.

The SQL is only parsed and rewritten, never run, so its cost depends on its
shape and size, not on what it means. Each manifest carries the macros of a
real dbt project (`fixtures/macros.json.gz`), which is most of a real
manifest's bytes.
"""
import gzip
import hashlib
import json
import random
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import yaml

from domain.model import ManifestKind, ManifestRequest
from service.content_hash import content_hash_fold

DEFAULT_SEED = 1
MACROS_PATH = Path(__file__).parent / "fixtures" / "macros.json.gz"

_IMAGE_TAG = "bench"
_PYTHON_SERVICE = "svc-py"
_PYTHON_SCHEMA = "py"
_DBT_PACKAGES = {
    "dbt-core": "1.12.0b1",
    "dbt-adapters": "1.24.5",
    "dbt-common": "1.39.0",
    "dbt-postgres": "1.10.0",
}
_MODEL_MACROS = ["macro.dbt.run_query", "macro.dbt.statement"]
_TEST_MACROS = ["macro.dbt.test_not_null"]
_SEED_COLUMNS = ["id", "code", "value"]
# The columns the model SQL names beyond c0: its window sums c2, so a model
# needs at least three columns.
_MIN_COLUMNS = 3


@dataclass(frozen=True)
class Estate:
    """One release's S3 objects, and the facts a run needs about them.

    `objects` maps each S3 key to its bytes, `requests` is the list of
    artifacts the release request names, and `image_tags` the per-service tag
    map it carries. The counts are what the estate holds: `python_reads` is the
    number of reads declared across the python nodes.
    """
    release_id: str
    objects: dict[str, bytes]
    requests: list[ManifestRequest]
    image_tags: dict[str, str]
    models: int
    tests: int
    seeds: int
    python_nodes: int
    python_reads: int

    @property
    def dbt_nodes(self) -> int:
        """Every dbt node the parse returns: models, tests and seeds."""
        return self.models + self.tests + self.seeds

    @property
    def dbt_nodes_with_sql(self) -> int:
        """The dbt nodes that carry compiled SQL, so one candidate upload each."""
        return self.models + self.tests


@cache
def _macros() -> dict:
    with gzip.open(MACROS_PATH, "rb") as macros_file:
        return json.load(macros_file)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _split(total: int, parts: int) -> list[int]:
    """Split `total` evenly; the remainder goes to the first parts."""
    base, remainder = divmod(total, parts)
    return [base + 1 if part < remainder else base for part in range(parts)]


def _model_sql(columns: list[str], upstreams: list[tuple[str, str]]) -> str:
    """A CTE over the first upstream, a window sum, and a left join to each
    further upstream."""
    first_schema, first_table = upstreams[0]
    inner = ",\n        ".join(columns)
    outer = [f"a.{column}" for column in columns]
    outer.append("sum(a.c2) over (partition by a.c0) as c2_total")
    joins = []
    for position, (schema, table) in enumerate(upstreams[1:], start=1):
        alias = f"b{position}"
        outer.append(f"{alias}.c1 as {alias}_c1")
        joins.append(f"left join {schema}.{table} as {alias} on a.c0 = {alias}.c0")
    outer_list = ",\n    ".join(outer)
    join_lines = "".join(f"\n{join}" for join in joins)
    return (
        f"with src as (\n    select\n        {inner}\n    from {first_schema}.{first_table}\n)\n"
        f"select\n    {outer_list}\nfrom src as a{join_lines}"
    )


def _model_node(service: int, name: str, columns: list[str], sql: str) -> dict:
    schema = f"s{service}"
    return {
        "unique_id": f"model.svc_{service}.{name}",
        "resource_type": "model",
        "name": name,
        "schema": schema,
        "fqn": [f"svc_{service}", name],
        "tags": ["daily"],
        "config": {"materialized": "table", "meta": {"owner": f"team-{service}"}},
        "checksum": {"name": "sha256", "checksum": _sha256(name)},
        "original_file_path": f"models/{name}.sql",
        "columns": {
            column: {"name": column, "data_type": "integer", "description": f"The {column} column of {name}."}
            for column in columns
        },
        "depends_on": {"macros": list(_MODEL_MACROS)},
        "compiled_code": sql,
    }


def _test_node(service: int, model: dict, column: str) -> dict:
    schema = model["schema"]
    name = f"not_null_{model['name']}_{column}"
    return {
        "unique_id": f"test.svc_{service}.{name}",
        "resource_type": "test",
        "name": name,
        "schema": schema,
        "fqn": [f"svc_{service}", name],
        "tags": [],
        "config": {},
        "original_file_path": f"models/{model['name']}.yml",
        "attached_node": model["unique_id"],
        "depends_on": {"macros": list(_TEST_MACROS)},
        "compiled_code": f"select {column} from {schema}.{model['name']} where {column} is null",
    }


def _seed_node(service: int) -> dict:
    name = f"seed_svc_{service}"
    return {
        "unique_id": f"seed.svc_{service}.{name}",
        "resource_type": "seed",
        "name": name,
        "schema": f"s{service}",
        "fqn": [f"svc_{service}", name],
        "tags": [],
        "config": {"materialized": "seed", "meta": {"owner": f"team-{service}"}},
        "checksum": {"name": "sha256", "checksum": _sha256(name)},
        "original_file_path": f"seeds/{name}.csv",
        "columns": {
            column: {"name": column, "data_type": "text", "description": f"The {column} column of {name}."}
            for column in _SEED_COLUMNS
        },
        "depends_on": {"macros": []},
    }


def _compile_extras(service: int, release_id: str, seed: dict) -> dict:
    return {
        "version": 1,
        "service": f"svc-{service}",
        "release_id": release_id,
        "seeds": {seed["unique_id"]: {
            "sha256": _sha256(seed["name"] + "-csv"),
            "header": list(_SEED_COLUMNS),
            "column_types": {},
        }},
        "project_flags": {},
        "packages": dict(_DBT_PACKAGES),
    }


def _python_node(index: int, columns: list[str], reads: dict[str, str]) -> dict:
    table = f"py_{index:05d}"
    source_hash = "sha256:" + _sha256(table)
    shared_code_hash = ""
    config_hash = _sha256(table + "cfg")
    return {
        "schema": _PYTHON_SCHEMA,
        "table": table,
        "owner": "team-py",
        "schedule": "daily",
        "criticality": "SECONDARY",
        "script": f"scripts/{table}.py",
        "reads": reads,
        "output_columns": [{"name": column, "type": "INTEGER", "nullable": True} for column in columns],
        "source_hash": source_hash,
        "shared_code_hash": shared_code_hash,
        "config_hash": config_hash,
        "content_hash": content_hash_fold(source_hash, shared_code_hash, config_hash),
    }


def generate_estate(
    models: int,
    *,
    columns: int = 20,
    tests_per_model: int = 2,
    services: int = 10,
    python_share: float = 0.1,
    seed: int = DEFAULT_SEED,
    release_id: str = "rel-bench",
) -> Estate:
    """Build the estate of `models` dbt models spread over `services` dbt
    services, each with `columns` columns and `tests_per_model` not-null tests,
    one seed per service, and, when `python_share` is above zero, one python
    service of `round(models * python_share)` nodes (at least one)."""
    if models < 1 or services < 1:
        raise ValueError("an estate needs at least one model and one service")
    if columns < _MIN_COLUMNS:
        raise ValueError(f"a model needs at least {_MIN_COLUMNS} columns, got {columns}")
    if not 0 <= tests_per_model <= columns:
        raise ValueError(f"tests_per_model must be between 0 and the column count {columns}, got {tests_per_model}")

    rng = random.Random(seed)
    column_names = [f"c{position}" for position in range(columns)]
    macros = _macros()

    objects: dict[str, bytes] = {}
    requests: list[ManifestRequest] = []
    image_tags: dict[str, str] = {}
    # (schema, table) of every model built so far, in global index order.
    built: list[tuple[str, str]] = []
    index = 0

    for service, count in enumerate(_split(models, services)):
        schema = f"s{service}"
        service_name = f"svc-{service}"
        seed_node = _seed_node(service)
        nodes: dict[str, dict] = {seed_node["unique_id"]: seed_node}
        for position in range(count):
            name = f"m_{index:05d}"
            if position == 0:
                upstreams = [(schema, seed_node["name"])]
            else:
                picks = rng.sample(range(len(built)), min(2, len(built)))
                upstreams = [built[pick] for pick in picks]
            model = _model_node(service, name, column_names, _model_sql(column_names, upstreams))
            nodes[model["unique_id"]] = model
            for column in column_names[:tests_per_model]:
                test = _test_node(service, model, column)
                nodes[test["unique_id"]] = test
            built.append((schema, name))
            index += 1

        manifest = {"metadata": {"dbt_version": _DBT_PACKAGES["dbt-core"]}, "nodes": nodes, "macros": macros}
        manifest_key = f"{service_name}/{release_id}/manifest.json"
        objects[manifest_key] = json.dumps(manifest, sort_keys=True).encode()
        objects[f"{service_name}/{release_id}/compile-extras.json"] = json.dumps(
            _compile_extras(service, release_id, seed_node), sort_keys=True,
        ).encode()
        requests.append(ManifestRequest(service=service_name, key=manifest_key, kind=ManifestKind.DBT))
        image_tags[service_name] = _IMAGE_TAG

    python_nodes = 0
    python_reads = 0
    if python_share > 0:
        python_nodes = max(1, round(models * python_share))
        entries = []
        for position in range(python_nodes):
            # One read on an even index, two on an odd one.
            wanted = 1 if position % 2 == 0 else 2
            picks = rng.sample(range(len(built)), min(wanted, len(built)))
            reads = {
                f"r{read}": f"select c0, c1 from {built[pick][0]}.{built[pick][1]}"
                for read, pick in enumerate(picks)
            }
            python_reads += len(reads)
            entries.append(_python_node(position, column_names, reads))
        contract = {"contract_version": 1, "service": _PYTHON_SERVICE, "nodes": entries}
        contract_key = f"{_PYTHON_SERVICE}/{release_id}/contract.yaml"
        objects[contract_key] = yaml.safe_dump(contract, sort_keys=False).encode()
        requests.append(ManifestRequest(service=_PYTHON_SERVICE, key=contract_key, kind=ManifestKind.PYTHON))
        image_tags[_PYTHON_SERVICE] = _IMAGE_TAG

    return Estate(
        release_id=release_id,
        objects=objects,
        requests=requests,
        image_tags=image_tags,
        models=models,
        tests=models * tests_per_model,
        seeds=services,
        python_nodes=python_nodes,
        python_reads=python_reads,
    )
