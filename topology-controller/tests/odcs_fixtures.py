"""The ODCS demo contracts and the producer bundle that carries them, for the tests of the
Python path.

`fixtures/odcs_demo/contracts` holds the three demo contracts of the service-py demo, and
`python_node` writes the contract of one more node from the few settings a test varies.
`bundle_file` writes a bundle the way `continuo-runtime pack` does: UTF-8 JSON from
`json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False)` and a newline.
"""
import hashlib
import json
from pathlib import Path

import continuo_contracts
import yaml

from domain.model import ManifestFile, ManifestKind
from service.release_timings import ReleaseTimings
from tests.fakes import FakeClock, ScriptedMemoryProbe

DEMO_DIR = Path(__file__).parent / "fixtures" / "odcs_demo"
PROJECT = "service-py"
DEMO_NAMES = ("demo_orders_csv", "fx_eur_usd_rates", "py_daily_kpis")
IMAGE_TAG = "img:1"


def demo_text(name: str) -> str:
    """The text of a demo contract, byte for byte."""
    return (DEMO_DIR / "contracts" / f"{name}.odcs.yaml").read_bytes().decode("utf-8")


def bundle_node(path: str, text: str, *, source_hash: str | None = None, shared_code_hash: str | None = None) -> dict:
    """One entry of a bundle's `nodes`. The two hash parts stand for what the
    producer computes from the repository: they default to a distinct digest
    of the path each."""
    return {
        "path": path,
        "text": text,
        "source_hash": source_hash if source_hash is not None else hashlib.sha256(f"source:{path}".encode()).hexdigest(),
        "shared_code_hash": (
            shared_code_hash if shared_code_hash is not None else hashlib.sha256(f"shared:{path}".encode()).hexdigest()
        ),
    }


def python_node(
    table: str = "py_metrics",
    *,
    schema: str = "test_schema",
    kind: str = "python-node",
    reads: dict[str, str] | None = None,
    csv_source: str = "s3://drops/orders.csv",
    secret_ref: str | None = None,
    timeout_minutes: int | None = None,
) -> dict:
    """One entry of a bundle's `nodes` whose contract declares the python node `schema.table`:
    daily, SECONDARY, with one INTEGER column `id`. A `python-node` runs `scripts/<table>.py` and
    reads `reads` (by default one query on `test_schema.orders`), a `python-api` node runs its
    script and reads nothing, and a `python-csv` node reads `csv_source` and runs no script."""
    properties = [
        {"vendor": "continuo", "property": "kind", "value": kind},
        {"vendor": "continuo", "property": "schedule", "value": "daily"},
        {"vendor": "continuo", "property": "criticality", "value": "SECONDARY"},
    ]
    if kind == "python-csv":
        properties.append({"vendor": "continuo", "property": "csvSource", "value": csv_source})
    else:
        properties.append({"vendor": "continuo", "property": "script", "value": f"scripts/{table}.py"})
    if kind == "python-node":
        queries = {"orders": "select id from test_schema.orders"} if reads is None else reads
        properties.append({"vendor": "continuo", "property": "readQueries", "value": queries})
    if secret_ref is not None:
        properties.append({"vendor": "continuo", "property": "secretRef", "value": secret_ref})
    if timeout_minutes is not None:
        properties.append({"vendor": "continuo", "property": "timeoutMinutes", "value": timeout_minutes})
    contract = {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": f"{schema}.{table}",
        "name": table,
        "version": "0",
        "description": {"purpose": f"The {table} node."},
        "team": {"name": "team-py"},
        "customProperties": properties,
        "schema": [{
            "name": table,
            "physicalType": "table",
            "properties": [{"name": "id", "physicalType": "INTEGER", "required": True}],
        }],
    }
    return bundle_node(f"contracts/{table}.odcs.yaml", yaml.safe_dump(contract, sort_keys=False))


def bound_kpis_text(table: str = "py_daily_kpis") -> str:
    """The demo contract `py_daily_kpis` for the table `table`, with its grain and the three
    aggregates of its columns bound to the things of the library's ontology vectors (Transaction)."""
    return f"""\
apiVersion: v3.2.0
kind: DataContract
id: analytics.{table}
name: {table}
version: "0"
description:
  purpose: Daily transaction count and total amount in EUR, rolled up from analytics.daily_transactions.
team:
  name: service-py
customProperties:
  - {{vendor: continuo, property: schedule, value: daily}}
  - {{vendor: continuo, property: criticality, value: SECONDARY}}
  - {{vendor: continuo, property: script, value: scripts/{table}.py}}
  - vendor: continuo
    property: readQueries
    value:
      daily: select created_at, amount_eur from analytics.daily_transactions
schema:
  - name: {table}
    physicalType: table
    customProperties:
      - {{vendor: continuo, property: grain, value: [day]}}
    properties:
      - name: day
        physicalType: DATE
        required: true
        customProperties:
          - {{vendor: continuo, property: aggregate, value: {{from: Transaction.created_at, period: day}}}}
      - name: tx_count
        physicalType: INTEGER
        required: true
        customProperties:
          - {{vendor: continuo, property: aggregate, value: {{from: Transaction, agg: count}}}}
      - name: total_amount
        physicalType: "NUMERIC(12,2)"
        required: true
        customProperties:
          - {{vendor: continuo, property: aggregate, value: {{from: Transaction.amount_eur, agg: sum}}}}
"""


def ontology_vector(name: str) -> dict:
    """The library's ontology vector `name`: a stored form that writes bindings, the ontology text
    it was resolved against, the resolved form, its hash and the checker's findings."""
    vectors = json.loads((Path(continuo_contracts.__file__).parent / "vectors" / "ontology.json").read_text())
    return next(vector for vector in vectors if vector["name"] == name)


def demo_node(name: str) -> dict:
    return bundle_node(f"contracts/{name}.odcs.yaml", demo_text(name))


def demo_nodes() -> list[dict]:
    return [demo_node(name) for name in DEMO_NAMES]


def bundle_doc(
    nodes: list[dict],
    *,
    project: str = PROJECT,
    producer_runtime: str = "0.13.0",
    producer_contracts: str = "0.2.0",
    image_packages: dict | None = None,
) -> dict:
    """A producer bundle, as `continuo-runtime pack` builds it."""
    return {
        "bundle_version": 1,
        "project": project,
        "producer": {"continuo_python_runtime": producer_runtime, "continuo_contracts": producer_contracts},
        "image_packages": {"pyarrow": "26.0.0"} if image_packages is None else image_packages,
        "nodes": nodes,
    }


def write_bundle(tmp_path: Path, doc: object, *, declared_project: str = PROJECT, name: str = "odcs-bundle.json") -> ManifestFile:
    """Write `doc` as a bundle file and return the ManifestFile that names it."""
    path = tmp_path / name
    path.write_bytes((json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    return ManifestFile(path=str(path), image_tag=IMAGE_TAG, declared_project=declared_project, kind=ManifestKind.PYTHON)


def bundle_file(tmp_path: Path, nodes: list[dict], *, declared_project: str = PROJECT, **producer) -> ManifestFile:
    """Write a bundle of `nodes` and return the ManifestFile that names it. `producer` takes
    `project`, `producer_runtime`, `producer_contracts` and `image_packages`."""
    return write_bundle(tmp_path, bundle_doc(nodes, **producer), declared_project=declared_project)


def release_timings() -> ReleaseTimings:
    return ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0))
