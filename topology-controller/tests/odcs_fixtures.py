"""The ODCS demo contracts and the producer bundle that carries them, for the tests of the
`python-odcs` path.

`fixtures/odcs_demo/contracts` holds the three demo contracts of the service-py demo and
`fixtures/odcs_demo/old` the wire entries they replace, one `nodes:` file each. `bundle_file`
writes a bundle the way `continuo-runtime pack` does: UTF-8 JSON from
`json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False)` and a newline.
"""
import hashlib
import json
from pathlib import Path

from domain.model import ManifestFile, ManifestKind
from service.release_timings import ReleaseTimings
from tests.fakes import FakeClock, ScriptedMemoryProbe

DEMO_DIR = Path(__file__).parent / "fixtures" / "odcs_demo"
SERVICE = "service-py"
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


def demo_node(name: str) -> dict:
    return bundle_node(f"contracts/{name}.odcs.yaml", demo_text(name))


def demo_nodes() -> list[dict]:
    return [demo_node(name) for name in DEMO_NAMES]


def bundle_doc(
    nodes: list[dict],
    *,
    service: str = SERVICE,
    producer_runtime: str = "0.13.0",
    producer_contracts: str = "0.2.0",
    image_packages: dict | None = None,
) -> dict:
    """A producer bundle, as `continuo-runtime pack` builds it."""
    return {
        "bundle_version": 1,
        "service": service,
        "producer": {"continuo_python_runtime": producer_runtime, "continuo_contracts": producer_contracts},
        "image_packages": {"pyarrow": "26.0.0"} if image_packages is None else image_packages,
        "nodes": nodes,
    }


def write_bundle(tmp_path: Path, doc: object, *, declared_service: str = SERVICE, name: str = "odcs-bundle.json") -> ManifestFile:
    """Write `doc` as a bundle file and return the ManifestFile that names it."""
    path = tmp_path / name
    path.write_bytes((json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    return ManifestFile(path=str(path), image_tag=IMAGE_TAG, declared_service=declared_service, kind=ManifestKind.PYTHON_ODCS)


def bundle_file(tmp_path: Path, nodes: list[dict], *, declared_service: str = SERVICE, **producer) -> ManifestFile:
    """Write a bundle of `nodes` and return the ManifestFile that names it. `producer` takes
    `service`, `producer_runtime`, `producer_contracts` and `image_packages`."""
    return write_bundle(tmp_path, bundle_doc(nodes, **producer), declared_service=declared_service)


def release_timings() -> ReleaseTimings:
    return ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0))
