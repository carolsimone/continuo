"""Tests for the worker's executor client and its one-shot artifact hydration.

Both are exercised against a real socket and a real dbt Manifest round trip: the
client's retry and terminal-status rules decide whether a superseded worker
hammers the fence, and the store's validation decides whether a worker will
execute an artifact its pool was never pinned to.
"""
import hashlib
import json
from pathlib import Path

import pytest
from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.api_client import (
    CancelledError,
    ExecutorClient,
    PoolMismatchError,
    RequestFailed,
    StaleLeaseError,
)
from continuo_dbt_runtime.artifact_store import ArtifactStore, InitializationError
from continuo_dbt_runtime.descriptor import FORMAT
from continuo_dbt_runtime.parse_context import parse_context_sha256
from continuo_dbt_runtime.worker import WorkerConfig

from tests.conftest import SERVICE_NAME, UNIQUE_ID

CREDENTIAL = "pool-credential-do-not-leak"
POOL_KEY = "e" * 64
IMAGE_TAG = "sha-1"
CONTROLLER_CONTEXT = '{"command_dialect_sha256":"abc"}'

RUNTIME_PATH = "/internal/v1/worker/runtime"
CLAIM_PATH = "/internal/v1/workers/claim"
LEASE_ID = "9f1d0b3a-0000-4000-8000-000000000001"
DEPLOYMENT_ID = "9f1d0b3a-0000-4000-8000-000000000002"
START_PATH = f"/internal/v1/leases/{LEASE_ID}/start"
HEARTBEAT_PATH = f"/internal/v1/leases/{LEASE_ID}/heartbeat"


def client_for(executor, **kwargs) -> ExecutorClient:
    """A client whose backoff does not actually sleep."""
    kwargs.setdefault("sleep", lambda _seconds: None)
    return ExecutorClient(executor.base_url, POOL_KEY, CREDENTIAL, **kwargs)


# --- client transport -----------------------------------------------------


def test_request_sends_both_credentials_in_their_own_headers(fake_executor):
    fake_executor.queue(HEARTBEAT_PATH, (200, None))

    client_for(fake_executor).heartbeat(LEASE_ID, DEPLOYMENT_ID, "lease-token-1")

    sent = fake_executor.requests[-1]
    assert sent.headers["Authorization"] == f"Bearer {CREDENTIAL}"
    assert sent.headers["X-Continuo-Pool-Key"] == POOL_KEY
    assert sent.headers["X-Continuo-Lease-Token"] == "lease-token-1"
    assert sent.body == {"deployment_id": DEPLOYMENT_ID}


def test_request_omits_the_lease_header_when_there_is_no_lease(fake_executor):
    fake_executor.queue(RUNTIME_PATH, (200, {"descriptor_url": "d", "artifact_url": "a"}))

    client_for(fake_executor).runtime()

    assert "X-Continuo-Lease-Token" not in fake_executor.requests[-1].headers


def test_claim_returns_no_lease_on_204(fake_executor):
    fake_executor.queue(CLAIM_PATH, (204, None))

    assert client_for(fake_executor).claim(
        wait_seconds=5, owner="w1", pod_name="p", pod_uid="u"
    ) is None


def test_claim_returns_the_granted_lease(fake_executor):
    lease = {"lease_id": LEASE_ID, "deployment_id": DEPLOYMENT_ID, "argv": ["dbt", "run"]}
    fake_executor.queue(CLAIM_PATH, (200, lease))

    assert client_for(fake_executor).claim(
        wait_seconds=5, owner="w1", pod_name="p", pod_uid="u"
    ) == lease


# --- retry rules ----------------------------------------------------------


def test_retries_a_5xx_then_succeeds(fake_executor):
    fake_executor.queue(
        HEARTBEAT_PATH,
        (500, {"error": {"code": "internal", "message": "boom"}}),
        (503, {"error": {"code": "internal", "message": "boom"}}),
        (200, None),
    )

    client_for(fake_executor).heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")

    assert fake_executor.count(HEARTBEAT_PATH) == 3


def test_retries_a_429(fake_executor):
    fake_executor.queue(HEARTBEAT_PATH, (429, None), (200, None))

    client_for(fake_executor).heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")

    assert fake_executor.count(HEARTBEAT_PATH) == 2


def test_gives_up_after_max_attempts_on_a_persistent_5xx(fake_executor):
    fake_executor.queue(HEARTBEAT_PATH, (500, {"error": {"code": "internal", "message": "boom"}}))

    with pytest.raises(RequestFailed):
        client_for(fake_executor, max_attempts=3).heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")

    assert fake_executor.count(HEARTBEAT_PATH) == 3


def test_a_connection_error_is_retried_then_reported(fake_executor):
    fake_executor.stop()
    client = ExecutorClient(
        fake_executor.base_url, POOL_KEY, CREDENTIAL, max_attempts=2,
        sleep=lambda _s: None,
    )

    with pytest.raises(RequestFailed):
        client.heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")


@pytest.mark.parametrize(
    "status, code, expected",
    [
        (409, "stale_lease", StaleLeaseError),
        (403, "pool_mismatch", PoolMismatchError),
        (410, "cancelled", CancelledError),
    ],
)
def test_terminal_statuses_are_never_retried(fake_executor, status, code, expected):
    """A superseded worker must stop, not loop against the fence.

    Asserted on the call count as well as the exception: raising the right type
    while still having retried would be the failure this rule exists to prevent.
    """
    fake_executor.queue(HEARTBEAT_PATH, (status, {"error": {"code": code, "message": "no"}}))

    with pytest.raises(expected):
        client_for(fake_executor).heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")

    assert fake_executor.count(HEARTBEAT_PATH) == 1


@pytest.mark.parametrize("status", [400, 401])
def test_client_faults_are_never_retried(fake_executor, status):
    fake_executor.queue(START_PATH, (status, {"error": {"code": "invalid_request", "message": "no"}}))

    with pytest.raises(RequestFailed):
        client_for(fake_executor).start(LEASE_ID, DEPLOYMENT_ID, "t")

    assert fake_executor.count(START_PATH) == 1


def test_an_unknown_deployment_reads_as_a_stale_lease(fake_executor):
    """409 covers a stale token and an unknown deployment alike; both are final."""
    fake_executor.queue(START_PATH, (409, {"error": {"code": "stale_lease", "message": "no"}}))

    with pytest.raises(StaleLeaseError):
        client_for(fake_executor).start(LEASE_ID, DEPLOYMENT_ID, "t")


# --- redaction ------------------------------------------------------------


def test_errors_never_carry_the_credential(fake_executor):
    fake_executor.queue(HEARTBEAT_PATH, (409, {"error": {"code": "stale_lease", "message": "no"}}))

    with pytest.raises(StaleLeaseError) as caught:
        client_for(fake_executor).heartbeat(LEASE_ID, DEPLOYMENT_ID, "t")

    assert CREDENTIAL not in str(caught.value)
    assert CREDENTIAL not in repr(caught.value)


def test_a_failed_download_does_not_echo_the_signed_query(fake_executor):
    """A presigned URL's query is a capability, so it stays out of the error."""
    fake_executor.queue_raw("/artifact?X-Amz-Signature=deadbeef", 500, b"nope", "text/plain")
    from continuo_dbt_runtime.artifact_store import download_bytes

    with pytest.raises(RuntimeError) as caught:
        download_bytes(f"{fake_executor.base_url}/artifact?X-Amz-Signature=deadbeef")

    assert "deadbeef" not in str(caught.value)
    assert "X-Amz-Signature" not in str(caught.value)


# --- artifact hydration ---------------------------------------------------


def descriptor_for(packed: bytes, **overrides) -> dict:
    manifest = Manifest.from_msgpack(packed)
    descriptor = {
        "format": FORMAT,
        "service_name": SERVICE_NAME,
        "release_id": "r1",
        "image_tag": IMAGE_TAG,
        "artifact_uri": "s3://continuo/service-1/r1/partial_parse.msgpack",
        "sha256": hashlib.sha256(packed).hexdigest(),
        "dbt_core_version": manifest.metadata.dbt_version,
        "adapter_type": manifest.metadata.adapter_type,
        "parse_context_sha256": parse_context_sha256(manifest, CONTROLLER_CONTEXT),
    }
    descriptor.update(overrides)
    return descriptor


def config_for(executor, tmp_path: Path, **overrides) -> WorkerConfig:
    values = {
        "executor_url": executor.base_url,
        "pool_key": POOL_KEY,
        "service_name": SERVICE_NAME,
        "image_tag": IMAGE_TAG,
        "runtime_manifest_sha256": "",
        "controller_context_json": CONTROLLER_CONTEXT,
        "pod_name": "worker-0",
        "pod_uid": "uid-0",
        "cache_dir": tmp_path / "cache",
        "ready_file": tmp_path / "ready",
    }
    values.update(overrides)
    return WorkerConfig(**values)


def serve_artifact(executor, packed: bytes, descriptor: dict) -> None:
    executor.queue(RUNTIME_PATH, (200, {
        "descriptor_url": f"{executor.base_url}/descriptor",
        "artifact_url": f"{executor.base_url}/artifact",
    }))
    executor.queue_raw("/descriptor", 200, json.dumps(descriptor).encode(), "application/json")
    executor.queue_raw("/artifact", 200, packed, "application/octet-stream")


def store_for(executor, tmp_path, packed, descriptor=None, **config_overrides) -> ArtifactStore:
    descriptor = descriptor if descriptor is not None else descriptor_for(packed)
    serve_artifact(executor, packed, descriptor)
    config = config_for(
        executor, tmp_path,
        runtime_manifest_sha256=config_overrides.pop(
            "runtime_manifest_sha256", descriptor["sha256"]),
        **config_overrides,
    )
    return ArtifactStore(config, client_for(executor))


def test_artifact_store_hydrates_once_and_rejects_parser_fallback(
    fake_executor, tmp_path, real_manifest_bytes
):
    store = store_for(fake_executor, tmp_path, real_manifest_bytes)

    loaded = store.load()

    assert loaded.manifest.nodes[UNIQUE_ID].name == "table_a"
    assert fake_executor.count("/artifact") == 1
    assert store.load() is loaded
    assert fake_executor.count("/artifact") == 1


def test_artifact_store_writes_the_canonical_partial_parse(
    fake_executor, tmp_path, real_manifest_bytes
):
    loaded = store_for(fake_executor, tmp_path, real_manifest_bytes).load()

    assert loaded.canonical_path.name == "partial_parse.msgpack"
    assert loaded.canonical_path.read_bytes() == real_manifest_bytes


def test_artifact_store_rejects_a_sha_the_pool_was_not_pinned_to(
    fake_executor, tmp_path, real_manifest_bytes
):
    """The one check that binds the artifact to this pool.

    Every other check is self-consistent: a valid descriptor for a different
    release of the same service and image passes all of them.
    """
    store = store_for(
        fake_executor, tmp_path, real_manifest_bytes, runtime_manifest_sha256="c" * 64
    )

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_rejected"


def test_artifact_store_rejects_a_checksum_that_does_not_match_the_bytes(
    fake_executor, tmp_path, real_manifest_bytes
):
    descriptor = descriptor_for(real_manifest_bytes, sha256="d" * 64)
    store = store_for(
        fake_executor, tmp_path, real_manifest_bytes, descriptor=descriptor,
        runtime_manifest_sha256="d" * 64,
    )

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_checksum_mismatch"


def test_artifact_store_rejects_a_foreign_dbt_version(
    fake_executor, tmp_path, real_manifest_bytes
):
    descriptor = descriptor_for(real_manifest_bytes, dbt_core_version="1.9.9")
    store = store_for(fake_executor, tmp_path, real_manifest_bytes, descriptor=descriptor)

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_dbt_version_mismatch"


def test_artifact_store_rejects_a_foreign_adapter(
    fake_executor, tmp_path, real_manifest_bytes
):
    descriptor = descriptor_for(real_manifest_bytes, adapter_type="snowflake")
    store = store_for(fake_executor, tmp_path, real_manifest_bytes, descriptor=descriptor)

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_adapter_mismatch"


@pytest.mark.parametrize(
    "field, value",
    [("service_name", "finance"), ("image_tag", "sha-other")],
)
def test_artifact_store_rejects_a_descriptor_for_another_pool(
    fake_executor, tmp_path, real_manifest_bytes, field, value
):
    descriptor = descriptor_for(real_manifest_bytes, **{field: value})
    store = store_for(fake_executor, tmp_path, real_manifest_bytes, descriptor=descriptor)

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_rejected"


def test_artifact_store_rejects_a_parse_context_it_cannot_reproduce(
    fake_executor, tmp_path, real_manifest_bytes
):
    """A worker that would parse differently must not reuse the artifact."""
    descriptor = descriptor_for(real_manifest_bytes)
    store = store_for(
        fake_executor, tmp_path, real_manifest_bytes, descriptor=descriptor,
        controller_context_json='{"command_dialect_sha256":"changed"}',
    )

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_parse_context_mismatch"


def test_artifact_store_rejects_malformed_msgpack(fake_executor, tmp_path):
    packed = b"not msgpack at all"
    descriptor = descriptor_for_bytes(packed)
    store = store_for(fake_executor, tmp_path, packed, descriptor=descriptor)

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_unreadable"


def descriptor_for_bytes(packed: bytes) -> dict:
    """A well-formed descriptor for bytes that are not a Manifest."""
    return {
        "format": FORMAT,
        "service_name": SERVICE_NAME,
        "release_id": "r1",
        "image_tag": IMAGE_TAG,
        "artifact_uri": "s3://continuo/service-1/r1/partial_parse.msgpack",
        "sha256": hashlib.sha256(packed).hexdigest(),
        "dbt_core_version": "1.12.0b1",
        "adapter_type": "postgres",
        "parse_context_sha256": "b" * 64,
    }


def test_artifact_store_rejects_a_manifest_without_the_service_nodes(
    fake_executor, tmp_path
):
    packed = Manifest().to_msgpack()
    descriptor = descriptor_for(packed, adapter_type="postgres", dbt_core_version="1.12.0b1")
    store = store_for(fake_executor, tmp_path, packed, descriptor=descriptor)

    with pytest.raises(InitializationError) as caught:
        store.load()

    assert caught.value.code == "runtime_manifest_service_nodes_missing"


def test_artifact_store_never_parses_the_project(
    fake_executor, tmp_path, real_manifest_bytes, monkeypatch
):
    """Hydration reads the artifact; it must never fall back to a full parse."""
    def forbidden(*args, **kwargs):
        raise AssertionError("ManifestLoader.get_full_manifest must not run")

    monkeypatch.setattr(
        "dbt.parser.manifest.ManifestLoader.get_full_manifest", forbidden
    )

    loaded = store_for(fake_executor, tmp_path, real_manifest_bytes).load()

    assert loaded.manifest.nodes[UNIQUE_ID].name == "table_a"


def test_a_rejected_artifact_does_not_fall_back_to_parsing(
    fake_executor, tmp_path, real_manifest_bytes, monkeypatch
):
    """The no-fallback rule at the moment it would be tempting to break it."""
    def forbidden(*args, **kwargs):
        raise AssertionError("ManifestLoader.get_full_manifest must not run")

    monkeypatch.setattr(
        "dbt.parser.manifest.ManifestLoader.get_full_manifest", forbidden
    )
    store = store_for(
        fake_executor, tmp_path, real_manifest_bytes, runtime_manifest_sha256="c" * 64
    )

    with pytest.raises(InitializationError):
        store.load()
