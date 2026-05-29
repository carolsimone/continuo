import json
import logging
import os
import threading
import redis
from config.config import (
    REDIS_URL, REDIS_STREAM, REDIS_GROUP,
    REGISTRY_PATH, MANIFESTS_BASE,
    S3_ENDPOINT_URL, S3_BUCKET, S3_ENV,
    MANIFEST_LOADED_STREAM,
    RELEASE_REQUESTED_STREAM, RELEASE_REQUESTED_GROUP,
    MANIFEST_LOADED_CANDIDATE_STREAM,
    validate,
)
from adapters.redis.publisher import ManifestLoadedPublisher
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.filesystem.registry_repository import FilesystemRegistryRepository
from adapters.redis.consumer import Consumer
from adapters.sources.local import LocalFilesystemSource
from adapters.sources.s3 import S3Source
from adapters.sources.s3_uri import parse_s3_uri
from service.manifest_handler import ManifestHandler
from service.candidate_manifest_handler import CandidateManifestHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_KNOWN_SOURCES = {"local", "s3"}


def _decode_field(fields: dict, name: str) -> str | None:
    raw = fields.get(name.encode()) or fields.get(name)
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else raw


def main() -> None:
    validate()
    logger.info("manifest-controller starting (two consumers: legacy + candidate)")

    import boto3  # imported inside main() to avoid module-level side effects in tests

    registry_repo = FilesystemRegistryRepository(REGISTRY_PATH)

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    redis_client = redis.from_url(REDIS_URL, decode_responses=False)

    # Legacy update.graph:v1 -> manifest.loaded:v1 wiring.
    manifest_publisher = ManifestLoadedPublisher(redis_client, MANIFEST_LOADED_STREAM)
    legacy_sources = {
        "local": lambda: LocalFilesystemSource(MANIFESTS_BASE),
        "s3":    lambda: S3Source(bucket=S3_BUCKET, env=S3_ENV, s3_client=s3_client),
    }

    def handle_update_graph(fields: dict) -> None:
        source_name = _decode_field(fields, "source")
        if not source_name:
            raise ValueError("update.graph:v1 message missing source")
        if source_name not in _KNOWN_SOURCES:
            raise ValueError(f"update.graph:v1 unknown source '{source_name}'")
        source = legacy_sources[source_name]()
        try:
            ManifestHandler(
                source=source,
                manifest_publisher=manifest_publisher,
                registry_repo=registry_repo,
            ).handle()
        finally:
            source.cleanup()

    # Candidate-parse flow (release.requested:v1 -> manifest.loaded.candidate:v1).
    candidate_publisher = CandidateManifestPublisher(
        redis_client, MANIFEST_LOADED_CANDIDATE_STREAM,
    )

    def handle_release_requested(fields: dict) -> None:
        payload_raw = _decode_field(fields, "payload")
        if not payload_raw:
            raise ValueError("release.requested:v1 message missing payload")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"release.requested:v1 payload not valid JSON: {exc}") from exc
        release_id = payload.get("release_id")
        manifests_uri = payload.get("manifests_uri")
        if not release_id or not manifests_uri:
            raise ValueError(
                "release.requested:v1 payload missing release_id or manifests_uri",
            )
        bucket, prefix = parse_s3_uri(manifests_uri)
        source = S3Source(bucket=bucket, env=S3_ENV, s3_client=s3_client, prefix=prefix)
        CandidateManifestHandler(source=source, publisher=candidate_publisher).handle(
            release_id=release_id,
        )

    legacy_consumer = Consumer(
        redis_client=redis_client,
        stream_name=REDIS_STREAM,
        group_name=REDIS_GROUP,
        message_handler=handle_update_graph,
    )
    candidate_consumer = Consumer(
        redis_client=redis_client,
        stream_name=RELEASE_REQUESTED_STREAM,
        group_name=RELEASE_REQUESTED_GROUP,
        message_handler=handle_release_requested,
    )

    legacy_thread = threading.Thread(
        target=legacy_consumer.start, daemon=True, name="consumer-update-graph",
    )
    candidate_thread = threading.Thread(
        target=candidate_consumer.start, daemon=True, name="consumer-release-requested",
    )
    legacy_thread.start()
    candidate_thread.start()
    legacy_thread.join()
    candidate_thread.join()


if __name__ == "__main__":
    main()
