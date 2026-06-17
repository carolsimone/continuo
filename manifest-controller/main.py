import json
import logging
import os
import threading
import redis
from config.config import (
    REDIS_URL,
    S3_ENDPOINT_URL, S3_BUCKET, S3_ENV,
    RELEASE_REQUESTED_STREAM, RELEASE_REQUESTED_GROUP,
    MANIFEST_LOADED_CANDIDATE_STREAM,
    validate,
)
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.redis.consumer import Consumer
from adapters.sources.s3 import S3Source
from adapters.sources.s3_uri import parse_s3_uri
from service.candidate_manifest_handler import CandidateManifestHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def _decode_field(fields: dict, name: str) -> str | None:
    raw = fields.get(name.encode()) or fields.get(name)
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else raw


def main() -> None:
    validate()
    logger.info("manifest-controller starting (candidate consumer)")

    import boto3  # imported inside main() to avoid module-level side effects in tests

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    redis_client = redis.from_url(REDIS_URL, decode_responses=False)

    # Candidate-parse flow (release.requested:v1 -> manifest.loaded.candidate:v1).
    candidate_publisher = CandidateManifestPublisher(
        redis_client, MANIFEST_LOADED_CANDIDATE_STREAM,
    )
    candidate_uploader = CandidateSqlUploader(s3_client, S3_BUCKET)

    def handle_release_requested(fields: dict) -> None:
        payload_raw = _decode_field(fields, "payload")
        if not payload_raw:
            raise ValueError("release.requested:v1 message missing payload")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"release.requested:v1 payload not valid JSON: {exc}") from exc
        release_id = payload.get("release_id")
        manifest_keys_raw = payload.get("manifest_keys")
        if not release_id or manifest_keys_raw is None:
            raise ValueError(
                "release.requested:v1 payload missing release_id or manifest_keys",
            )
        # All entries must share a single bucket; derive it from the first URI and
        # assert the rest agree so misrouted multi-bucket payloads are caught early.
        # Each entry must carry a non-empty "service" field; a missing or empty
        # service is treated as a permanent malformed-payload error (not ACKed) so
        # the service-mismatch/empty-manifest validation in the handler cannot be
        # silently bypassed.
        buckets = []
        keyed_pairs: list[tuple[str, str]] = []
        for entry in manifest_keys_raw:
            svc = entry.get("service") if isinstance(entry, dict) else None
            if not svc:
                raise ValueError(
                    "release.requested:v1 manifest_keys entry missing or empty 'service' field"
                )
            bucket, key = parse_s3_uri(entry["s3_uri"])
            buckets.append(bucket)
            # parse_s3_uri appends a trailing slash to all non-empty paths; strip it
            # because object keys never end with "/" in S3.
            keyed_pairs.append((svc, key.rstrip("/")))
        if len(set(buckets)) > 1:
            raise ValueError(
                f"release.requested:v1 manifest_keys span multiple buckets: {set(buckets)}"
            )
        shared_bucket = buckets[0] if buckets else S3_BUCKET
        source = S3Source(bucket=shared_bucket, env=S3_ENV, s3_client=s3_client, keys=keyed_pairs)
        # Cleanup is owned by CandidateManifestHandler.handle() via its own finally block.
        CandidateManifestHandler(
            source=source,
            publisher=candidate_publisher,
            uploader=candidate_uploader,
        ).handle(release_id=release_id)

    candidate_consumer = Consumer(
        redis_client=redis_client,
        stream_name=RELEASE_REQUESTED_STREAM,
        group_name=RELEASE_REQUESTED_GROUP,
        message_handler=handle_release_requested,
    )

    candidate_thread = threading.Thread(
        target=candidate_consumer.start, daemon=True, name="consumer-release-requested",
    )
    candidate_thread.start()
    # Park the main thread on the candidate consumer loop; on SIGTERM the
    # process exits and the daemon thread is abandoned.
    candidate_thread.join()


if __name__ == "__main__":
    main()
