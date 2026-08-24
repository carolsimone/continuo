import json
import logging
import threading
import redis
from config.config import (
    REDIS_URL,
    HTTP_PORT,
    S3_ENDPOINT_URL, S3_BUCKET, S3_ENV,
    AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    RELEASE_REQUESTED_STREAM, RELEASE_REQUESTED_GROUP,
    MANIFEST_LOADED_CANDIDATE_STREAM,
    validate,
    warehouse_dialect,
)
from adapters.candidate_spec_uploader import CandidateSpecUploader
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.code_bundle_uploader import CodeBundleUploader
from adapters.health.server import start_health_server
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.redis.consumer import Consumer
from adapters.sources.s3 import S3Source
from adapters.sources.s3_uri import parse_s3_uri
from domain.model import ManifestKind, ManifestRequest, Runtime
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
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
    logger.info("topology-controller starting (candidate consumer)")

    import boto3  # imported inside main() to avoid module-level side effects in tests

    # All credential values come from config, where validate() has already
    # required them: a missing credential fails the boot instead of falling
    # back to a placeholder that breaks on the first S3 call.
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    redis_client = redis.from_url(REDIS_URL, decode_responses=False)

    # Candidate-parse flow (release.requested:v1 -> manifest.loaded.candidate:v1).
    candidate_publisher = CandidateManifestPublisher(
        redis_client, MANIFEST_LOADED_CANDIDATE_STREAM,
    )
    candidate_uploader = CandidateSqlUploader(s3_client, S3_BUCKET)
    candidate_spec_uploader = CandidateSpecUploader(s3_client, S3_BUCKET)
    code_bundle_uploader = CodeBundleUploader(s3_client, S3_BUCKET)

    # Resolved once at boot: validate() has already rejected an unsupported
    # engine, so every release parses and re-renders SQL for the warehouse this
    # install actually targets.
    dialect = warehouse_dialect()
    logger.info("topology-controller SQL dialect: %s", dialect)

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
        requests: list[ManifestRequest] = []
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
            # Only an ABSENT kind defaults to dbt — that is the compatibility
            # path for producers that predate python support. Any value the
            # producer actually set is passed through verbatim, empty string
            # included, so the handler reports it as a permanent
            # UnknownManifestKind failure the operator sees. Defaulting a
            # present-but-invalid value would parse a python contract with the
            # dbt parser and misreport it as MalformedManifest, sending the
            # operator to the wrong artifact.
            requests.append(ManifestRequest(
                service=svc,
                key=key.rstrip("/"),
                kind=entry.get("kind", ManifestKind.DBT),
            ))
        if len(set(buckets)) > 1:
            raise ValueError(
                f"release.requested:v1 manifest_keys span multiple buckets: {set(buckets)}"
            )
        shared_bucket = buckets[0] if buckets else S3_BUCKET
        source = S3Source(bucket=shared_bucket, env=S3_ENV, s3_client=s3_client, keys=requests)
        # Cleanup is owned by CandidateManifestHandler.handle() via its own finally block.
        CandidateManifestHandler(
            source=source,
            publisher=candidate_publisher,
            bundle_uploader=code_bundle_uploader,
            artifact_builders={
                Runtime.DBT: DbtSqlArtifactBuilder(candidate_uploader),
                Runtime.PYTHON: PythonSpecArtifactBuilder(candidate_spec_uploader),
            },
            dialect=dialect,
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

    # Backstop for the consumer loop's own retry-on-error handling: exposes
    # /health and /ready wired to candidate_consumer.last_heartbeat, so if the
    # loop ever stops making progress — this bug recurring in a different
    # shape, a future bug, anything — Kubernetes' liveness probe notices and
    # restarts the pod instead of it running "1/1 Ready" while silently dead.
    start_health_server(HTTP_PORT, candidate_consumer, candidate_thread)

    # Park the main thread on the candidate consumer loop; on SIGTERM the
    # process exits and the daemon thread is abandoned.
    candidate_thread.join()


if __name__ == "__main__":
    main()
