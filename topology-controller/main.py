import logging
import threading
import redis
from config.config import (
    REDIS_URL,
    HTTP_PORT,
    SERVICE_NAME,
    S3_ENDPOINT_URL, S3_BUCKET, S3_ENV,
    AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    RELEASE_REQUESTED_STREAM, RELEASE_REQUESTED_GROUP,
    MANIFEST_LOADED_CANDIDATE_STREAM,
    log_level,
    validate,
    warehouse_dialect,
)
from adapters.candidate_spec_uploader import CandidateSpecUploader
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.code_bundle_uploader import CodeBundleUploader
from adapters.health.server import start_health_server
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.redis.consumer import Consumer
from adapters.redis.release_requested_binding import parse_release_requested
from adapters.sources.s3 import S3Source
from domain.model import Runtime
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler

logger = logging.getLogger(__name__)


def main() -> None:
    # The root logger takes the level LOG_LEVEL names; log_level() refuses an
    # unknown name, which stops the process before anything else starts.
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger().setLevel(log_level())
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
        message = parse_release_requested(fields, S3_BUCKET)
        source = S3Source(bucket=message.bucket, env=S3_ENV, s3_client=s3_client, keys=message.requests)
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
            image_tags=message.image_tags,
        ).handle(release_id=message.release_id)

    candidate_consumer = Consumer(
        redis_client=redis_client,
        stream_name=RELEASE_REQUESTED_STREAM,
        group_name=RELEASE_REQUESTED_GROUP,
        message_handler=handle_release_requested,
        service_name=SERVICE_NAME,
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
