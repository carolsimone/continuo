import logging
import os
import uuid
import redis
from config.config import (
    REDIS_URL, REDIS_STREAM, REDIS_GROUP,
    REGISTRY_PATH, MANIFESTS_BASE,
    S3_ENDPOINT_URL, S3_BUCKET, S3_ENV,
    SCHEDULES_LOADED_STREAM, MANIFEST_LOADED_STREAM,
    validate,
)
from adapters.redis.publisher import SchedulesLoadedPublisher, ManifestLoadedPublisher
from adapters.filesystem.registry_repository import FilesystemRegistryRepository
from adapters.redis.consumer import Consumer
from adapters.sources.local import LocalFilesystemSource
from adapters.sources.s3 import S3Source
from service.manifest_handler import ManifestHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    validate()
    logger.info("manifest-controller starting")

    import boto3  # imported inside main() to avoid module-level side effects in tests

    registry_repo = FilesystemRegistryRepository(REGISTRY_PATH)

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    sources = {
        "local": lambda: LocalFilesystemSource(MANIFESTS_BASE),
        "s3":    lambda: S3Source(bucket=S3_BUCKET, env=S3_ENV, s3_client=s3_client),
    }

    redis_client = redis.from_url(REDIS_URL, decode_responses=False)

    schedules_publisher = SchedulesLoadedPublisher(redis_client, SCHEDULES_LOADED_STREAM)
    manifest_publisher = ManifestLoadedPublisher(redis_client, MANIFEST_LOADED_STREAM)

    def handle_event(source_name: str) -> None:
        source = sources[source_name]()
        try:
            # Step 1: load manifests, resolve deps, publish manifest.loaded:v1 event
            schedule_names, manifest_versions = ManifestHandler(
                source=source,
                manifest_publisher=manifest_publisher,
                registry_repo=registry_repo,
            ).handle()

            # Step 2: publish schedules.loaded:v1
            # Both steps must succeed before the consumer ACKs the trigger message.
            schedules_publisher.publish(
                event_id=str(uuid.uuid4()),
                schedule_names=schedule_names,
                manifest_versions=manifest_versions,
            )
        finally:
            source.cleanup()

    consumer = Consumer(
        redis_client=redis_client,
        stream_name=REDIS_STREAM,
        group_name=REDIS_GROUP,
        handler_factory=handle_event,
    )
    consumer.start()


if __name__ == "__main__":
    main()
