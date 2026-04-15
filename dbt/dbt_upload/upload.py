"""Manifest filtering and S3 upload logic."""
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def filter_manifest(service_dir: str) -> None:
    """Remove non-model/seed nodes and local_stub-tagged nodes from manifest.json."""
    manifest_path = os.path.join(service_dir, "target", "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["nodes"] = {
        k: v
        for k, v in manifest["nodes"].items()
        if v.get("resource_type") in ("model", "seed")
        and "local_stub" not in v.get("tags", [])
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)


def upload_manifest(
    s3_client, service_dir: str, env: str, bucket: str
) -> bool:
    """Upload target/manifest.json to S3. Returns True on success."""
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    key = f"{env}/manifest/{service_name}/manifest.json"
    s3_client.upload_file(manifest_path, bucket, key)
    logger.info("Uploaded %s -> s3://%s/%s", service_name, bucket, key)
    return True


def upload_services(
    service_dirs: list[str], target_config: dict
) -> tuple[list[str], list[str]]:
    """Filter and upload manifests for each service directory.

    Returns (succeeded_dirs, failed_dirs).
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=target_config["endpoint_url"],
        aws_access_key_id=target_config["access_key_id"],
        aws_secret_access_key=target_config["secret_access_key"],
        region_name=target_config["region"],
    )

    env = target_config["env"]
    bucket = target_config["bucket"]

    succeeded: list[str] = []
    failed: list[str] = []

    for service_dir in service_dirs:
        service_name = os.path.basename(service_dir)
        logger.info("Uploading %s", service_name)

        try:
            filter_manifest(service_dir)
        except FileNotFoundError:
            logger.error("No compiled manifest for %s", service_name)
            failed.append(service_dir)
            continue

        if upload_manifest(s3_client, service_dir, env, bucket):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
