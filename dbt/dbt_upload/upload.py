"""Manifest filtering and S3 upload logic."""
import json
import logging
import os
import re
from pathlib import Path

import boto3

from dbt_upload.service_metadata import (
    parse_image_tag_env,
    write_service_metadata_json,
    MissingImageTagError,
)

logger = logging.getLogger(__name__)

_VERSION_RE = re.compile(r'^manifest_(v\d+)\.json$')


def next_version(s3_client, bucket: str, prefix: str) -> int:
    """Return the next version int for a service S3 prefix.

    Lists all objects under prefix, finds the highest manifest_v{N}.json,
    and returns N+1. Returns 1 if no versioned manifest exists yet.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    max_v = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            m = _VERSION_RE.match(filename)
            if m:
                n = int(m.group(1)[1:])  # "v3" → 3
                max_v = max(max_v, n)
    return max_v + 1


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
    s3_client, service_dir: str, env: str, bucket: str, image_tag: str = ""
) -> bool:
    """Upload target/manifest.json to S3 with an auto-incremented version key.

    Checks the current highest manifest_v{N}.json in the service S3 prefix
    and uploads as manifest_v{N+1}.json. Returns True on success.

    If image_tag is provided, also writes and uploads service_metadata.json
    alongside the versioned manifest.
    """
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    prefix = f"{env}/manifest/{service_name}/"
    version = next_version(s3_client, bucket, prefix)
    key = f"{env}/manifest/{service_name}/manifest_v{version}.json"
    try:
        s3_client.upload_file(manifest_path, bucket, key)
    except Exception:
        logger.exception("S3 upload failed for %s", service_name)
        return False
    logger.info("Uploaded %s -> s3://%s/%s (v%d)", service_name, bucket, key, version)

    # Write and upload service_metadata.json sidecar if image_tag is provided.
    if image_tag:
        import tempfile
        meta_dir = Path(tempfile.mkdtemp())
        write_service_metadata_json(
            out_dir=meta_dir,
            service_name=service_name,
            manifest_version=f"v{version}",
            image_tag=image_tag,
        )
        meta_key = f"{env}/manifest/{service_name}/service_metadata.json"
        s3_client.upload_file(str(meta_dir / "service_metadata.json"), bucket, meta_key)
        logger.info("Uploaded service_metadata.json -> s3://%s/%s", bucket, meta_key)

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

    image_tag_map = parse_image_tag_env(os.getenv("IMAGE_TAG_PER_SERVICE", ""))

    for service_dir in service_dirs:
        service_name = os.path.basename(service_dir)
        logger.info("Uploading %s", service_name)

        try:
            filter_manifest(service_dir)
        except FileNotFoundError:
            logger.error("No compiled manifest for %s", service_name)
            failed.append(service_dir)
            continue

        image_tag = image_tag_map.get(service_name, "")
        if upload_manifest(s3_client, service_dir, env, bucket, image_tag=image_tag):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
