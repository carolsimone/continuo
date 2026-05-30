"""Manifest filtering and S3 upload logic."""
import json
import logging
import os
import re
import tempfile
from pathlib import Path

import boto3

from dbt_upload.service_metadata import (
    parse_image_tag_env,
    write_service_metadata_json,
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
    s3_client,
    service_dir: str,
    env: str,
    bucket: str,
    image_tag: str = "",
    release_id: str = "",
) -> bool:
    """Upload target/manifest.json to S3. Returns True on success.

    Two layouts:
    - Per-release (release_id set): uploads to a fresh per-release prefix
      releases/{release_id}/manifests/{service_name}/manifest_v1.json. The
      filename is always v1 (the prefix is fresh per release, so no version
      lookup is needed) and no service_metadata.json sidecar is written —
      image tags travel in the POST /releases body, not in S3.
    - Legacy (release_id empty): checks the current highest manifest_v{N}.json
      in the service S3 prefix and uploads as manifest_v{N+1}.json. If image_tag
      is provided, also writes and uploads a service_metadata.json sidecar.
    """
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    if release_id:
        key = f"releases/{release_id}/manifests/{service_name}/manifest_v1.json"
        try:
            s3_client.upload_file(manifest_path, bucket, key)
        except Exception:
            logger.exception("S3 upload failed for %s", service_name)
            return False
        logger.info("Uploaded %s -> s3://%s/%s", service_name, bucket, key)
        return True

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
        with tempfile.TemporaryDirectory() as _tmp:
            meta_dir = Path(_tmp)
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
    service_dirs: list[str], target_config: dict, release_id: str = ""
) -> tuple[list[str], list[str]]:
    """Filter and upload manifests for each service directory.

    When release_id is set, manifests go to the per-release prefix
    releases/{release_id}/manifests/{service}/manifest_v1.json and no
    image_tag is required (tags travel in the POST /releases body). When
    release_id is empty, the legacy {env}/manifest/{service}/manifest_v{N}.json
    layout is used and every service must have an image_tag for its sidecar.

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

        # On the legacy path, refuse to upload before image_tag is known: a
        # successful manifest upload without a sidecar poisons the snapshot —
        # manifest-controller propagates image_tag="" and the run fails at
        # deployment time on every task. The per-release path carries no sidecar
        # (tags travel in the POST /releases body), so it needs no image_tag.
        image_tag = image_tag_map.get(service_name, "")
        if not release_id and not image_tag:
            logger.error(
                "image_tag missing for %s — set IMAGE_TAG_PER_SERVICE=%s=<tag>,...; refusing to upload",
                service_name, service_name,
            )
            failed.append(service_dir)
            continue

        try:
            filter_manifest(service_dir)
        except FileNotFoundError:
            logger.error("No compiled manifest for %s", service_name)
            failed.append(service_dir)
            continue

        if upload_manifest(
            s3_client, service_dir, env, bucket,
            image_tag=image_tag, release_id=release_id,
        ):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
