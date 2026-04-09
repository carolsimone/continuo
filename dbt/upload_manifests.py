#!/usr/bin/env python3
"""
Compile each dbt service and upload target/manifest.json to S3.

Usage:
  python upload_manifests.py [--services-dir ./services] [--env local]

Environment variables:
  S3_ENDPOINT_URL  default: http://localstack:4566
  S3_BUCKET        default: continuo
  S3_ENV           default: local  (overridden by --env)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""
import argparse
import json
import logging
import os
import subprocess
import sys

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localstack:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def compile_service(service_dir: str) -> bool:
    """Run `dbt compile` in service_dir. Returns True on success."""
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(
            "dbt compile failed for %s: %s",
            os.path.basename(service_dir),
            result.stderr.strip(),
        )
        return False
    return True


def filter_manifest(service_dir: str) -> None:
    """Remove local_stub models from manifest.json before upload; preserve seeds."""
    manifest_path = os.path.join(service_dir, "target", "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["nodes"] = {
        k: v for k, v in manifest["nodes"].items()
        if v.get("resource_type") in ("model", "seed")
        and "local_stub" not in v.get("tags", [])
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)


def upload_manifest(s3_client, service_dir: str, env: str, bucket: str) -> bool:
    """Upload target/manifest.json to S3. Returns True on success."""
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    key = f"{env}/manifest/{service_name}/manifest.json"
    s3_client.upload_file(manifest_path, bucket, key)
    logger.info("Uploaded %s → s3://%s/%s", service_name, bucket, key)
    return True


def main():
    parser = argparse.ArgumentParser(description="Compile dbt services and upload manifests to S3")
    parser.add_argument("--services-dir", default="./services",
                        help="Directory containing service subdirectories")
    parser.add_argument("--env", default=os.getenv("S3_ENV", "local"),
                        help="Environment prefix for S3 keys")
    args = parser.parse_args()

    bucket = os.getenv("S3_BUCKET", "continuo")
    s3_client = get_s3_client()

    services_dir = os.path.abspath(args.services_dir)
    service_dirs = sorted(
        os.path.join(services_dir, d)
        for d in os.listdir(services_dir)
        if os.path.isdir(os.path.join(services_dir, d))
    )

    succeeded = 0
    failed = 0

    for service_dir in service_dirs:
        service_name = os.path.basename(service_dir)
        logger.info("Processing %s", service_name)

        if not compile_service(service_dir):
            failed += 1
            continue

        filter_manifest(service_dir)

        if not upload_manifest(s3_client, service_dir, args.env, bucket):
            failed += 1
            continue

        succeeded += 1

    logger.info("Done: %d succeeded, %d failed", succeeded, failed)

    if succeeded == 0:
        logger.error("No services uploaded successfully — exiting with error")
        sys.exit(1)


if __name__ == "__main__":
    main()
