#!/usr/bin/env python3
"""Shared S3 helpers for the compile sidecar (compile_uploader.py).

Provides URI parsing, env-var validation, and S3 client construction used by
compile_uploader.py.
"""
import os
import sys

import boto3


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an ``s3://bucket/key`` URI and return ``(bucket, key)``.

    Raises :class:`ValueError` for non-s3:// URIs, a missing bucket, or a
    missing/empty key (e.g. ``s3://bucket-only`` or ``s3://bucket/``).
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"invalid S3 URI (must start with s3://): {uri!r}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid S3 URI (missing bucket or key): {uri!r}")
    return bucket, key


def require_env(name: str, *, caller: str) -> str:
    """Return the value of env var *name*, or print an error and exit with code 2.

    *caller* is prepended to the error message so the originating script is
    identifiable in pod logs (e.g. ``"compile_uploader"``).
    """
    value = os.environ.get(name)
    if not value:
        print(f"{caller}: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return value


def make_s3_client():
    """Construct a boto3 S3 client from the four standard env vars.

    Reads ``S3_ENDPOINT_URL``, ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    and ``AWS_DEFAULT_REGION``. All four are optional at the env level; boto3
    applies its own credential resolution chain for any that are absent.

    Tests patch ``s3_common.boto3`` to intercept the underlying ``client`` call.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
