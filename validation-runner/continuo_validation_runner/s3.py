"""S3 helpers for the validation runner.

``parse_s3_uri`` and ``make_s3_client`` back the runner's candidate-SQL fetch
from ``CANDIDATE_SQL_URI``.
"""
import os
from typing import Any

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


def make_s3_client() -> Any:
    """Construct a boto3 S3 client, letting boto3 resolve credentials itself.

    Only ``S3_ENDPOINT_URL`` (when set and non-empty, e.g. for MinIO) is passed
    explicitly. Credentials and region are left for boto3's own chain, which
    natively reads ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    ``AWS_SESSION_TOKEN``, and ``AWS_DEFAULT_REGION`` from the environment, and
    falls through to IAM roles / web identity / instance profiles when those are
    absent. Passing raw (possibly blank or missing) env values as explicit
    keyword args would instead select empty/explicit credentials or drop
    temporary-credential session tokens, breaking that chain.

    Tests patch ``s3.boto3`` to intercept the underlying ``client`` call.
    """
    kwargs: dict[str, Any] = {}
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)
