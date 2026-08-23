def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an s3:// URI into (bucket, prefix).

    A trailing slash is appended to the prefix when it is non-empty; a
    bucket-root URI like 's3://bucket/' yields an empty prefix.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI must start with s3://, got: {uri!r}")
    rest = uri[len("s3://"):]
    if not rest:
        raise ValueError(f"S3 URI missing bucket: {uri!r}")
    parts = rest.split("/", 1)
    bucket = parts[0]
    if not bucket:
        raise ValueError(f"S3 URI missing bucket: {uri!r}")
    prefix = parts[1] if len(parts) == 2 else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix
