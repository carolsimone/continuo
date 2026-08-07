"""Uploads a node's compiled candidate SQL to S3 and returns its s3:// URI.

The compiled SQL is large and release-specific; it lives in S3 (key
candidate-sql/<release_id>/candidate_<unique_id>.sql) rather than inline in events or
Postgres. Empty SQL (seeds) uploads nothing and yields an empty URI.
"""


class CandidateSqlUploader:
    def __init__(self, s3_client, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def upload(self, release_id: str, unique_id: str, sql: str) -> str:
        if not sql:
            return ""
        key = f"candidate-sql/{release_id}/candidate_{unique_id}.sql"
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=sql.encode("utf-8"))
        return f"s3://{self._bucket}/{key}"
