"""Uploads a python node's validation spec to S3 and returns its s3:// URI.

The spec is what the validation Job fetches in place of compiled SQL: the
node's declared reads (rewritten to the candidate schema) to bind-check, plus
the output columns and physical-layout config to create the empty table from.
Its shape is the published continuo-python-runtime image's validation contract.

Serialized with sorted keys so equal content always produces identical bytes.
"""
import json

from adapters.candidate_object_key import candidate_object_key


class CandidateSpecUploader:
    def __init__(self, s3_client, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def upload(self, release_id: str, unique_id: str, spec: dict) -> str:
        key = candidate_object_key(release_id, unique_id, "json")
        body = json.dumps(spec, sort_keys=True, indent=2).encode("utf-8")
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body)
        return f"s3://{self._bucket}/{key}"
