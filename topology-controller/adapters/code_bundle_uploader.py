"""Uploads a release's code-bundle contract document to S3.

One JSON object per release at code-bundles/<release_id>/bundle.json. The
release-scoped subfolder keeps prefix deletes exact: release-controller prunes
code-bundles/<id>/ alongside candidate-sql/<id>/, and a flat key would let a
prefix delete of rel-1 also match rel-10.
"""
import json


class CodeBundleUploader:
    def __init__(self, s3_client, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def upload(self, release_id: str, bundle: dict) -> str:
        key = f"code-bundles/{release_id}/bundle.json"
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(bundle).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self._bucket}/{key}"
