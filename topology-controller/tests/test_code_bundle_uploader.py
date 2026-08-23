import json
from unittest.mock import MagicMock

from adapters.code_bundle_uploader import CodeBundleUploader


def test_uploads_bundle_json_and_returns_uri():
    s3 = MagicMock()
    up = CodeBundleUploader(s3, "continuo-bucket")
    uri = up.upload("rel-1", {"contract_version": 1, "release_id": "rel-1"})
    assert uri == "s3://continuo-bucket/code-bundles/rel-1/bundle.json"
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "continuo-bucket"
    assert kwargs["Key"] == "code-bundles/rel-1/bundle.json"
    assert json.loads(kwargs["Body"].decode()) == {"contract_version": 1, "release_id": "rel-1"}
    assert kwargs["ContentType"] == "application/json"
