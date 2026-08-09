import json
from unittest.mock import MagicMock

from adapters.candidate_spec_uploader import CandidateSpecUploader


def test_uploads_the_spec_under_the_shared_candidate_prefix():
    """The .json spec lives beside the .sql objects on purpose: release-controller's
    retention job and the bucket lifecycle rule both prune by that prefix."""
    s3 = MagicMock()
    uri = CandidateSpecUploader(s3, "continuo").upload(
        release_id="rel-1", unique_id="test_schema.py_metrics",
        spec={"reads": ["select 1"], "output_columns": [], "config": {}},
    )

    assert uri == "s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json"
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Key"] == "candidate-sql/rel-1/candidate_test_schema.py_metrics.json"
    assert json.loads(kwargs["Body"].decode()) == {
        "reads": ["select 1"], "output_columns": [], "config": {},
    }


def test_the_serialized_body_is_deterministic():
    """Two uploads of equal content must produce identical bytes, so a
    re-triggered release does not churn the object."""
    s3 = MagicMock()
    uploader = CandidateSpecUploader(s3, "continuo")
    spec_a = {"config": {"b": 1, "a": 2}, "reads": ["x"], "output_columns": []}
    spec_b = {"reads": ["x"], "output_columns": [], "config": {"a": 2, "b": 1}}

    uploader.upload(release_id="r", unique_id="s.t", spec=spec_a)
    first = s3.put_object.call_args.kwargs["Body"]
    uploader.upload(release_id="r", unique_id="s.t", spec=spec_b)

    assert s3.put_object.call_args.kwargs["Body"] == first
