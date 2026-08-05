from unittest.mock import MagicMock
from adapters.candidate_sql_uploader import CandidateSqlUploader


def test_upload_puts_object_and_returns_s3_uri():
    s3 = MagicMock()
    up = CandidateSqlUploader(s3, bucket="continuo")
    uri = up.upload(release_id="rel-abc-1", unique_id="public.orders", sql="SELECT 1")
    s3.put_object.assert_called_once_with(
        Bucket="continuo", Key="candidate-sql/rel-abc-1/candidate_public.orders.sql", Body=b"SELECT 1")
    assert uri == "s3://continuo/candidate-sql/rel-abc-1/candidate_public.orders.sql"


def test_empty_sql_returns_empty_uri_and_does_not_upload():
    s3 = MagicMock()
    up = CandidateSqlUploader(s3, bucket="continuo")
    assert up.upload(release_id="r", unique_id="n", sql="") == ""
    s3.put_object.assert_not_called()
