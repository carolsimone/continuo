import json
from unittest.mock import MagicMock
from dbt_upload.upload import filter_manifest, next_version, upload_manifest, upload_services


class TestFilterManifest:
    def test_keeps_models_and_seeds(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)

        manifest = {
            "nodes": {
                "model.svc.table_a": {"resource_type": "model", "name": "table_a", "tags": []},
                "seed.svc.seed_1": {"resource_type": "seed", "name": "seed_1", "tags": []},
                "test.svc.test_1": {"resource_type": "test", "name": "test_1", "tags": []},
                "source.svc.src_1": {"resource_type": "source", "name": "src_1", "tags": []},
            }
        }
        (target_dir / "manifest.json").write_text(json.dumps(manifest))

        filter_manifest(str(service_dir))

        result = json.loads((target_dir / "manifest.json").read_text())
        assert set(result["nodes"].keys()) == {
            "model.svc.table_a",
            "seed.svc.seed_1",
        }

    def test_removes_local_stub_tagged_nodes(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)

        manifest = {
            "nodes": {
                "model.svc.real": {"resource_type": "model", "name": "real", "tags": []},
                "model.svc.stub": {"resource_type": "model", "name": "stub", "tags": ["local_stub"]},
            }
        }
        (target_dir / "manifest.json").write_text(json.dumps(manifest))

        filter_manifest(str(service_dir))

        result = json.loads((target_dir / "manifest.json").read_text())
        assert list(result["nodes"].keys()) == ["model.svc.real"]


class TestUploadManifest:
    def test_uploads_to_versioned_key_when_s3_is_empty(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]  # empty prefix → version 1
        s3.get_paginator.return_value = paginator

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is True
        s3.upload_file.assert_called_once_with(
            str(target_dir / "manifest.json"),
            "my-bucket",
            "dev/manifest/service-1/manifest_v1.json",
        )

    def test_increments_version_when_v1_exists(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [{"Key": "dev/manifest/service-1/manifest_v1.json"}]
        }]
        s3.get_paginator.return_value = paginator

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is True
        s3.upload_file.assert_called_once_with(
            str(target_dir / "manifest.json"),
            "my-bucket",
            "dev/manifest/service-1/manifest_v2.json",
        )

    def test_returns_false_when_manifest_missing(self, tmp_path):
        service_dir = tmp_path / "service-1"
        service_dir.mkdir()
        s3 = MagicMock()

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is False
        s3.upload_file.assert_not_called()
        s3.get_paginator.assert_not_called()


class TestUploadServices:
    def test_filters_and_uploads_each(self, tmp_path):
        for name in ["svc-1", "svc-2"]:
            target_dir = tmp_path / name / "target"
            target_dir.mkdir(parents=True)
            manifest = {
                "nodes": {
                    f"model.{name}.t": {"resource_type": "model", "name": "t", "tags": []},
                }
            }
            (target_dir / "manifest.json").write_text(json.dumps(manifest))

        target_config = {
            "endpoint_url": "http://localstack:4566",
            "bucket": "continuo",
            "region": "us-east-1",
            "env": "local",
            "access_key_id": "test",
            "secret_access_key": "test",
        }

        from unittest.mock import patch
        with patch("dbt_upload.upload.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3

            # Configure paginator to return no existing versions → each upload gets v1
            paginator = MagicMock()
            paginator.paginate.return_value = [{}]
            mock_s3.get_paginator.return_value = paginator

            succeeded, failed = upload_services(
                [str(tmp_path / "svc-1"), str(tmp_path / "svc-2")],
                target_config,
            )

        assert len(succeeded) == 2
        assert len(failed) == 0
        # Paginator called once per service (versioning path ran)
        assert mock_s3.get_paginator.call_count == 2
        # Both uploads used versioned keys
        uploaded_keys = [call.args[2] for call in mock_s3.upload_file.call_args_list]
        assert all("_v1" in key for key in uploaded_keys)


class TestNextVersion:
    def _make_s3(self, pages: list[dict]):
        """Return a mock s3 client whose paginator yields the given pages."""
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        s3.get_paginator.return_value = paginator
        return s3

    def test_returns_1_when_prefix_is_empty(self):
        s3 = self._make_s3([{}])  # one page, no Contents

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 1

    def test_returns_1_when_no_versioned_files(self):
        s3 = self._make_s3([{
            "Contents": [{"Key": "dev/manifest/service-1/manifest.json"}]
        }])

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 1

    def test_returns_max_plus_1(self):
        s3 = self._make_s3([{
            "Contents": [
                {"Key": "dev/manifest/service-1/manifest_v1.json"},
                {"Key": "dev/manifest/service-1/manifest_v3.json"},
            ]
        }])

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 4

    def test_single_existing_version(self):
        s3 = self._make_s3([{
            "Contents": [{"Key": "dev/manifest/service-1/manifest_v2.json"}]
        }])

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 3

    def test_passes_correct_bucket_and_prefix(self):
        s3 = self._make_s3([{}])

        result = next_version(s3, "continuo-dev", "dev/manifest/svc/")

        s3.get_paginator.assert_called_once_with("list_objects_v2")
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="continuo-dev", Prefix="dev/manifest/svc/"
        )
        assert result == 1
