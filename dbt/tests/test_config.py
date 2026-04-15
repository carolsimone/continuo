import os
import pytest
from dbt_upload.config import load_target, resolve_service_dirs


class TestLoadTarget:
    def test_loads_localstack_target(self, tmp_path, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        yaml_content = (
            "targets:\n"
            "  localstack:\n"
            "    endpoint_url: http://localstack:4566\n"
            "    bucket: continuo\n"
            "    region: us-east-1\n"
            "    env: local\n"
            "    access_key_id: test\n"
            "    secret_access_key: test\n"
        )
        targets_file = tmp_path / "targets.yaml"
        targets_file.write_text(yaml_content)

        target = load_target(str(targets_file), "localstack")

        assert target["endpoint_url"] == "http://localstack:4566"
        assert target["bucket"] == "continuo"
        assert target["region"] == "us-east-1"
        assert target["env"] == "local"
        assert target["access_key_id"] == "test"
        assert target["secret_access_key"] == "test"

    def test_env_vars_override_yaml_credentials(self, tmp_path, monkeypatch):
        yaml_content = (
            "targets:\n"
            "  localstack:\n"
            "    endpoint_url: http://localstack:4566\n"
            "    bucket: continuo\n"
            "    region: us-east-1\n"
            "    env: local\n"
            "    access_key_id: yaml-key\n"
            "    secret_access_key: yaml-secret\n"
        )
        targets_file = tmp_path / "targets.yaml"
        targets_file.write_text(yaml_content)

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

        target = load_target(str(targets_file), "localstack")

        assert target["access_key_id"] == "env-key"
        assert target["secret_access_key"] == "env-secret"

    def test_missing_yaml_credentials_uses_env_vars(self, tmp_path, monkeypatch):
        yaml_content = (
            "targets:\n"
            "  hetzner:\n"
            "    endpoint_url: https://nbg1.your-objectstorage.com\n"
            "    bucket: continuo-dev\n"
            "    region: eu-central-1\n"
            "    env: dev\n"
        )
        targets_file = tmp_path / "targets.yaml"
        targets_file.write_text(yaml_content)

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hetzner-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "hetzner-secret")

        target = load_target(str(targets_file), "hetzner")

        assert target["access_key_id"] == "hetzner-key"
        assert target["secret_access_key"] == "hetzner-secret"

    def test_unknown_target_raises(self, tmp_path):
        yaml_content = "targets:\n  localstack:\n    endpoint_url: http://localhost\n"
        targets_file = tmp_path / "targets.yaml"
        targets_file.write_text(yaml_content)

        with pytest.raises(ValueError, match="Unknown target 'nope'"):
            load_target(str(targets_file), "nope")

    def test_missing_credentials_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        yaml_content = (
            "targets:\n"
            "  hetzner:\n"
            "    endpoint_url: https://nbg1.your-objectstorage.com\n"
            "    bucket: continuo-dev\n"
            "    region: eu-central-1\n"
            "    env: dev\n"
        )
        targets_file = tmp_path / "targets.yaml"
        targets_file.write_text(yaml_content)

        with pytest.raises(ValueError, match="requires credentials"):
            load_target(str(targets_file), "hetzner")


class TestResolveServiceDirs:
    def test_explicit_paths(self, tmp_path):
        svc1 = tmp_path / "service-1"
        svc2 = tmp_path / "service-2"
        svc1.mkdir()
        svc2.mkdir()

        result = resolve_service_dirs([str(svc1), str(svc2)], None)

        assert result == [str(svc1), str(svc2)]

    def test_services_dir_discovers_subdirs(self, tmp_path):
        (tmp_path / "service-a").mkdir()
        (tmp_path / "service-b").mkdir()
        (tmp_path / "not-a-dir.txt").touch()

        result = resolve_service_dirs(None, str(tmp_path))

        assert result == [
            str(tmp_path / "service-a"),
            str(tmp_path / "service-b"),
        ]

    def test_neither_argument_raises(self):
        with pytest.raises(ValueError, match="Provide either"):
            resolve_service_dirs(None, None)
