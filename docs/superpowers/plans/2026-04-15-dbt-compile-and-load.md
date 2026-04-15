# dbt Compile and Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace flat dbt scripts with a modular `dbt_upload` Python package exposing `compile`, `upload`, and `load` subcommands, with YAML-based S3 target profiles for switching between LocalStack and Hetzner Object Storage.

**Architecture:** A Python package (`dbt_upload`) with separate modules for compile, upload, and config. CLI is argparse with subparsers. S3 target profiles live in `targets.yaml`; credentials resolve env vars > YAML values. Runs containerized via `Dockerfile.upload`.

**Tech Stack:** Python 3.12, argparse, boto3, pyyaml, uv, Docker (dbt-base image)

**Spec:** `docs/superpowers/specs/2026-04-15-dbt-compile-and-load-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `dbt/dbt_upload/__init__.py` | Package marker (empty) |
| `dbt/dbt_upload/__main__.py` | Entry point — delegates to `cli.main()` |
| `dbt/dbt_upload/config.py` | `load_target()`, `resolve_service_dirs()` |
| `dbt/dbt_upload/compile.py` | `compile_service()`, `compile_services()` |
| `dbt/dbt_upload/upload.py` | `filter_manifest()`, `upload_manifest()`, `upload_services()` |
| `dbt/dbt_upload/cli.py` | Argparse subparsers: `compile`, `upload`, `load` |
| `dbt/targets.yaml` | Named S3 target profiles |
| `dbt/.env.hetzner` | Gitignored Hetzner credentials |
| `dbt/pyproject.toml` | Updated package metadata + pyyaml dep |
| `dbt/Dockerfile.upload` | Updated to install package, new entrypoint |
| `dbt/tests/test_upload.py` | Updated imports, same test logic |
| `docker-compose.yml:527-553` | Renamed service `dbt-compile-and-load` |
| `docker-compose.yml:428` | Updated depends_on reference |
| `.gitignore` | Add `.env.hetzner` |
| `deploy/app/values.yaml:27-33` | Fix endpoint + bucket |

---

### Task 1: config module + tests

**Files:**
- Create: `dbt/dbt_upload/__init__.py`
- Create: `dbt/dbt_upload/config.py`
- Create: `dbt/targets.yaml`
- Create: `dbt/tests/test_config.py`

- [ ] **Step 1: Create package directory and empty `__init__.py`**

```bash
mkdir -p dbt/dbt_upload
touch dbt/dbt_upload/__init__.py
```

- [ ] **Step 2: Create `targets.yaml`**

Create `dbt/targets.yaml`:

```yaml
targets:
  localstack:
    endpoint_url: http://localstack:4566
    bucket: continuo
    region: us-east-1
    env: local
    access_key_id: test
    secret_access_key: test

  hetzner:
    endpoint_url: https://nbg1.your-objectstorage.com
    bucket: continuo-dev
    region: eu-central-1
    env: dev
```

- [ ] **Step 3: Write failing tests for `config.py`**

Create `dbt/tests/test_config.py`:

```python
import os
import pytest
from dbt_upload.config import load_target, resolve_service_dirs


class TestLoadTarget:
    def test_loads_localstack_target(self, tmp_path):
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
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `cd dbt && uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dbt_upload'`

- [ ] **Step 5: Implement `config.py`**

Create `dbt/dbt_upload/config.py`:

```python
"""Target configuration loading and service directory resolution."""
import os
from pathlib import Path

import yaml


def load_target(targets_path: str, name: str) -> dict:
    """Load a named S3 target profile from a YAML file.

    Credential resolution: env vars AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    override any values in the YAML.
    """
    with open(targets_path) as f:
        data = yaml.safe_load(f)

    targets = data.get("targets", {})
    if name not in targets:
        available = ", ".join(sorted(targets.keys()))
        raise ValueError(
            f"Unknown target '{name}'. Available targets: {available}"
        )

    target = dict(targets[name])

    # Env vars override YAML credentials
    env_key = os.environ.get("AWS_ACCESS_KEY_ID")
    if env_key:
        target["access_key_id"] = env_key
    env_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if env_secret:
        target["secret_access_key"] = env_secret

    return target


def resolve_service_dirs(
    paths: list[str] | None, services_dir: str | None
) -> list[str]:
    """Resolve CLI arguments into a sorted list of absolute service directories.

    Either explicit paths or --services-dir must be provided, not both.
    """
    if paths:
        return [os.path.abspath(p) for p in paths]

    if services_dir:
        base = os.path.abspath(services_dir)
        return sorted(
            os.path.join(base, d)
            for d in os.listdir(base)
            if os.path.isdir(os.path.join(base, d))
        )

    raise ValueError(
        "Provide either service paths as positional arguments or --services-dir"
    )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd dbt && uv run pytest tests/test_config.py -v`
Expected: all 6 tests PASS

- [ ] **Step 7: Commit**

```bash
git add dbt/dbt_upload/__init__.py dbt/dbt_upload/config.py dbt/targets.yaml dbt/tests/test_config.py
git commit -m "feat(dbt): add config module with target loading and service dir resolution"
```

---

### Task 2: compile module + tests

**Files:**
- Create: `dbt/dbt_upload/compile.py`
- Create: `dbt/tests/test_compile.py`

- [ ] **Step 1: Write failing tests for `compile.py`**

Create `dbt/tests/test_compile.py`:

```python
from unittest.mock import patch, MagicMock
from dbt_upload.compile import compile_service, compile_services


class TestCompileService:
    @patch("dbt_upload.compile.subprocess.run")
    def test_returns_true_on_success(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0)
        service_dir = str(tmp_path / "service-1")

        assert compile_service(service_dir) is True

        mock_run.assert_called_once_with(
            ["dbt", "compile", "--profiles-dir", "."],
            cwd=service_dir,
            capture_output=True,
            text=True,
        )

    @patch("dbt_upload.compile.subprocess.run")
    def test_returns_false_on_failure(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=1, stderr="compile error")
        service_dir = str(tmp_path / "service-1")

        assert compile_service(service_dir) is False


class TestCompileServices:
    @patch("dbt_upload.compile.compile_service")
    def test_returns_succeeded_and_failed(self, mock_compile):
        mock_compile.side_effect = [True, False, True]
        dirs = ["/app/services/svc-1", "/app/services/svc-2", "/app/services/svc-3"]

        succeeded, failed = compile_services(dirs)

        assert succeeded == ["/app/services/svc-1", "/app/services/svc-3"]
        assert failed == ["/app/services/svc-2"]

    @patch("dbt_upload.compile.compile_service")
    def test_all_succeed(self, mock_compile):
        mock_compile.return_value = True
        dirs = ["/app/services/svc-1", "/app/services/svc-2"]

        succeeded, failed = compile_services(dirs)

        assert succeeded == ["/app/services/svc-1", "/app/services/svc-2"]
        assert failed == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd dbt && uv run pytest tests/test_compile.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dbt_upload.compile'`

- [ ] **Step 3: Implement `compile.py`**

Create `dbt/dbt_upload/compile.py`:

```python
"""dbt compile logic."""
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def compile_service(service_dir: str) -> bool:
    """Run `dbt compile` in service_dir. Returns True on success."""
    name = os.path.basename(service_dir)
    logger.info("Compiling %s", name)
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("dbt compile failed for %s: %s", name, result.stderr.strip())
        return False
    logger.info("Compiled %s successfully", name)
    return True


def compile_services(
    service_dirs: list[str],
) -> tuple[list[str], list[str]]:
    """Compile each service directory.

    Returns (succeeded_dirs, failed_dirs).
    """
    succeeded: list[str] = []
    failed: list[str] = []

    for service_dir in service_dirs:
        if compile_service(service_dir):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd dbt && uv run pytest tests/test_compile.py -v`
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add dbt/dbt_upload/compile.py dbt/tests/test_compile.py
git commit -m "feat(dbt): add compile module with compile_service and compile_services"
```

---

### Task 3: upload module + tests

**Files:**
- Create: `dbt/dbt_upload/upload.py`
- Create: `dbt/tests/test_upload_unit.py`

- [ ] **Step 1: Write failing tests for `upload.py`**

Create `dbt/tests/test_upload_unit.py`:

```python
import json
from unittest.mock import MagicMock
from dbt_upload.upload import filter_manifest, upload_manifest, upload_services


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
    def test_uploads_to_correct_key(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        s3 = MagicMock()

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is True
        s3.upload_file.assert_called_once_with(
            str(target_dir / "manifest.json"),
            "my-bucket",
            "dev/manifest/service-1/manifest.json",
        )

    def test_returns_false_when_manifest_missing(self, tmp_path):
        service_dir = tmp_path / "service-1"
        service_dir.mkdir()
        s3 = MagicMock()

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is False
        s3.upload_file.assert_not_called()


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

        # Patch boto3 inside upload module
        from unittest.mock import patch
        with patch("dbt_upload.upload.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3

            succeeded, failed = upload_services(
                [str(tmp_path / "svc-1"), str(tmp_path / "svc-2")],
                target_config,
            )

        assert len(succeeded) == 2
        assert len(failed) == 0
        assert mock_s3.upload_file.call_count == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd dbt && uv run pytest tests/test_upload_unit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dbt_upload.upload'`

- [ ] **Step 3: Implement `upload.py`**

Create `dbt/dbt_upload/upload.py`:

```python
"""Manifest filtering and S3 upload logic."""
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def filter_manifest(service_dir: str) -> None:
    """Remove non-model/seed nodes and local_stub-tagged nodes from manifest.json."""
    manifest_path = os.path.join(service_dir, "target", "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["nodes"] = {
        k: v
        for k, v in manifest["nodes"].items()
        if v.get("resource_type") in ("model", "seed")
        and "local_stub" not in v.get("tags", [])
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)


def upload_manifest(
    s3_client, service_dir: str, env: str, bucket: str
) -> bool:
    """Upload target/manifest.json to S3. Returns True on success."""
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    key = f"{env}/manifest/{service_name}/manifest.json"
    s3_client.upload_file(manifest_path, bucket, key)
    logger.info("Uploaded %s -> s3://%s/%s", service_name, bucket, key)
    return True


def upload_services(
    service_dirs: list[str], target_config: dict
) -> tuple[list[str], list[str]]:
    """Filter and upload manifests for each service directory.

    Returns (succeeded_dirs, failed_dirs).
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=target_config["endpoint_url"],
        aws_access_key_id=target_config["access_key_id"],
        aws_secret_access_key=target_config["secret_access_key"],
        region_name=target_config["region"],
    )

    env = target_config["env"]
    bucket = target_config["bucket"]

    succeeded: list[str] = []
    failed: list[str] = []

    for service_dir in service_dirs:
        service_name = os.path.basename(service_dir)
        logger.info("Uploading %s", service_name)

        try:
            filter_manifest(service_dir)
        except FileNotFoundError:
            logger.error("No compiled manifest for %s", service_name)
            failed.append(service_dir)
            continue

        if upload_manifest(s3_client, service_dir, env, bucket):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd dbt && uv run pytest tests/test_upload_unit.py -v`
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add dbt/dbt_upload/upload.py dbt/tests/test_upload_unit.py
git commit -m "feat(dbt): add upload module with filter_manifest, upload_manifest, upload_services"
```

---

### Task 4: CLI module + `__main__.py`

**Files:**
- Create: `dbt/dbt_upload/cli.py`
- Create: `dbt/dbt_upload/__main__.py`
- Create: `dbt/tests/test_cli.py`

- [ ] **Step 1: Write failing tests for `cli.py`**

Create `dbt/tests/test_cli.py`:

```python
from unittest.mock import patch, MagicMock
from dbt_upload.cli import main


class TestCliCompile:
    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_compile_subcommand(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/services/svc-1"]
        mock_compile.return_value = (["/app/services/svc-1"], [])

        code = main(["compile", "--services-dir", "./services"])

        assert code == 0
        mock_resolve.assert_called_once_with(None, "./services")
        mock_compile.assert_called_once_with(["/app/services/svc-1"])

    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_compile_with_failures_exits_nonzero(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/services/svc-1"]
        mock_compile.return_value = ([], ["/app/services/svc-1"])

        code = main(["compile", "--services-dir", "./services"])

        assert code == 1


class TestCliUpload:
    @patch("dbt_upload.cli.upload_services")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_upload_subcommand(self, mock_resolve, mock_load_target, mock_upload):
        mock_resolve.return_value = ["/app/services/svc-1"]
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/services/svc-1"], [])

        code = main(["upload", "--services-dir", "./services", "--target", "localstack"])

        assert code == 0
        mock_upload.assert_called_once()


class TestCliLoad:
    @patch("dbt_upload.cli.upload_services")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_load_compiles_then_uploads_succeeded(
        self, mock_resolve, mock_compile, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/services/svc-1", "/app/services/svc-2"]
        mock_compile.return_value = (["/app/services/svc-1"], ["/app/services/svc-2"])
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/services/svc-1"], [])

        code = main(["load", "--services-dir", "./services", "--target", "localstack"])

        # upload_services receives only the successfully compiled dirs
        mock_upload.assert_called_once()
        upload_dirs = mock_upload.call_args[0][0]
        assert upload_dirs == ["/app/services/svc-1"]

        # Non-zero because svc-2 failed to compile
        assert code == 1

    @patch("dbt_upload.cli.upload_services")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_load_all_succeed(
        self, mock_resolve, mock_compile, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/services/svc-1"]
        mock_compile.return_value = (["/app/services/svc-1"], [])
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/services/svc-1"], [])

        code = main(["load", "--services-dir", "./services"])

        assert code == 0

    @patch("dbt_upload.cli.upload_services")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_load_env_override(
        self, mock_resolve, mock_compile, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/services/svc-1"]
        mock_compile.return_value = (["/app/services/svc-1"], [])
        target_cfg = {"env": "local", "bucket": "continuo"}
        mock_load_target.return_value = target_cfg
        mock_upload.return_value = (["/app/services/svc-1"], [])

        main(["load", "--services-dir", "./services", "--env", "staging"])

        # Verify env was overridden in the target config
        assert target_cfg["env"] == "staging"


class TestCliPositionalPaths:
    @patch("dbt_upload.cli.compile_services")
    @patch("dbt_upload.cli.resolve_service_dirs")
    def test_compile_with_positional_paths(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/services/svc-1", "/app/services/svc-3"]
        mock_compile.return_value = (["/app/services/svc-1", "/app/services/svc-3"], [])

        code = main(["compile", "./services/svc-1", "./services/svc-3"])

        assert code == 0
        mock_resolve.assert_called_once_with(
            ["./services/svc-1", "./services/svc-3"], None
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd dbt && uv run pytest tests/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dbt_upload.cli'`

- [ ] **Step 3: Implement `cli.py`**

Create `dbt/dbt_upload/cli.py`:

```python
"""CLI entry point with argparse subcommands."""
import logging
import os
import sys

from dbt_upload.compile import compile_services
from dbt_upload.config import load_target, resolve_service_dirs
from dbt_upload.upload import upload_services

logger = logging.getLogger(__name__)


def _find_targets_yaml() -> str:
    """Locate targets.yaml relative to this package."""
    here = os.path.dirname(os.path.abspath(__file__))
    # In Docker: /app/dbt_upload/ -> /app/targets.yaml
    # In dev:    dbt/dbt_upload/  -> dbt/targets.yaml
    candidate = os.path.join(os.path.dirname(here), "targets.yaml")
    if os.path.exists(candidate):
        return candidate
    # Fallback: current working directory
    cwd_candidate = os.path.join(os.getcwd(), "targets.yaml")
    if os.path.exists(cwd_candidate):
        return cwd_candidate
    raise FileNotFoundError("Cannot find targets.yaml")


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the appropriate subcommand. Returns exit code."""
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(
        prog="dbt_upload",
        description="Compile dbt services and upload manifests to S3",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- compile --
    p_compile = subparsers.add_parser("compile", help="Compile dbt services")
    p_compile.add_argument("paths", nargs="*", default=[], help="Service directories")
    p_compile.add_argument("--services-dir", default=None, help="Directory containing services")

    # -- upload --
    p_upload = subparsers.add_parser("upload", help="Upload compiled manifests to S3")
    p_upload.add_argument("paths", nargs="*", default=[], help="Service directories")
    p_upload.add_argument("--services-dir", default=None, help="Directory containing services")
    p_upload.add_argument("--target", default="localstack", help="Target profile name")
    p_upload.add_argument("--env", default=None, help="Override S3 env prefix")

    # -- load --
    p_load = subparsers.add_parser("load", help="Compile + upload (primary workflow)")
    p_load.add_argument("paths", nargs="*", default=[], help="Service directories")
    p_load.add_argument("--services-dir", default=None, help="Directory containing services")
    p_load.add_argument("--target", default="localstack", help="Target profile name")
    p_load.add_argument("--env", default=None, help="Override S3 env prefix")

    args = parser.parse_args(argv)

    service_dirs = resolve_service_dirs(
        args.paths if args.paths else None,
        args.services_dir,
    )

    if args.command == "compile":
        succeeded, failed = compile_services(service_dirs)
        logger.info("Compile done: %d succeeded, %d failed", len(succeeded), len(failed))
        return 1 if failed else 0

    # upload and load both need a target
    targets_yaml = _find_targets_yaml()
    target_config = load_target(targets_yaml, args.target)
    if args.env:
        target_config["env"] = args.env

    if args.command == "upload":
        succeeded, failed = upload_services(service_dirs, target_config)
        logger.info("Upload done: %d succeeded, %d failed", len(succeeded), len(failed))
        return 1 if failed else 0

    if args.command == "load":
        compiled_ok, compile_failed = compile_services(service_dirs)

        if not compiled_ok:
            logger.error("No services compiled successfully")
            return 1

        uploaded_ok, upload_failed = upload_services(compiled_ok, target_config)
        total_failed = len(compile_failed) + len(upload_failed)
        logger.info(
            "Load done: %d compiled, %d uploaded, %d failed",
            len(compiled_ok), len(uploaded_ok), total_failed,
        )
        return 1 if total_failed else 0

    return 1


def cli() -> None:
    """Entry point that calls sys.exit."""
    sys.exit(main())
```

- [ ] **Step 4: Create `__main__.py`**

Create `dbt/dbt_upload/__main__.py`:

```python
from dbt_upload.cli import cli

cli()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd dbt && uv run pytest tests/test_cli.py -v`
Expected: all 7 tests PASS

- [ ] **Step 6: Commit**

```bash
git add dbt/dbt_upload/cli.py dbt/dbt_upload/__main__.py dbt/tests/test_cli.py
git commit -m "feat(dbt): add CLI with compile, upload, load subcommands"
```

---

### Task 5: Update `pyproject.toml` and lock file

**Files:**
- Modify: `dbt/pyproject.toml`

- [ ] **Step 1: Update `pyproject.toml`**

Replace the contents of `dbt/pyproject.toml` with:

```toml
[project]
name = "dbt-compile-and-load"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "boto3>=1.34.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
]

[tool.pytest.ini_options]
pythonpath = ["."]

[tool.hatch.build.targets.wheel]
only-include = ["dbt_upload"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 2: Regenerate lock file**

Run: `cd dbt && uv lock`
Expected: `uv.lock` updated with pyyaml dependency

- [ ] **Step 3: Verify all unit tests still pass**

Run: `cd dbt && uv run pytest tests/test_config.py tests/test_compile.py tests/test_upload_unit.py tests/test_cli.py -v`
Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add dbt/pyproject.toml dbt/uv.lock
git commit -m "chore(dbt): update pyproject.toml with pyyaml dep and package rename"
```

---

### Task 6: Delete old scripts

**Files:**
- Delete: `dbt/compile_only.py`
- Delete: `dbt/upload_manifests.py`

- [ ] **Step 1: Delete old scripts**

```bash
rm dbt/compile_only.py dbt/upload_manifests.py
```

- [ ] **Step 2: Verify no imports reference old scripts**

Run: `grep -r "from upload_manifests\|import upload_manifests\|from compile_only\|import compile_only" dbt/`
Expected: only `dbt/tests/test_upload.py` (will be updated in next task)

- [ ] **Step 3: Commit**

```bash
git add -u dbt/compile_only.py dbt/upload_manifests.py
git commit -m "chore(dbt): remove old flat scripts compile_only.py and upload_manifests.py"
```

---

### Task 7: Update integration tests

**Files:**
- Modify: `dbt/tests/test_upload.py`

- [ ] **Step 1: Update `test_upload.py` imports and references**

Replace the full contents of `dbt/tests/test_upload.py` with:

```python
"""
Integration tests for dbt compile+upload pipeline.
Requires localstack running at S3_ENDPOINT_URL (default: http://localstack:4566).
Run from the repo root:
  docker run --rm --network continuo_default --workdir /app \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -v "$(pwd)/dbt/services:/app/services" \
    dbt-compile-and-load:latest uv run pytest tests/ -v
"""
import json
import os
import subprocess

import boto3
import pytest

SERVICES_DIR = "/app/services"
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "continuo")
S3_ENV = os.getenv("S3_ENV", "local")


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def test_dbt_compile_service1_succeeds():
    """dbt compile runs without error for service-1."""
    service_dir = os.path.join(SERVICES_DIR, "service-1")
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt compile failed:\n{result.stderr}"
    manifest = os.path.join(service_dir, "target", "manifest.json")
    assert os.path.exists(manifest), "target/manifest.json not created"


def test_upload_and_read_back(s3):
    """compile + upload produces a readable manifest.json in S3."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    service_dir = os.path.join(SERVICES_DIR, "service-1")
    assert compile_service(service_dir), "compile_service returned False"
    assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), "upload_manifest returned False"

    key = f"{S3_ENV}/manifest/service-1/manifest.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())

    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names


def test_all_valid_services_upload(s3):
    """service-1, service-2, service-3 all compile and upload; service-3-broken is skipped."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    valid = ["service-1", "service-2", "service-3"]
    for name in valid:
        service_dir = os.path.join(SERVICES_DIR, name)
        assert compile_service(service_dir), f"{name} failed to compile"
        assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), f"{name} failed to upload"

    # verify keys exist in S3
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_ENV}/manifest/")
    keys = {obj["Key"] for obj in response.get("Contents", [])}
    for name in valid:
        assert f"{S3_ENV}/manifest/{name}/manifest.json" in keys


def test_service3_broken_compile_fails():
    """service-3-broken fails dbt compile — compile_service returns False."""
    from dbt_upload.compile import compile_service

    service_dir = os.path.join(SERVICES_DIR, "service-3-broken")
    assert not compile_service(service_dir), "Expected compile to fail for broken service"
```

- [ ] **Step 2: Verify unit tests still pass**

Run: `cd dbt && uv run pytest tests/test_config.py tests/test_compile.py tests/test_upload_unit.py tests/test_cli.py -v`
Expected: all unit tests PASS (integration tests require Docker/LocalStack)

- [ ] **Step 3: Commit**

```bash
git add dbt/tests/test_upload.py
git commit -m "refactor(dbt): update integration tests to use dbt_upload package imports"
```

---

### Task 8: Update Dockerfile + docker-compose

**Files:**
- Modify: `dbt/Dockerfile.upload`
- Modify: `docker-compose.yml:428-429` (depends_on reference)
- Modify: `docker-compose.yml:527-553` (service definition)

- [ ] **Step 1: Update `Dockerfile.upload`**

Replace the full contents of `dbt/Dockerfile.upload` with:

```dockerfile
FROM dbt-base:latest

# Override the inherited dbt entrypoint — this image runs Python scripts, not dbt jobs
ENTRYPOINT []

WORKDIR /app

# Install uv
RUN pip install uv --quiet

# Install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Copy package, targets config, and tests
COPY dbt_upload/ ./dbt_upload/
COPY targets.yaml .
COPY tests/ ./tests/

ENTRYPOINT ["uv", "run", "python", "-m", "dbt_upload"]
CMD ["load", "--services-dir", "./services"]
```

- [ ] **Step 2: Rename service in `docker-compose.yml`**

In `docker-compose.yml`, change the service definition at line 527 from `compile-and-upload:` to `dbt-compile-and-load:`.

Also update the depends_on reference at line 428 from `compile-and-upload:` to `dbt-compile-and-load:`.

Remove the S3-specific environment variables from the service definition (the container now reads them from `targets.yaml` for localstack, or from `--env-file` for hetzner). Keep the dbt PostgreSQL env vars.

The updated service block should be:

```yaml
  dbt-compile-and-load:
    build:
      context: ./dbt
      dockerfile: Dockerfile.upload
    restart: "no"
    depends_on:
      localstack:
        condition: service_healthy
      postgres:
        condition: service_started
    environment:
      # dbt PostgreSQL connection for dbt compile
      - DBT_POSTGRES_HOST=postgres
      - DBT_POSTGRES_PORT=5432
      - DBT_POSTGRES_DB=continuo_dbt
      - DBT_POSTGRES_USER=continuo_svc
      - DBT_POSTGRES_PASSWORD=continuo
    volumes:
      - ./dbt/services:/app/services
    profiles:
      - e2e
```

- [ ] **Step 3: Verify docker-compose config is valid**

Run: `docker compose config --quiet`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add dbt/Dockerfile.upload docker-compose.yml
git commit -m "feat(dbt): update Dockerfile and rename docker-compose service to dbt-compile-and-load"
```

---

### Task 9: Add `.env.hetzner` and update `.gitignore`

**Files:**
- Create: `dbt/.env.hetzner`
- Modify: `.gitignore`

- [ ] **Step 1: Create `.env.hetzner`**

Create `dbt/.env.hetzner` with the real Hetzner Object Storage credentials:

```
AWS_ACCESS_KEY_ID=<your-hetzner-access-key>
AWS_SECRET_ACCESS_KEY=<your-hetzner-secret-key>
```

- [ ] **Step 2: Add to `.gitignore`**

Add the following line to `.gitignore` (after line 21, under `# env file`):

```
.env.hetzner
```

- [ ] **Step 3: Verify `.env.hetzner` is not tracked**

Run: `git status dbt/.env.hetzner`
Expected: file does not appear (it is ignored)

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "chore: add .env.hetzner to gitignore"
```

---

### Task 10: Update Helm values

**Files:**
- Modify: `deploy/app/values.yaml:27-33`

- [ ] **Step 1: Fix S3 endpoint and bucket in Helm values**

In `deploy/app/values.yaml`, update the `s3` block (lines 27-33) from:

```yaml
  s3:
    endpointUrl: https://fsn1.your-objectstorage.com
    bucket: continuo
    env: dev
    region: eu-central-1
    accessKeyId: ""
    secretKey: ""
```

To:

```yaml
  s3:
    endpointUrl: https://nbg1.your-objectstorage.com
    bucket: continuo-dev
    env: dev
    region: eu-central-1
    accessKeyId: ""
    secretKey: ""
```

- [ ] **Step 2: Commit**

```bash
git add deploy/app/values.yaml
git commit -m "fix(deploy): correct Hetzner object storage endpoint and bucket name"
```

---

### Task 11: Build and run integration tests

- [ ] **Step 1: Build the dbt-base image (if not already built)**

```bash
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest ./dbt/base
```

- [ ] **Step 2: Build the dbt-compile-and-load image**

```bash
DOCKER_BUILDKIT=1 docker build -t dbt-compile-and-load:latest -f dbt/Dockerfile.upload dbt/
```

- [ ] **Step 3: Start LocalStack**

```bash
docker compose up -d localstack
```

Wait until healthy:

```bash
docker compose ps localstack
```

- [ ] **Step 4: Run integration tests inside the container**

```bash
docker run --rm --network continuo_default --workdir /app \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest uv run pytest tests/ -v
```

Expected: all 4 integration tests PASS

- [ ] **Step 5: Test the `compile` subcommand**

```bash
docker run --rm \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest compile ./services/service-1
```

Expected: "Compiled service-1 successfully" in output

- [ ] **Step 6: Test the `load` subcommand against LocalStack**

```bash
docker run --rm --network continuo_default \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest load --services-dir ./services --target localstack
```

Expected: all valid services compile and upload; service-3-broken fails compile (non-zero exit)

- [ ] **Step 7: Commit (if any fixes were needed)**

Only commit if code changes were required to make tests pass.

---

### Task 12: Update architecture docs

**Files:**
- Modify: `docs/arch/01-topology.md` (if it references old service name or old S3 endpoint)
- Modify: `docs/arch/services/manifest-controller.md` (if it references the compile-and-upload flow)

- [ ] **Step 1: Review and update architecture docs**

Check `docs/arch/` for references to `compile-and-upload`, `fsn1`, or the old bucket name `continuo` (when referring to object storage, not the project). Update any stale references to use `dbt-compile-and-load`, `nbg1.your-objectstorage.com`, and `continuo-dev`.

- [ ] **Step 2: Commit**

```bash
git add docs/arch/
git commit -m "docs(arch): update references to dbt-compile-and-load and Hetzner object storage"
```
