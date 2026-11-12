import os
from unittest.mock import patch, MagicMock
from dbt_upload.compile import compile_service, compile_services


def _base_project(tmp_path):
    """A dbt base image project directory holding one shared macro."""
    base = tmp_path / "base-project"
    (base / "macros").mkdir(parents=True)
    (base / "macros" / "generate_schema_name.sql").write_text("{% macro generate_schema_name() %}{% endmacro %}")
    return base


def _service(tmp_path):
    """A service project directory with a model and a stale target/ and logs/."""
    service = tmp_path / "services" / "service-1"
    (service / "models").mkdir(parents=True)
    (service / "models" / "table_a.sql").write_text("select 1")
    (service / "dbt_project.yml").write_text("name: service_1\n")
    (service / "target").mkdir()
    (service / "target" / "stale.txt").write_text("stale")
    (service / "logs").mkdir()
    return service


class TestCompileService:
    @patch("dbt_upload.compile.subprocess.run")
    def test_compiles_the_service_over_the_base_project(self, mock_run, tmp_path):
        """dbt runs in target/staged-project, which holds the base project's macros
        with the service's files over them, as the service image built FROM the dbt
        base image does, and writes its output into the service's target/. The
        staged project stays, since the manifest's root_path names it."""
        base = _base_project(tmp_path)
        service = _service(tmp_path)
        (service / "target" / "staged-project" / "models").mkdir(parents=True)
        (service / "target" / "staged-project" / "models" / "deleted.sql").write_text("select 0")
        seen = {}

        def run(argv, cwd, capture_output, text):
            seen["argv"] = argv
            seen["cwd"] = cwd
            seen["files"] = sorted(
                os.path.relpath(os.path.join(root, f), cwd) for root, _, files in os.walk(cwd) for f in files)
            return MagicMock(returncode=0)

        mock_run.side_effect = run

        assert compile_service(str(service), base_project_dir=str(base)) is True

        assert seen["cwd"] == str(service / "target" / "staged-project")
        assert seen["files"] == ["dbt_project.yml", "macros/generate_schema_name.sql", "models/table_a.sql"]
        assert seen["argv"] == [
            "dbt", "compile", "--profiles-dir", ".", "--target-path", str(service / "target"),
        ]
        assert os.path.isdir(seen["cwd"])

    @patch("dbt_upload.compile.subprocess.run")
    def test_a_service_file_replaces_the_base_file_of_the_same_path(self, mock_run, tmp_path):
        """A service that ships its own macros/generate_schema_name.sql compiles with
        it, as the image's COPY of the service's files over the base project does."""
        base = _base_project(tmp_path)
        service = _service(tmp_path)
        (service / "macros").mkdir()
        (service / "macros" / "generate_schema_name.sql").write_text("service macro")
        seen = {}

        def run(argv, cwd, capture_output, text):
            with open(os.path.join(cwd, "macros", "generate_schema_name.sql")) as f:
                seen["macro"] = f.read()
            return MagicMock(returncode=0)

        mock_run.side_effect = run

        assert compile_service(str(service), base_project_dir=str(base)) is True
        assert seen["macro"] == "service macro"

    @patch("dbt_upload.compile.subprocess.run")
    def test_refuses_without_the_base_project(self, mock_run, tmp_path):
        """With no base project directory the service is failed and dbt never runs,
        since its manifest would lack the macros every service image carries."""
        service = _service(tmp_path)

        assert compile_service(str(service), base_project_dir=str(tmp_path / "absent")) is False
        mock_run.assert_not_called()

    @patch("dbt_upload.compile.subprocess.run")
    def test_returns_false_on_failure(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=1, stderr="compile error")
        service = _service(tmp_path)

        assert compile_service(str(service), base_project_dir=str(_base_project(tmp_path))) is False


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
