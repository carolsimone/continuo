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
