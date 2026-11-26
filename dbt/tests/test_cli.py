from unittest.mock import patch, MagicMock
from dbt_upload.cli import main


class TestCliCompile:
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_compile_subcommand(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_compile.return_value = (["/app/projects/svc-1"], [])

        code = main(["compile", "--projects-dir", "./projects"])

        assert code == 0
        mock_resolve.assert_called_once_with(None, "./projects")
        mock_compile.assert_called_once_with(["/app/projects/svc-1"])

    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_compile_with_failures_exits_nonzero(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_compile.return_value = ([], ["/app/projects/svc-1"])

        code = main(["compile", "--projects-dir", "./projects"])

        assert code == 1


class TestCliUpload:
    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_upload_subcommand(self, mock_resolve, mock_find_yaml, mock_load_target, mock_upload):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/projects/svc-1"], [])

        code = main(["upload", "--projects-dir", "./projects", "--target", "minio",
                     "--release-id", "rel-456"])

        assert code == 0
        mock_upload.assert_called_once()

    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_upload_without_release_id_exits_nonzero(self, mock_resolve, mock_find_yaml, mock_load_target, mock_upload):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}

        code = main(["upload", "--projects-dir", "./projects"])

        assert code == 1
        mock_upload.assert_not_called()


class TestCliLoad:
    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_load_compiles_then_uploads_succeeded(
        self, mock_resolve, mock_compile, mock_find_yaml, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/projects/svc-1", "/app/projects/svc-2"]
        mock_compile.return_value = (["/app/projects/svc-1"], ["/app/projects/svc-2"])
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/projects/svc-1"], [])

        code = main(["load", "--projects-dir", "./projects", "--target", "minio",
                     "--release-id", "rel-789"])

        # upload_projects receives only the successfully compiled dirs
        mock_upload.assert_called_once()
        upload_dirs = mock_upload.call_args[0][0]
        assert upload_dirs == ["/app/projects/svc-1"]

        # Non-zero because svc-2 failed to compile
        assert code == 1

    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_load_all_succeed(
        self, mock_resolve, mock_compile, mock_find_yaml, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_compile.return_value = (["/app/projects/svc-1"], [])
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/projects/svc-1"], [])

        code = main(["load", "--projects-dir", "./projects", "--release-id", "rel-001"])

        assert code == 0

    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_load_env_override(
        self, mock_resolve, mock_compile, mock_find_yaml, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_compile.return_value = (["/app/projects/svc-1"], [])
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        target_cfg = {"env": "local", "bucket": "continuo"}
        mock_load_target.return_value = target_cfg
        mock_upload.return_value = (["/app/projects/svc-1"], [])

        main(["load", "--projects-dir", "./projects", "--env", "staging"])

        # Verify env was overridden in the target config
        assert target_cfg["env"] == "staging"

    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_load_release_id_threaded_to_upload(
        self, mock_resolve, mock_compile, mock_find_yaml, mock_load_target, mock_upload
    ):
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_compile.return_value = (["/app/projects/svc-1"], [])
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}
        mock_upload.return_value = (["/app/projects/svc-1"], [])

        code = main(["load", "--projects-dir", "./projects", "--release-id", "rel-123"])

        assert code == 0
        mock_upload.assert_called_once()
        assert mock_upload.call_args.kwargs["release_id"] == "rel-123"

    @patch("dbt_upload.cli.upload_projects")
    @patch("dbt_upload.cli.load_target")
    @patch("dbt_upload.cli._find_targets_yaml")
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_load_without_release_id_exits_nonzero(
        self, mock_resolve, mock_compile, mock_find_yaml, mock_load_target, mock_upload
    ):
        """load without --release-id must exit 1 and never call upload_projects."""
        mock_resolve.return_value = ["/app/projects/svc-1"]
        mock_find_yaml.return_value = "/dummy/targets.yaml"
        mock_load_target.return_value = {"env": "local", "bucket": "continuo"}

        code = main(["load", "--projects-dir", "./projects"])

        assert code == 1
        mock_upload.assert_not_called()


class TestCliPositionalPaths:
    @patch("dbt_upload.cli.compile_projects")
    @patch("dbt_upload.cli.resolve_project_dirs")
    def test_compile_with_positional_paths(self, mock_resolve, mock_compile):
        mock_resolve.return_value = ["/app/projects/svc-1", "/app/projects/svc-3"]
        mock_compile.return_value = (["/app/projects/svc-1", "/app/projects/svc-3"], [])

        code = main(["compile", "./projects/svc-1", "./projects/svc-3"])

        assert code == 0
        mock_resolve.assert_called_once_with(
            ["./projects/svc-1", "./projects/svc-3"], None
        )
