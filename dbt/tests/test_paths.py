from dbt_upload.paths import build_dir


def test_each_project_builds_in_its_own_directory_under_the_build_root(build_root):
    """A project's compile output goes to <build root>/<project directory name>,
    the same name its S3 keys carry, and never under the project directory."""
    assert build_dir("/app/projects/service-1") == str(build_root / "service-1")
    assert build_dir("/app/projects/service-2") == str(build_root / "service-2")
