from dbt_upload.paths import build_dir


def test_each_service_builds_in_its_own_directory_under_the_build_root(build_root):
    """A service's compile output goes to <build root>/<service directory name>,
    the same name its S3 keys carry, and never under the service directory."""
    assert build_dir("/app/services/service-1") == str(build_root / "service-1")
    assert build_dir("/app/services/service-2") == str(build_root / "service-2")
