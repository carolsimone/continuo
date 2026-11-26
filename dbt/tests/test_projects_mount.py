import os

import pytest

PROJECTS_DIR = "/app/projects"


@pytest.mark.integration
def test_the_projects_mount_is_read_only():
    """dbt-compile-and-load mounts dbt/projects read-only, so nothing it runs
    writes into the checkout the projects come from, whichever user owns it: a
    compile writes under /app/build and any other dbt command under the image's
    DBT_TARGET_PATH and DBT_LOG_PATH."""
    assert os.statvfs(PROJECTS_DIR).f_flag & os.ST_RDONLY, f"{PROJECTS_DIR} is mounted writable"
