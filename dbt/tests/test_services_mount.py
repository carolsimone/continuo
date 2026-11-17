import os

import pytest

SERVICES_DIR = "/app/services"


@pytest.mark.integration
def test_the_services_mount_is_read_only():
    """dbt-compile-and-load mounts dbt/services read-only, so nothing it runs
    writes into the checkout the services come from, whichever user owns it: a
    compile writes under /app/build and any other dbt command under the image's
    DBT_TARGET_PATH and DBT_LOG_PATH."""
    assert os.statvfs(SERVICES_DIR).f_flag & os.ST_RDONLY, f"{SERVICES_DIR} is mounted writable"
