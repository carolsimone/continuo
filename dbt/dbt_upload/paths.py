"""Where dbt_upload writes what it builds for a service."""
import os

# The image's own directory every service's compile output goes under, one
# subdirectory per service: the staged project dbt compiles, the manifest and
# compile-extras.json. dbt-compile-and-load mounts the services read-only, so
# nothing is ever written beside a service's files, whichever user owns them.
BUILD_ROOT = "/app/build"


def build_dir(service_dir: str) -> str:
    """The directory the compile of service_dir writes into, named like the
    service directory, as the service's S3 keys are."""
    return os.path.join(BUILD_ROOT, os.path.basename(service_dir))
