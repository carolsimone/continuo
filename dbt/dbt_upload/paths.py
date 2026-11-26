"""Where dbt_upload writes what it builds for a project."""
import os

# The image's own directory every project's compile output goes under, one
# subdirectory per project: the staged project dbt compiles, the manifest and
# compile-extras.json. dbt-compile-and-load mounts the projects read-only, so
# nothing is ever written beside a project's files, whichever user owns them.
BUILD_ROOT = "/app/build"


def build_dir(project_dir: str) -> str:
    """The directory the compile of project_dir writes into, named like the
    project directory, as the project's S3 keys are."""
    return os.path.join(BUILD_ROOT, os.path.basename(project_dir))
