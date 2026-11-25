"""dbt compile logic."""
import logging
import os
import shutil
import subprocess

from dbt_upload.paths import build_dir

logger = logging.getLogger(__name__)

# The project directory of the dbt base image this uploader's image is built
# FROM. Every project image is built FROM the same base image and copies its
# project files over this directory, so the shared macros it holds (the
# generate_schema_name that routes a validation run to its candidate schema)
# are macros of the root project dbt compiles when the project's image compiles.
BASE_PROJECT_DIR = "/project"

# The directory under a project's build directory that holds the project dbt
# compiles. It stays after the compile, since the manifest's root_path names it
# and the compile extras read each seed's file from there.
STAGED_PROJECT_DIR = "staged-project"

# What dbt writes into a project directory; never staged from the top of a
# project directory or the base project, where a dbt command run there before
# may have left them.
_DBT_OUTPUT_DIRS = frozenset({"target", "logs"})


def _copy_project(src: str, dst: str) -> None:
    """Copy the project files of src into dst, over what dst holds, leaving out
    src's own target/ and logs/."""
    shutil.copytree(src, dst, dirs_exist_ok=True,
                    ignore=lambda d, names: _DBT_OUTPUT_DIRS & set(names) if d == src else set())


def compile_project(project_dir: str, base_project_dir: str = BASE_PROJECT_DIR) -> bool:
    """Run `dbt compile` for project_dir as the project's image compiles it.
    Returns True on success.

    dbt runs in staged-project under the project's build directory: a copy of
    base_project_dir with the project's files copied over it (a project file
    replaces a base file of the same path), so the manifest holds the same
    root-project macros the compile Job's manifest holds. dbt writes its output
    into the build directory and nothing into project_dir, which may be
    read-only. A missing base_project_dir fails the project: the manifest would
    lack the base image's macros, and every content_hash computed from it would
    differ from a release's.
    """
    name = os.path.basename(project_dir)
    if not os.path.isdir(base_project_dir):
        logger.error(
            "Cannot compile %s: no dbt base project at %s, whose macros every project image compiles with",
            name, base_project_dir)
        return False
    logger.info("Compiling %s", name)
    project_dir = os.path.abspath(project_dir)
    target_dir = build_dir(project_dir)
    staged_dir = os.path.join(target_dir, STAGED_PROJECT_DIR)
    shutil.rmtree(staged_dir, ignore_errors=True)
    _copy_project(base_project_dir, staged_dir)
    _copy_project(project_dir, staged_dir)
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", ".", "--target-path", target_dir],
        cwd=staged_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip()
        logger.error("dbt compile failed for %s:\n%s", name, output)
        return False
    logger.info("Compiled %s successfully", name)
    return True


def compile_projects(
    project_dirs: list[str],
) -> tuple[list[str], list[str]]:
    """Compile each project directory.

    Returns (succeeded_dirs, failed_dirs).
    """
    succeeded: list[str] = []
    failed: list[str] = []

    for project_dir in project_dirs:
        if compile_project(project_dir):
            succeeded.append(project_dir)
        else:
            failed.append(project_dir)

    return succeeded, failed
