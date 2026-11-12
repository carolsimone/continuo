"""dbt compile logic."""
import logging
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)

# The project directory of the dbt base image this uploader's image is built
# FROM. Every service image is built FROM the same base image and copies its
# project files over this directory, so the shared macros it holds (the
# generate_schema_name that routes a validation run to its candidate schema)
# are macros of each service's root project when the service's image compiles.
BASE_PROJECT_DIR = "/project"

# The directory under a service's target/ that holds the project dbt compiles.
# It stays after the compile, since the manifest's root_path names it and the
# compile extras read each seed's file from there.
STAGED_PROJECT_DIR = "staged-project"

# What dbt writes into a project directory; never staged from the top of a
# service directory or the base project.
_DBT_OUTPUT_DIRS = frozenset({"target", "logs"})


def _copy_project(src: str, dst: str) -> None:
    """Copy the project files of src into dst, over what dst holds, leaving out
    src's own target/ and logs/."""
    shutil.copytree(src, dst, dirs_exist_ok=True,
                    ignore=lambda d, names: _DBT_OUTPUT_DIRS & set(names) if d == src else set())


def compile_service(service_dir: str, base_project_dir: str = BASE_PROJECT_DIR) -> bool:
    """Run `dbt compile` for service_dir as the service's image compiles it.
    Returns True on success.

    dbt runs in target/staged-project: a copy of base_project_dir with the
    service's files copied over it (a service file replaces a base file of the
    same path), so the manifest holds the same root-project macros the compile
    Job's manifest holds. dbt writes its output into service_dir's target/. A
    missing base_project_dir fails the service: the manifest would lack the base
    image's macros, and every content_hash computed from it would differ from a
    release's.
    """
    name = os.path.basename(service_dir)
    if not os.path.isdir(base_project_dir):
        logger.error(
            "Cannot compile %s: no dbt base project at %s, whose macros every service image compiles with",
            name, base_project_dir)
        return False
    logger.info("Compiling %s", name)
    service_dir = os.path.abspath(service_dir)
    target_dir = os.path.join(service_dir, "target")
    project_dir = os.path.join(target_dir, STAGED_PROJECT_DIR)
    shutil.rmtree(project_dir, ignore_errors=True)
    _copy_project(base_project_dir, project_dir)
    _copy_project(service_dir, project_dir)
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", ".", "--target-path", target_dir],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip()
        logger.error("dbt compile failed for %s:\n%s", name, output)
        return False
    logger.info("Compiled %s successfully", name)
    return True


def compile_services(
    service_dirs: list[str],
) -> tuple[list[str], list[str]]:
    """Compile each service directory.

    Returns (succeeded_dirs, failed_dirs).
    """
    succeeded: list[str] = []
    failed: list[str] = []

    for service_dir in service_dirs:
        if compile_service(service_dir):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
