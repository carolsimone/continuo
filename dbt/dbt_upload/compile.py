"""dbt compile logic."""
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def compile_service(service_dir: str) -> bool:
    """Run `dbt compile` in service_dir. Returns True on success."""
    name = os.path.basename(service_dir)
    logger.info("Compiling %s", name)
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir,
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
