"""CLI entry point with argparse subcommands."""
import logging
import os
import sys

from dbt_upload.compile import compile_projects
from dbt_upload.config import load_target, resolve_project_dirs
from dbt_upload.upload import upload_projects

logger = logging.getLogger(__name__)


def _find_targets_yaml() -> str:
    """Locate targets.yaml relative to this package."""
    here = os.path.dirname(os.path.abspath(__file__))
    # In Docker: /app/dbt_upload/ -> /app/targets.yaml
    # In dev:    dbt/dbt_upload/  -> dbt/targets.yaml
    candidate = os.path.join(os.path.dirname(here), "targets.yaml")
    if os.path.exists(candidate):
        return candidate
    # Fallback: current working directory
    cwd_candidate = os.path.join(os.getcwd(), "targets.yaml")
    if os.path.exists(cwd_candidate):
        return cwd_candidate
    raise FileNotFoundError("Cannot find targets.yaml")


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the appropriate subcommand. Returns exit code."""
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(
        prog="dbt_upload",
        description="Compile dbt projects and upload manifests to S3",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- compile --
    p_compile = subparsers.add_parser("compile", help="Compile dbt projects")
    p_compile.add_argument("paths", nargs="*", default=[], help="Project directories")
    p_compile.add_argument("--projects-dir", default=None, help="Directory containing projects")

    # -- upload --
    p_upload = subparsers.add_parser("upload", help="Upload compiled manifests to S3")
    p_upload.add_argument("paths", nargs="*", default=[], help="Project directories")
    p_upload.add_argument("--projects-dir", default=None, help="Directory containing projects")
    p_upload.add_argument("--target", default="minio", help="Target profile name")
    p_upload.add_argument("--env", default=None, help="Override S3 env prefix")
    p_upload.add_argument(
        "--release-id",
        default="",
        help="Release ID for canonical S3 key <project>/<release_id>/manifest.json (required)",
    )

    # -- load --
    p_load = subparsers.add_parser("load", help="Compile + upload (primary workflow)")
    p_load.add_argument("paths", nargs="*", default=[], help="Project directories")
    p_load.add_argument("--projects-dir", default=None, help="Directory containing projects")
    p_load.add_argument("--target", default="minio", help="Target profile name")
    p_load.add_argument("--env", default=None, help="Override S3 env prefix")
    p_load.add_argument(
        "--release-id",
        default="",
        help="Release ID for canonical S3 key <project>/<release_id>/manifest.json (required)",
    )

    args = parser.parse_args(argv)

    project_dirs = resolve_project_dirs(
        args.paths if args.paths else None,
        args.projects_dir,
    )

    if args.command == "compile":
        succeeded, failed = compile_projects(project_dirs)
        logger.info("Compile done: %d succeeded, %d failed", len(succeeded), len(failed))
        return 1 if failed else 0

    # upload and load both need a target
    targets_yaml = _find_targets_yaml()
    target_config = load_target(targets_yaml, args.target)
    if args.env:
        target_config["env"] = args.env

    if args.command == "upload":
        if not args.release_id:
            logger.error("--release-id is required for the upload subcommand")
            return 1
        succeeded, failed = upload_projects(project_dirs, target_config, release_id=args.release_id)
        logger.info("Upload done: %d succeeded, %d failed", len(succeeded), len(failed))
        return 1 if failed else 0

    if args.command == "load":
        if not args.release_id:
            logger.error("--release-id is required for the load subcommand")
            return 1

        compiled_ok, compile_failed = compile_projects(project_dirs)

        if not compiled_ok:
            logger.error("No projects compiled successfully")
            return 1

        uploaded_ok, upload_failed = upload_projects(
            compiled_ok, target_config, release_id=args.release_id
        )
        total_failed = len(compile_failed) + len(upload_failed)
        logger.info(
            "Load done: %d compiled, %d uploaded, %d failed",
            len(compiled_ok), len(uploaded_ok), total_failed,
        )
        return 1 if total_failed else 0

    return 1


def cli() -> None:
    """Entry point that calls sys.exit."""
    sys.exit(main())
