#!/usr/bin/env python3
"""Compile each dbt service in services_dir WITHOUT uploading to S3."""
import argparse
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--services-dir", default="./services")
    args = parser.parse_args()

    services_dir = os.path.abspath(args.services_dir)
    service_dirs = sorted(
        os.path.join(services_dir, d)
        for d in os.listdir(services_dir)
        if os.path.isdir(os.path.join(services_dir, d))
    )

    failed = 0
    for service_dir in service_dirs:
        name = os.path.basename(service_dir)
        logger.info("Compiling %s", name)
        result = subprocess.run(
            ["dbt", "compile", "--profiles-dir", "."],
            cwd=service_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error("dbt compile failed for %s: %s", name, result.stderr.strip())
            failed += 1
        else:
            logger.info("Compiled %s successfully", name)

    if failed > 0:
        logger.error("%d service(s) failed to compile", failed)
        sys.exit(1)
    logger.info("All services compiled successfully")


if __name__ == "__main__":
    main()
