"""Target configuration loading and service directory resolution."""
import os

import yaml


def load_target(targets_path: str, name: str) -> dict:
    """Load a named S3 target profile from a YAML file.

    Credential resolution: env vars AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    override any values in the YAML.
    """
    with open(targets_path) as f:
        data = yaml.safe_load(f)

    targets = data.get("targets", {})
    if name not in targets:
        available = ", ".join(sorted(targets.keys()))
        raise ValueError(
            f"Unknown target '{name}'. Available targets: {available}"
        )

    target = dict(targets[name])

    # Env vars override YAML credentials
    env_key = os.environ.get("AWS_ACCESS_KEY_ID")
    if env_key:
        target["access_key_id"] = env_key
    env_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if env_secret:
        target["secret_access_key"] = env_secret

    if "access_key_id" not in target or "secret_access_key" not in target:
        raise ValueError(
            f"Target '{name}' requires credentials. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars "
            "or add access_key_id/secret_access_key to targets.yaml."
        )

    return target


def resolve_service_dirs(
    paths: list[str] | None, services_dir: str | None
) -> list[str]:
    """Resolve CLI arguments into a sorted list of absolute service directories.

    Either explicit paths or --services-dir must be provided, not both.
    """
    if paths:
        return [os.path.abspath(p) for p in paths]

    if services_dir:
        base = os.path.abspath(services_dir)
        return sorted(
            os.path.join(base, d)
            for d in os.listdir(base)
            if os.path.isdir(os.path.join(base, d))
        )

    raise ValueError(
        "Provide either service paths as positional arguments or --services-dir"
    )
