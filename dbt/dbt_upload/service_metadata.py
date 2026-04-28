"""Parse IMAGE_TAG_PER_SERVICE env and write service_metadata.json per service."""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class MissingImageTagError(ValueError):
    """Raised when a service has no image_tag — refuses silent fallback to 'latest'."""


def parse_image_tag_env(raw: str) -> dict[str, str]:
    """Parse 'svc1=tag1,svc2=tag2' into {'svc1': 'tag1', 'svc2': 'tag2'}.

    Empty string returns {}. Malformed entries (no '=') raise ValueError.
    """
    if not raw or not raw.strip():
        return {}

    out: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(f"malformed entry in IMAGE_TAG_PER_SERVICE: {entry!r}")
        svc, tag = entry.split("=", 1)
        out[svc.strip()] = tag.strip()
    return out


def write_service_metadata_json(
    out_dir: Path,
    service_name: str,
    manifest_version: str,
    image_tag: str,
) -> None:
    """Write {out_dir}/service_metadata.json containing {manifest_version, image_tag}.

    Refuses to write an empty image_tag — fail loud rather than poison downstream snapshots.
    """
    if not image_tag:
        raise MissingImageTagError(
            f"image_tag empty for service {service_name!r} — refuse to write service_metadata.json"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "service_metadata.json"
    target.write_text(json.dumps({
        "manifest_version": manifest_version,
        "image_tag": image_tag,
    }))
    logger.info(
        "Wrote service_metadata.json",
        extra={"service": service_name, "image_tag": image_tag, "manifest_version": manifest_version},
    )
