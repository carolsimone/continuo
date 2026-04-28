import json
import logging
import os
from adapters.sources import ManifestSource
from domain.model import ManifestFile
from service.version import _VERSION_RE, parse_version

logger = logging.getLogger(__name__)


class LocalFilesystemSource(ManifestSource):
    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def list_manifests(self) -> list[ManifestFile]:
        result = []
        try:
            entries = sorted(os.scandir(self._base_path), key=lambda e: e.name)
        except FileNotFoundError:
            return []

        for entry in entries:
            if not entry.is_dir():
                continue
            candidates: list[tuple[int, str]] = []
            for filename in os.listdir(entry.path):
                m = _VERSION_RE.match(filename)
                if m:
                    n = int(m.group(1)[1:])  # "v3" → 3
                    candidates.append((n, filename))
            if not candidates:
                logger.warning("No versioned manifest found in service dir — skipping",
                               extra={"service_dir": entry.path})
                continue
            _, filename = max(candidates)
            version = parse_version(filename)

            image_tag = ""
            meta_path = os.path.join(entry.path, "service_metadata.json")
            if os.path.exists(meta_path):
                try:
                    with open(meta_path) as mf:
                        meta = json.load(mf)
                    image_tag = meta.get("image_tag", "")
                except Exception:
                    logger.warning(
                        "Failed to read service_metadata.json — image_tag will be empty",
                        extra={"meta_path": meta_path},
                    )

            result.append(ManifestFile(
                path=os.path.join(entry.path, filename),
                version=version,
                image_tag=image_tag,
            ))
        return result
