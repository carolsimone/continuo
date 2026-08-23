import logging
import os
import tempfile
from adapters.sources import ManifestSource
from domain.model import ManifestFile, ManifestRequest

logger = logging.getLogger(__name__)


class S3Source(ManifestSource):
    """Downloads manifests from S3 to a local temp dir, returns ManifestFile objects.

    Accepts an explicit list of ManifestRequest within the bucket. Each request
    maps to exactly one ManifestFile; declared_service is propagated onto the
    ManifestFile so the handler can validate that the downloaded manifest
    actually belongs to the declared service, and kind rides along from the
    event so the handler knows which parser to use. No S3 listing is performed.
    Keys are provided by the release.requested:v1 event payload
    (manifest_keys[].s3_uri stripped to a plain key by the caller).

    image_tag is left empty by design; release-controller joins the per-service
    image tags from the POST /releases body onto the topology.
    """

    def __init__(self, bucket: str, env: str, s3_client, keys: list[ManifestRequest]) -> None:
        self._bucket = bucket
        self._env = env
        self._s3 = s3_client
        self._keys = keys
        self._tmpdir = tempfile.TemporaryDirectory()

    def list_manifests(self) -> list[ManifestFile]:
        result = []
        for request in self._keys:
            local_path = os.path.join(self._tmpdir.name, request.key.replace("/", "_"))
            self._s3.download_file(self._bucket, request.key, local_path)
            result.append(ManifestFile(
                path=local_path, version="", image_tag="",
                declared_service=request.service, kind=request.kind,
            ))
        return result

    def cleanup(self) -> None:
        self._tmpdir.cleanup()
