import json
import logging
import os
import tempfile
from botocore.exceptions import ClientError
from adapters.sources import ManifestSource
from domain.exceptions import MalformedRuntimeManifestError
from domain.model import ManifestFile, RuntimeManifestRef
from service.runtime_manifest import parse_descriptor

logger = logging.getLogger(__name__)

# The error codes S3 returns for a key that does not exist. Only these mean
# "this release has no runtime manifest"; every other code (auth, throttling)
# is a fault that a retry may resolve and must not be read as absence.
_ABSENT_KEY_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class S3Source(ManifestSource):
    """Downloads manifests from S3 to a local temp dir, returns ManifestFile objects.

    Accepts an explicit list of (declared_service, object_key) pairs within the
    bucket. Each pair maps to exactly one ManifestFile; declared_service is
    propagated onto the ManifestFile so the handler can validate that the
    downloaded manifest actually belongs to the declared service. No S3 listing
    is performed. Keys are provided by the release.requested:v1 event payload
    (manifest_keys[].s3_uri stripped to a plain key by the caller).

    image_tag is left empty by design; release-controller joins the per-service
    image tags from the POST /releases body onto the topology.

    Each manifest may be accompanied by a runtime manifest descriptor at the
    sibling runtime-manifest.json key, describing the pre-built dbt partial parse
    uploaded beside it. The descriptor's key is derived from the manifest's own
    key, so no listing is needed to find it.
    """

    def __init__(self, bucket: str, env: str, s3_client, keys: list[tuple[str, str]]) -> None:
        self._bucket = bucket
        self._env = env
        self._s3 = s3_client
        self._keys = keys
        self._tmpdir = tempfile.TemporaryDirectory()

    def list_manifests(self) -> list[ManifestFile]:
        result = []
        for declared_service, key in self._keys:
            local_path = os.path.join(self._tmpdir.name, key.replace("/", "_"))
            self._s3.download_file(self._bucket, key, local_path)
            result.append(ManifestFile(
                path=local_path,
                version="",
                image_tag="",
                declared_service=declared_service,
                runtime_manifest=self._read_runtime_manifest(declared_service, key),
            ))
        return result

    def _read_runtime_manifest(self, declared_service: str, key: str) -> RuntimeManifestRef | None:
        """Return the validated ref for the manifest at key, or None if the release
        carries no descriptor.

        Raises MalformedRuntimeManifestError for a descriptor that exists but is
        unusable; S3 faults other than an absent key propagate unchanged so the
        caller can retry them.
        """
        prefix = key.rsplit("/", 1)[0]
        descriptor_key = f"{prefix}/runtime-manifest.json"
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=descriptor_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in _ABSENT_KEY_CODES:
                raise
            logger.info(
                "No runtime manifest descriptor; manifest-only release",
                extra={"service": declared_service, "descriptor_key": descriptor_key},
            )
            return None

        raw = response["Body"].read()
        try:
            descriptor = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MalformedRuntimeManifestError(
                f"s3://{self._bucket}/{descriptor_key}: not valid JSON: {exc}"
            ) from exc

        return parse_descriptor(
            descriptor,
            expected_service=declared_service,
            expected_artifact_uri=f"s3://{self._bucket}/{prefix}/partial_parse.msgpack",
        )

    def cleanup(self) -> None:
        self._tmpdir.cleanup()
