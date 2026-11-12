import logging
import os
import posixpath
import tempfile

from botocore.exceptions import ClientError

from domain.model import ManifestFile, ManifestKind, ManifestRequest

logger = logging.getLogger(__name__)

EXTRAS_OBJECT = "compile-extras.json"
_ABSENT_CODES = {"404", "NoSuchKey", "NotFound"}
_FORBIDDEN_CODES = {"403", "AccessDenied", "Forbidden"}


class S3Source:
    """Downloads manifests from S3 to a local temp dir, returns ManifestFile objects.

    Accepts an explicit list of ManifestRequest within the bucket. Each request
    maps to exactly one ManifestFile; declared_service is propagated onto the
    ManifestFile so the handler can validate that the downloaded manifest
    actually belongs to the declared service, and kind rides along from the
    event so the handler knows which parser to use. No S3 listing is performed.

    A dbt manifest is fetched with the compile-extras.json that sits beside it
    (same prefix); a release whose compile wrote none has extras_path "". A
    python contract has no extras object.
    Keys are provided by the release.requested:v1 event payload
    (manifest_keys[].s3_uri stripped to a plain key by the caller).

    image_tag is left empty here; the candidate handler joins the release
    request's per-service image tags onto the nodes.
    """

    def __init__(self, bucket: str, env: str, s3_client, keys: list[ManifestRequest]) -> None:
        self._bucket = bucket
        self._env = env
        self._s3 = s3_client
        self._keys = keys
        self._tmpdir = tempfile.TemporaryDirectory()

    def manifest_count(self) -> int:
        return len(self._keys)

    def list_manifests(self) -> list[ManifestFile]:
        result = []
        for request in self._keys:
            local_path = os.path.join(self._tmpdir.name, request.key.replace("/", "_"))
            self._s3.download_file(self._bucket, request.key, local_path)
            result.append(ManifestFile(
                path=local_path, image_tag="",
                declared_service=request.service, kind=request.kind,
                size_bytes=os.path.getsize(local_path),
                extras_path=self._download_extras(request) if request.kind == ManifestKind.DBT else "",
            ))
        return result

    def _download_extras(self, request: ManifestRequest) -> str:
        """Fetch the extras object beside the request's manifest and return its
        local path, or "" when the object is missing. Any other S3 error
        propagates, as a manifest download failure does, so the message stays
        pending and is redelivered."""
        key = posixpath.join(posixpath.dirname(request.key), EXTRAS_OBJECT)
        local_path = os.path.join(self._tmpdir.name, key.replace("/", "_"))
        fields = {"service": request.service, "key": key}
        try:
            self._s3.download_file(self._bucket, key, local_path)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in _ABSENT_CODES:
                logger.info(f"no {EXTRAS_OBJECT} beside the manifest: {key}", extra=fields)
                return ""
            if code in _FORBIDDEN_CODES:
                # The object sits beside the manifest just read with the same
                # credentials, so a 403 here is a missing object the credentials
                # may not list.
                logger.warning(
                    f"{key} was refused with {code}: treating it as missing; granting"
                    " topology-controller s3:ListBucket on the manifest prefix turns a"
                    " missing object into a 404",
                    extra=fields,
                )
                return ""
            raise
        return local_path

    def cleanup(self) -> None:
        self._tmpdir.cleanup()
