import json
import logging
import os
import tempfile
from botocore.exceptions import ClientError
from adapters.sources import ManifestSource
from domain.model import ManifestFile
from service.version import _VERSION_RE, parse_version

logger = logging.getLogger(__name__)


class S3Source(ManifestSource):
    """Downloads manifests from S3 to a local temp dir, returns ManifestFile objects."""

    def __init__(self, bucket: str, env: str, s3_client) -> None:
        self._bucket = bucket
        self._env = env
        self._s3 = s3_client
        self._tmpdir = tempfile.TemporaryDirectory()

    def list_manifests(self) -> list[ManifestFile]:
        prefix = f"{self._env}/manifest/"
        response = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        contents = response.get("Contents", [])

        # Group all keys by service prefix
        all_by_service: dict[str, list[str]] = {}
        root_prefix = prefix.rstrip("/")
        for obj in contents:
            key = obj["Key"]
            service_prefix = key.rsplit("/", 1)[0]
            if service_prefix == root_prefix:
                continue  # skip keys directly under the root prefix
            all_by_service.setdefault(service_prefix, []).append(key)

        result = []
        for service_prefix in sorted(all_by_service):
            keys = all_by_service[service_prefix]
            candidates: list[tuple[int, str]] = []
            for key in keys:
                filename = key.split("/")[-1]
                m = _VERSION_RE.match(filename)
                if m:
                    n = int(m.group(1)[1:])  # "v3" → 3
                    candidates.append((n, key))
            if not candidates:
                logger.warning("No versioned manifest found for S3 prefix — skipping",
                               extra={"service_prefix": service_prefix})
                continue
            _, key = max(candidates)
            filename = key.split("/")[-1]
            version = parse_version(filename)
            local_path = os.path.join(self._tmpdir.name, key.replace("/", "_"))
            self._s3.download_file(self._bucket, key, local_path)

            image_tag = ""
            meta_key = f"{service_prefix}/service_metadata.json"
            try:
                meta_obj = self._s3.get_object(Bucket=self._bucket, Key=meta_key)
                meta = json.loads(meta_obj["Body"].read())
                image_tag = meta.get("image_tag", "")
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "NoSuchKey":
                    logger.warning(
                        "service_metadata.json missing — image_tag will be empty",
                        extra={"service_prefix": service_prefix},
                    )
                else:
                    raise

            result.append(ManifestFile(path=local_path, version=version, image_tag=image_tag))

        return result

    def cleanup(self) -> None:
        self._tmpdir.cleanup()
