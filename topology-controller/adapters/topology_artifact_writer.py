"""Stores a release's topology artifact in S3 (see service/topology_artifact.py).

The object is the gzip stream of the canonical document, written with
Content-Type application/gzip and no Content-Encoding, so no HTTP client
decompresses it in transit: the bytes a reader downloads are the bytes whose
SHA-256 the parse result carries. The gzip header holds no timestamp and no
file name, so one release always produces the same bytes and a redelivered
request rewrites the object unchanged.
"""
import gzip
import hashlib

from domain.model import TopologyArtifactRef
from service.topology_artifact import canonical_topology_json, topology_artifact_key


class TopologyArtifactWriter:
    def __init__(self, s3_client, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def write(self, *, tenant_id: str, release_id: str, nodes: list[dict]) -> TopologyArtifactRef:
        body = gzip.compress(
            canonical_topology_json(tenant_id=tenant_id, release_id=release_id, nodes=nodes),
            mtime=0,
        )
        key = topology_artifact_key(tenant_id, release_id)
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/gzip")
        return TopologyArtifactRef(
            uri=f"s3://{self._bucket}/{key}",
            sha256=hashlib.sha256(body).hexdigest(),
            node_count=len(nodes),
        )
