"""Test doubles shared by the candidate-handler test modules."""
from domain.model import TopologyArtifactRef


class RecordingArtifactWriter:
    """A TopologyArtifactWriterPort that records every write and returns a
    reference shaped like the real writer's. Pass `fail` to make write raise
    it instead."""

    def __init__(self, fail: BaseException | None = None) -> None:
        self.writes: list[dict] = []
        self.refs: list[TopologyArtifactRef] = []
        self._fail = fail

    def write(self, *, tenant_id: str, release_id: str, nodes: list[dict]) -> TopologyArtifactRef:
        if self._fail is not None:
            raise self._fail
        self.writes.append({"tenant_id": tenant_id, "release_id": release_id, "nodes": list(nodes)})
        ref = TopologyArtifactRef(
            uri=f"s3://continuo/tenants/{tenant_id}/topologies/{release_id}/topology.json.gz",
            sha256=f"{len(self.writes):064x}",
            node_count=len(nodes),
        )
        self.refs.append(ref)
        return ref

    @property
    def nodes(self) -> list[dict]:
        """The nodes of the one artifact this writer stored."""
        assert len(self.writes) == 1, f"expected one artifact write, got {len(self.writes)}"
        return self.writes[0]["nodes"]
