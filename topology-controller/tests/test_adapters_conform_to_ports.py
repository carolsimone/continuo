"""Drift guard: each concrete adapter still satisfies the port it implements.

The adapters implement the service.ports protocols structurally — they do not
inherit them — so nothing forces them to stay in sync as either side changes.
There is no static type checker in this service's CI, so a renamed adapter
method would otherwise only surface at runtime. These runtime_checkable
issubclass checks are structural (they inspect the methods the port declares),
so they fail the moment an adapter stops providing the port's surface.
"""
import pytest

from adapters.code_bundle_uploader import CodeBundleUploader
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.sources.s3 import S3Source
from service.ports import (
    CandidatePublisherPort,
    CodeBundleUploaderPort,
    ManifestSourcePort,
)

CONCRETE_PORT_PAIRS = [
    (CandidateManifestPublisher, CandidatePublisherPort),
    (CodeBundleUploader, CodeBundleUploaderPort),
    (S3Source, ManifestSourcePort),
]


@pytest.mark.parametrize(
    "concrete,port",
    CONCRETE_PORT_PAIRS,
    ids=[c.__name__ for c, _ in CONCRETE_PORT_PAIRS],
)
def test_adapter_conforms_to_its_port(concrete, port):
    assert issubclass(concrete, port), (
        f"{concrete.__name__} no longer satisfies {port.__name__} — the adapter "
        f"and its port have drifted apart"
    )


def test_check_is_structural_not_nominal():
    """A class missing one of the port's methods fails the check, proving the
    guard inspects the surface rather than trusting inheritance."""

    class MissingPublishFailed:
        def publish_ok(self, release_id, topology, code_bundle_uri=""):
            ...

    assert not issubclass(MissingPublishFailed, CandidatePublisherPort)
