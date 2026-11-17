"""An in-memory stand-in for the boto3 S3 client the parse writes through.

It implements the three calls the release parse makes, `put_object`,
`get_object` and `download_file`, counts the puts and their bytes, and can sleep
a fixed time in every call to stand in for a network round trip.
"""
import io
import time
from collections.abc import Mapping
from pathlib import Path

from botocore.exceptions import ClientError


class MemoryS3:
    """Holds objects by key. The objects it starts with are not counted: only
    puts made after construction (or after `reset_counts`) are."""

    def __init__(self, objects: Mapping[str, bytes] | None = None, *, put_latency_ms: float = 0.0) -> None:
        self._objects: dict[str, bytes] = dict(objects or {})
        self._latency_seconds = put_latency_ms / 1000
        self.puts = 0
        self.put_bytes = 0

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs) -> dict:
        self._wait()
        self._objects[Key] = Body
        self.puts += 1
        self.put_bytes += len(Body)
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        """The object's body, or the ClientError S3 answers for a key that is not there."""
        self._wait()
        if Key not in self._objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."},
                 "ResponseMetadata": {"HTTPStatusCode": 404}},
                "GetObject",
            )
        return {"Body": io.BytesIO(self._objects[Key])}

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        self._wait()
        Path(Filename).write_bytes(self._objects[Key])

    def reset_counts(self) -> None:
        self.puts = 0
        self.put_bytes = 0

    def _wait(self) -> None:
        if self._latency_seconds:
            time.sleep(self._latency_seconds)
