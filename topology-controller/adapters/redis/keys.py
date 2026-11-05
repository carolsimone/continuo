"""Redis keys of the contract streams, and the NOMKSTREAM append every producer uses.

Every stream the system reads or writes lives under streams_contract.KEY_PREFIX.
No process creates a stream or a group: the streams-init deploy step does.
"""

from streams_contract import KEY_PREFIX


class MissingStreamError(RuntimeError):
    """A producer's XADD found no stream at its key. The streams-init step
    creates the stream, so the append is retried until it has."""


def key(name: str) -> str:
    """The Redis key of the contract stream name: KEY_PREFIX followed by it."""
    return KEY_PREFIX + name


def xadd_existing(client, name: str, fields: dict) -> None:
    """Append fields to the stream key(name) without creating it. Redis answers
    an XADD with NOMKSTREAM on a missing stream with a nil reply, which raises
    MissingStreamError."""
    target = key(name)
    if client.xadd(target, fields, nomkstream=True) is None:
        raise MissingStreamError(f"stream {target} does not exist; the streams-init step creates it")
