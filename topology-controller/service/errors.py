"""Failure markers the application layer raises and the transport adapters read."""


class PermanentMessageError(ValueError):
    """A message no redelivery can process, such as a missing or malformed
    payload. The consumer dead-letters it at once."""
