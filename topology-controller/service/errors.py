"""Marks a message no redelivery can process. The release.requested binding
raises it for a malformed message; the consumer's error classifier reads it and
dead-letters the message at once."""


class PermanentMessageError(ValueError):
    """A message no redelivery can process, such as a missing or malformed
    payload. The consumer dead-letters it at once."""
