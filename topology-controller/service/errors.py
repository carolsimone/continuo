"""The errors of the application layer, each a failure no fault of a team's
release artifact explains. The release.requested binding raises
PermanentMessageError for a malformed message, and the consumer's error
classifier reads it and dead-letters the message at once. The dbt parse raises
ContractGenerationError for a defect of the contracts library, and the contract
bundle writer raises UnreadableContractBundleError for a stored bundle it cannot
read; the candidate handler reports the release as internal for either."""


class PermanentMessageError(ValueError):
    """A message no redelivery can process, such as a missing or malformed
    payload. The consumer dead-letters it at once."""


class ContractGenerationError(RuntimeError):
    """The contracts library failed where a manifest does not explain the failure:
    a call raised an error the library does not raise for a malformed node, a
    contract it generated could not be checked, or the run facts of a valid
    contract hold a value the topology cannot take. It is a defect in continuo,
    not a fault of the team's project, so the release fails as internal, never
    as an invalid artifact or an invalid contract."""


class UnreadableContractBundleError(RuntimeError):
    """The contract bundle already stored at a release's key cannot be read: it
    is not gzip, its JSON cannot be decoded, or it holds no `summary.by_family`
    of counts. A release's bundle is written once, so a redelivery meets the
    same object, and the release fails as internal."""
