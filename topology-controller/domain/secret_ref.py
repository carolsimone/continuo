"""The Secret name a python-api contract may name.

Mirrors pkg/domain/model.ValidateApiSecretRef; both are pinned to
pkg/domain/model/testdata/secret_ref_cases.json.
"""
import re

from domain.exceptions import MalformedContractError

SECRET_REF_PATTERN = re.compile(r"continuo-api-[a-z0-9]([-a-z0-9]*[a-z0-9])?")
SECRET_REF_MAX_LEN = 253


def validate_secret_ref(value, label: str) -> str:
    # fullmatch, not match with "$": "$" also accepts a trailing newline.
    if (
        not isinstance(value, str)
        or len(value) > SECRET_REF_MAX_LEN
        or not SECRET_REF_PATTERN.fullmatch(value)
    ):
        raise MalformedContractError(
            f"{label}: secret_ref must be a Kubernetes Secret name starting with"
            f" 'continuo-api-' (at most {SECRET_REF_MAX_LEN} chars), got {value!r}"
        )
    return value
