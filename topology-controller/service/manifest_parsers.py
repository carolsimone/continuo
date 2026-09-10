"""Which parser reads which kind of release artifact, and how each one fails.

One release may carry a dbt manifest.json and a python contract.yaml at once.
They are parsed by different code with different failure vocabularies, but they
produce the same ManifestNode, so this is the only place downstream code has to
know a kind exists.
"""
import json
from collections.abc import Callable
from dataclasses import dataclass

from domain.exceptions import MalformedContractError
from domain.model import ManifestKind, ManifestNode
from service.parser import parse_manifest
from service.python_contract_parser import parse_python_contract

ParseFn = Callable[[str, str, str], tuple[list[ManifestNode], dict]]


@dataclass(frozen=True)
class ManifestParser:
    """A kind's parser plus everything the handler needs to report its failures.

    permanent_errors are the exceptions a re-delivery cannot fix, so the handler
    publishes status=failed and the consumer ACKs. Every permanent parse error
    is an invalid artifact: the handler reports it under that kind.
    """
    parse: ParseFn
    permanent_errors: tuple[type[Exception], ...]
    empty_detail: str


_PARSERS: dict[str, ManifestParser] = {
    ManifestKind.DBT: ManifestParser(
        parse=parse_manifest,
        permanent_errors=(json.JSONDecodeError, KeyError, IndexError),
        empty_detail="manifest contains no model/seed nodes",
    ),
    ManifestKind.PYTHON: ManifestParser(
        parse=parse_python_contract,
        permanent_errors=(MalformedContractError,),
        empty_detail="contract declares no nodes",
    ),
}


def parser_for(kind: str) -> ManifestParser | None:
    """Resolve a wire kind value, or None if this build cannot parse it."""
    return _PARSERS.get(kind)
