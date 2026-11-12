"""Which parser reads which kind of release artifact, and how each one fails.

One release may carry a dbt manifest.json and a python contract.yaml at once.
They are parsed by different code with different failure vocabularies, but they
produce the same ParsedArtifact, so this is the only place downstream code has to
know a kind exists. A dbt manifest is parsed together with the compile-extras.json
downloaded beside it, when the release has one.
"""
import json
from dataclasses import dataclass
from typing import Protocol

from domain.exceptions import InvalidCompileExtrasError, InvalidNodeSettingError, MalformedContractError
from domain.model import ManifestFile, ManifestKind, ParsedArtifact, SkippedNode
from service.compile_extras import load_compile_extras
from service.parser import parse_manifest
from service.python_contract_parser import parse_python_contract


class ParseFn(Protocol):
    """A kind's parse function: the downloaded artifact to its nodes and shared
    code. A node the parse leaves out without failing is appended to `skipped`
    when it is given."""

    def __call__(self, mf: ManifestFile, *, skipped: list[SkippedNode] | None = None) -> ParsedArtifact:
        ...


def _parse_dbt(mf: ManifestFile, *, skipped: list[SkippedNode] | None = None) -> ParsedArtifact:
    """Parse a dbt manifest with the compile extras downloaded beside it, when
    there are any. The extras must name the manifest's declared service."""
    extras = load_compile_extras(mf.extras_path, mf.declared_service) if mf.extras_path else None
    return parse_manifest(mf.path, mf.image_tag, extras, skipped=skipped)


def _parse_python(mf: ManifestFile, *, skipped: list[SkippedNode] | None = None) -> ParsedArtifact:
    return parse_python_contract(mf.path, mf.image_tag, skipped=skipped)


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
        parse=_parse_dbt,
        permanent_errors=(
            json.JSONDecodeError, KeyError, IndexError, InvalidNodeSettingError, InvalidCompileExtrasError,
        ),
        empty_detail="manifest contains no model/seed nodes",
    ),
    ManifestKind.PYTHON: ManifestParser(
        parse=_parse_python,
        permanent_errors=(MalformedContractError,),
        empty_detail="contract declares no nodes",
    ),
}


def parser_for(kind: str) -> ManifestParser | None:
    """Resolve a wire kind value, or None if this build cannot parse it."""
    return _PARSERS.get(kind)
