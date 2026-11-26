"""Which parser reads which kind of release artifact, and how each one fails.

One release may carry a dbt manifest.json and a python odcs-bundle.json per
project. They are parsed by different code with different failure vocabularies,
but they produce the same ParsedArtifact, so this is the only place downstream
code has to know a kind exists. A dbt manifest is parsed together with the
compile-extras.json downloaded beside it, when the release has one.
"""
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from domain.exceptions import (
    InvalidCompileExtrasError,
    InvalidNodeSettingError,
    InvalidOdcsBundleError,
)
from domain.model import ManifestFile, ManifestKind, ParsedArtifact, SkippedNode
from service.compile_extras import load_compile_extras
from service.contract_validation import ContractValidator
from service.odcs_bundle_parser import parse_odcs_bundle
from service.parser import parse_manifest
from service.release_timings import ReleaseTimings


class ParseFn(Protocol):
    """A kind's parse function: the downloaded artifact to its nodes and shared
    code. A node the parse leaves out without failing is appended to `skipped`
    when it is given. `progress` is called as the parse works through its nodes,
    `timings` is the release's timing record and `validator` checks each contract
    (the one the process keeps, so a contract unchanged since an earlier release
    is answered from its memory)."""

    def __call__(
        self,
        mf: ManifestFile,
        *,
        skipped: list[SkippedNode] | None = None,
        progress: Callable[[], None],
        timings: ReleaseTimings,
        validator: ContractValidator,
    ) -> ParsedArtifact:
        ...


def _parse_dbt(
    mf: ManifestFile, *, skipped: list[SkippedNode] | None = None,
    progress: Callable[[], None], timings: ReleaseTimings, validator: ContractValidator,
) -> ParsedArtifact:
    """Parse a dbt manifest with the compile extras downloaded beside it, when
    there are any. The extras must name the manifest's declared project, and the
    declared project stands for the manifest's project when the manifest does
    not name it. Each node's contract is checked with `validator`."""
    extras = load_compile_extras(mf.extras_path, mf.declared_project) if mf.extras_path else None
    return parse_manifest(
        mf.path, mf.image_tag, extras, skipped=skipped, declared_project=mf.declared_project,
        validator=validator, progress=progress, timings=timings,
    )


@dataclass(frozen=True)
class ManifestParser:
    """A kind's parser plus everything the handler needs to report its failures.

    permanent_errors are the exceptions a re-delivery cannot fix, so the handler
    publishes status=failed and the consumer ACKs. Every permanent parse error
    is an invalid artifact: the handler reports it under that kind. A
    ContractGenerationError is not one: it is a defect, which the handler
    reports as internal.
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
    ManifestKind.PYTHON_ODCS: ManifestParser(
        parse=parse_odcs_bundle,
        permanent_errors=(InvalidOdcsBundleError,),
        empty_detail="bundle declares no nodes",
    ),
}


def parser_for(kind: str) -> ManifestParser | None:
    """Resolve a wire kind value, or None if this build cannot parse it."""
    return _PARSERS.get(kind)
