"""The install's company ontology, as a release carries it.

A release that carries a manifest of kind `ontology` holds one YAML file that declares no
node: it names the services the ontology covers and the things their contracts may be bound
to. With the install's ontology switch on, the file is read once, before any node manifest,
and the contracts of the services it declares are resolved and checked against it. This
module reads and checks the file on its own.
"""
from dataclasses import dataclass

from continuo_contracts import ontology as ontology_file
from continuo_contracts.errors import OntologyError
from continuo_contracts.ontology import Ontology
from continuo_contracts.ontology_check import Finding, ontology_findings

from domain.exceptions import InvalidOntologyError
from domain.model import ManifestFile

_ERROR = "error"


@dataclass(frozen=True)
class ReleaseOntology:
    """The ontology a release carries: the service that released the file, the release
    that wrote it (not necessarily the one being parsed), the hex SHA-256 of the file's
    text, the loaded ontology, and what its own rules found that is not an error."""
    service: str
    release_id: str
    sha256: str
    ontology: Ontology
    findings: tuple[Finding, ...]


def load_release_ontology(mf: ManifestFile) -> ReleaseOntology:
    """Read the ontology file of `mf`, load it and run the rule that checks the file alone.

    Raises InvalidOntologyError, whose message leads with the service that released the
    file, when the key does not name the release that wrote the file, when the file is
    not UTF-8 text or is not an ontology file the library reads, and when its own rule
    finds an error; the message then names each error and `rule_errors` the rule of each.
    A finding that is not an error is returned in `findings`.
    """
    service = mf.declared_service or mf.path
    if not mf.release_id:
        raise InvalidOntologyError(
            f"{service}: the release id of the ontology cannot be read from its key,"
            " which must be <service>/<release id>/ontology.yaml"
        )
    try:
        with open(mf.path, encoding="utf-8") as f:
            text = f.read()
    except UnicodeDecodeError as exc:
        raise InvalidOntologyError(f"{service}: the file is not UTF-8 text ({exc.reason})") from exc
    try:
        loaded = ontology_file.load(text)
    except OntologyError as exc:
        raise InvalidOntologyError(f"{service}: {exc}") from exc

    findings = ontology_findings(loaded)
    errors = [finding for finding in findings if finding.severity == _ERROR]
    if errors:
        raise InvalidOntologyError(
            f"{service}: " + "; ".join(f"{error.subject}: {error.message}" for error in errors),
            rule_errors=tuple(error.rule for error in errors),
        )
    return ReleaseOntology(
        service=service,
        release_id=mf.release_id,
        sha256=loaded.sha256,
        ontology=loaded,
        findings=tuple(findings),
    )
