"""The install's company ontology, as a release carries it.

A release that carries a manifest of kind `ontology` holds one YAML file that declares no
node: it names the projects the ontology covers and the things their contracts may be bound
to. With the install's ontology switch on, the file is read once, before any node manifest,
and the contracts of the projects it declares are resolved and checked against it. This
module reads and checks the file on its own, says how each contract is stored (its ontology
part taken away, kept as written or filled in) and describes the ontology a release used for
its contract bundle.
"""
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from continuo_contracts import ontology as ontology_file
from continuo_contracts.errors import OntologyError
from continuo_contracts.ontology import Ontology
from continuo_contracts.ontology_check import SEVERITIES, Finding, ontology_findings

from domain.exceptions import InvalidOntologyError
from domain.model import ManifestFile

_ERROR = "error"

# What a release with the switch on and no ontology says in its bundle: its contracts keep
# the bindings they write, and nothing checks them.
_NO_ONTOLOGY = Finding(
    severity="warning",
    rule="contract",
    subject="ontology",
    message="the ontology switch is on and no ontology is released: bindings are kept as written and not checked",
    contract_id=None,
)


@dataclass(frozen=True)
class ReleaseOntology:
    """The ontology a release carries: the project that released the file, the release
    that wrote it (not necessarily the one being parsed), the hex SHA-256 of the file's
    text, the loaded ontology, and what its own rules found that is not an error."""
    project: str
    release_id: str
    sha256: str
    ontology: Ontology
    findings: tuple[Finding, ...]


def load_release_ontology(mf: ManifestFile) -> ReleaseOntology:
    """Read the ontology file of `mf`, load it and run the rule that checks the file alone.

    Raises InvalidOntologyError, whose message leads with the project that released the
    file, when the key does not name the release that wrote the file, when the file is
    not UTF-8 text or is not an ontology file the library reads, and when its own rule
    finds an error; the message then names each error and `rule_errors` the rule of each.
    A finding that is not an error is returned in `findings`.
    """
    project = mf.declared_project or mf.path
    if not mf.release_id:
        raise InvalidOntologyError(
            f"{project}: the release id of the ontology cannot be read from its key,"
            " which must be <project>/<release id>/ontology.yaml"
        )
    try:
        with open(mf.path, encoding="utf-8") as f:
            text = f.read()
    except UnicodeDecodeError as exc:
        raise InvalidOntologyError(f"{project}: the file is not UTF-8 text ({exc.reason})") from exc
    try:
        loaded = ontology_file.load(text)
    except OntologyError as exc:
        raise InvalidOntologyError(f"{project}: {exc}") from exc

    findings = ontology_findings(loaded)
    errors = [finding for finding in findings if finding.severity == _ERROR]
    if errors:
        raise InvalidOntologyError(
            f"{project}: " + "; ".join(f"{error.subject}: {error.message}" for error in errors),
            rule_errors=tuple(error.rule for error in errors),
        )
    return ReleaseOntology(
        project=project,
        release_id=mf.release_id,
        sha256=loaded.sha256,
        ontology=loaded,
        findings=tuple(findings),
    )


def bind_contract(
    stored: dict[str, Any], *, enabled: bool, release_ontology: ReleaseOntology | None,
) -> dict[str, Any]:
    """The form in which a contract is kept and hashed, given its stored form.

    With the switch off (`enabled` false) the ontology part is taken away, so a contract
    that writes bindings stores and hashes as the same contract without them. With the
    switch on and no ontology the stored form is kept as written. With an ontology the
    stored form is filled in from it (`resolve`), which leaves the contract of a project
    the ontology does not declare as it is.
    """
    if not enabled:
        return ontology_file.strip(stored)
    if release_ontology is None:
        return stored
    return ontology_file.resolve(stored, release_ontology.ontology)


def ontology_envelope(
    *, enabled: bool, release_ontology: ReleaseOntology | None, findings: Sequence[Finding],
) -> dict[str, Any] | None:
    """The `ontology` value of a release's contract bundle.

    None with the switch off. Otherwise `enabled` is true and the ontology the release
    used is named by its project, the release that wrote the file and its SHA-256, with
    the projects it declares, sorted; a release that used none has `null` for each of
    the three and no declared project. `findings` are the check's findings that do not
    reject the release, and the envelope adds those the ontology file's own rules gave
    and, with no ontology, the warning that the switch is on and none is released. They
    are listed in the order the library sorts findings: by severity, rule, subject and
    message.
    """
    if not enabled:
        return None
    listed = [*findings, *(release_ontology.findings if release_ontology is not None else (_NO_ONTOLOGY,))]
    listed.sort(key=lambda f: (SEVERITIES.index(f.severity), f.rule, f.subject, f.message))
    return {
        "enabled": True,
        "project": release_ontology.project if release_ontology is not None else None,
        "release_id": release_ontology.release_id if release_ontology is not None else None,
        "sha256": release_ontology.sha256 if release_ontology is not None else None,
        "declared_projects": list(release_ontology.ontology.projects()) if release_ontology is not None else [],
        "findings": [
            {
                "severity": f.severity,
                "rule": f.rule,
                "subject": f.subject,
                "message": f.message,
                "contract_id": f.contract_id,
            }
            for f in listed
        ],
    }
