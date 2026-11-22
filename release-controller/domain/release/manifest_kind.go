package release

import "fmt"

// ManifestKind discriminates how a service's release artifact is authored and
// parsed: a dbt manifest.json, a python service's odcs-bundle.json (one ODCS
// contract per node), or the install's ontology.yaml (the company ontology,
// which declares no node). It travels POST /releases → Release →
// service_prod.manifest_kind → ManifestKey → the per-entry kind on
// release.requested:v1. release-controller only moves (service, pointer, kind)
// triples; topology-controller is the sole component that parses
// kind-specifically. The kinds ParseManifestKind accepts must match
// topology-controller's ManifestKind enum (domain/model.py); only
// ManifestKindPython, which only a stored row holds, is not in it.
type ManifestKind string

const (
	ManifestKindDbt ManifestKind = "dbt"
	// ManifestKindPython is the kind of a contract.yaml service. Only a stored
	// run or service_prod row holds it: ParseManifestKind refuses it, so no new
	// release or verification carries it, and a stored run of it is rejected
	// before it can reach production.
	ManifestKindPython     ManifestKind = "python"
	ManifestKindPythonODCS ManifestKind = "python-odcs"
	// ManifestKindOntology is the kind of the install's ontology service: its
	// artifact is ontology.yaml, it has no node, and it has no compile leg.
	ManifestKindOntology ManifestKind = "ontology"
)

// ErrPythonKindNotAccepted is the refusal of the kind python: the error
// ParseManifestKind returns for it, and the detail a stored run of that kind
// ends with.
var ErrPythonKindNotAccepted = fmt.Errorf(
	"kind %q is no longer accepted: release the service as %q, one ODCS contract per node (docs/contracts.md)",
	ManifestKindPython, ManifestKindPythonODCS)

// IsPython reports whether the kind is one of the two python kinds: a python
// service has no source overlay, whichever artifact it ships.
func (k ManifestKind) IsPython() bool {
	return k == ManifestKindPython || k == ManifestKindPythonODCS
}

// HasCompileLeg reports whether a release of the kind is compiled before it is
// parsed. Only a dbt service is: every other kind ships its artifact before the
// release is requested, so its release goes straight to parsing.
func (k ManifestKind) HasCompileLeg() bool {
	return k == ManifestKindDbt
}

// ParseManifestKind validates a raw kind string from a new release or
// verification. Empty is rejected: the absent-means-dbt defaulting is an
// API-boundary concern; domain objects never hold an empty kind. The python
// kind is refused with the kind to use instead. A stored row is rehydrated
// from its column and never parsed, so a row that holds the python kind still
// loads.
func ParseManifestKind(s string) (ManifestKind, error) {
	switch ManifestKind(s) {
	case ManifestKindDbt, ManifestKindPythonODCS, ManifestKindOntology:
		return ManifestKind(s), nil
	case ManifestKindPython:
		return "", ErrPythonKindNotAccepted
	default:
		return "", fmt.Errorf("unknown manifest kind %q (expected %q, %q or %q)", s, ManifestKindDbt, ManifestKindPythonODCS, ManifestKindOntology)
	}
}
