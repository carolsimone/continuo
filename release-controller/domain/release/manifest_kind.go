package release

import "fmt"

// ManifestKind discriminates how a service's release artifact is authored and
// parsed: a dbt manifest.json, or a python service's odcs-bundle.json, one ODCS
// contract per node. It travels POST /releases → Release →
// service_prod.manifest_kind → ManifestKey → the per-entry kind on
// release.requested:v1. release-controller only moves (service, pointer, kind)
// triples; topology-controller is the sole component that parses
// kind-specifically. Values must match topology-controller's ManifestKind enum
// (domain/model.py), except ManifestKindPython, which only a stored row holds.
type ManifestKind string

const (
	ManifestKindDbt ManifestKind = "dbt"
	// ManifestKindPython is the kind of a contract.yaml service. Only a stored
	// run or service_prod row holds it: ParseManifestKind refuses it, so no new
	// release or verification carries it, and a stored run of it is rejected
	// before it can reach production.
	ManifestKindPython     ManifestKind = "python"
	ManifestKindPythonODCS ManifestKind = "python-odcs"
)

// ErrPythonKindNotAccepted is the refusal of the kind python: the error
// ParseManifestKind returns for it, and the detail a stored run of that kind
// ends with.
var ErrPythonKindNotAccepted = fmt.Errorf(
	"kind %q is no longer accepted: release the service as %q, one ODCS contract per node (docs/contracts.md)",
	ManifestKindPython, ManifestKindPythonODCS)

// IsPython reports whether the kind is one of the two python kinds: a python
// service has no compile leg and no source overlay, whichever artifact it ships.
func (k ManifestKind) IsPython() bool {
	return k == ManifestKindPython || k == ManifestKindPythonODCS
}

// ParseManifestKind validates a raw kind string from a new release or
// verification. Empty is rejected: the absent-means-dbt defaulting is an
// API-boundary concern; domain objects never hold an empty kind. The python
// kind is refused with the kind to use instead. A stored row is rehydrated
// from its column and never parsed, so a row that holds the python kind still
// loads.
func ParseManifestKind(s string) (ManifestKind, error) {
	switch ManifestKind(s) {
	case ManifestKindDbt, ManifestKindPythonODCS:
		return ManifestKind(s), nil
	case ManifestKindPython:
		return "", ErrPythonKindNotAccepted
	default:
		return "", fmt.Errorf("unknown manifest kind %q (expected %q or %q)", s, ManifestKindDbt, ManifestKindPythonODCS)
	}
}
