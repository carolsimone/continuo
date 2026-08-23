package release

import "fmt"

// ManifestKind discriminates how a service's release artifact is authored and
// parsed: a dbt manifest.json or a Continuo python contract.yaml. It travels
// POST /releases → Release → service_prod.manifest_kind → ManifestKey → the
// per-entry kind on release.requested:v1. release-controller only moves
// (service, pointer, kind) triples; topology-controller is the sole component
// that parses kind-specifically. Values must match topology-controller's
// ManifestKind enum (domain/model.py).
type ManifestKind string

const (
	ManifestKindDbt    ManifestKind = "dbt"
	ManifestKindPython ManifestKind = "python"
)

// ParseManifestKind validates a raw kind string. Empty is rejected: the
// absent-means-dbt defaulting is an API-boundary concern; domain objects never
// hold an empty kind.
func ParseManifestKind(s string) (ManifestKind, error) {
	switch ManifestKind(s) {
	case ManifestKindDbt, ManifestKindPython:
		return ManifestKind(s), nil
	default:
		return "", fmt.Errorf("unknown manifest kind %q (expected %q or %q)", s, ManifestKindDbt, ManifestKindPython)
	}
}
