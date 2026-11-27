package release

import "fmt"

// ManifestKind discriminates how a project's release artifact is authored and
// parsed: a dbt manifest.json, a Python project's odcs-bundle.json (one ODCS
// contract per node), or the install's ontology.yaml (the company ontology,
// which declares no node). It travels POST /releases → Release →
// ProductionPointer → ManifestKey → the per-entry kind on
// release.requested:v1. release-controller only moves (project, pointer, kind)
// triples; topology-controller is the sole component that parses
// kind-specifically. The kinds ParseManifestKind accepts must match
// topology-controller's ManifestKind enum (domain/model.py).
type ManifestKind string

const (
	ManifestKindDbt ManifestKind = "dbt"
	// ManifestKindPython is the kind of a Python project: its artifact is the
	// odcs-bundle.json its CI packs, one ODCS contract per node.
	ManifestKindPython ManifestKind = "python"
	// ManifestKindOntology is the kind of the install's ontology project: its
	// artifact is ontology.yaml, it has no node, and it has no compile leg.
	ManifestKindOntology ManifestKind = "ontology"
)

// HasCompileLeg reports whether a release of the kind is compiled before it is
// parsed. Only a dbt project is: every other kind ships its artifact before the
// release is requested, so its release goes straight to parsing.
func (k ManifestKind) HasCompileLeg() bool {
	return k == ManifestKindDbt
}

// ParseManifestKind validates a raw kind string from a new release or a
// fix's release check. Empty is rejected: the absent-means-dbt defaulting is an
// API-boundary concern; domain objects never hold an empty kind.
func ParseManifestKind(s string) (ManifestKind, error) {
	switch ManifestKind(s) {
	case ManifestKindDbt, ManifestKindPython, ManifestKindOntology:
		return ManifestKind(s), nil
	default:
		return "", fmt.Errorf("unknown manifest kind %q (expected %q, %q or %q)", s, ManifestKindDbt, ManifestKindPython, ManifestKindOntology)
	}
}
