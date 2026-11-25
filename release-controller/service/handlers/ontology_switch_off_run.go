package handlers

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// isOntologyRunWhileSwitchOff reports whether r is an unfinished run of the
// kind ontology met while the install's ontology switch is off. Intake accepts
// the kind only while the switch is on, so such a run was accepted before the
// switch was turned off. While it is off topology-controller ignores the
// ontology file without reading it and answers the parse ok, so promoting the
// run would store a file nobody loaded or checked as production's ontology.
func isOntologyRunWhileSwitchOff(d *Deps, r *pipeline.Run) bool {
	return r.ManifestKind() == release.ManifestKindOntology && !r.Status().IsTerminal() && !d.OntologyEnabled
}

// rejectOntologyRunWhileSwitchOff ends an ontology run met with the switch off
// before any leg acts on it, with the refusal intake gives for the kind while
// the switch is off (ErrOntologyDisabled) as its detail
// (rejectInvalidArtifactRun).
func rejectOntologyRunWhileSwitchOff(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	d.Logger.Warn("rejecting a run of the kind ontology while the ontology switch is off",
		"release_id", r.ID(), "project", r.ChangedProject(), "status", string(r.Status()))
	return rejectInvalidArtifactRun(ctx, d, u, r, ErrOntologyDisabled.Error(), now)
}
