package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
)

// ActiveRun describes the run currently holding the pipeline.
type ActiveRun struct {
	RunID             string
	Kind              pipeline.Kind
	Status            pipeline.Status
	Service           string
	Since             time.Time
	VerifiesReleaseID string // verification only
	Attempt           int    // verification only
}

// Pipeline is what the pipeline is doing right now: the active run of
// either kind, or nothing.
type Pipeline struct {
	Active *ActiveRun
}

// GetPipeline reads the active run. It is the one read that spans both
// kinds, so an operator can see a verification holding the slot that
// candidate releases are queued behind.
func GetPipeline(ctx context.Context, d *Deps) (Pipeline, error) {
	u := d.NewUoW()
	r, err := u.RunRepo().Active(ctx)
	if err != nil {
		return Pipeline{}, fmt.Errorf("active run: %w", err)
	}
	if r == nil {
		return Pipeline{}, nil
	}
	since, _ := r.ActivatedAt()
	return Pipeline{Active: &ActiveRun{
		RunID: r.ID(), Kind: r.Kind(), Status: r.Status(), Service: r.ChangedService(), Since: since,
		VerifiesReleaseID: r.VerifiesReleaseID(), Attempt: r.Attempt(),
	}}, nil
}
