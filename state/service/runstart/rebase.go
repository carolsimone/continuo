package runstart

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/service/uow"
)

// ErrLiveReleaseMoved is returned when a rebase names the live release its
// caller previewed and another release is live: the selection the caller
// confirmed is not the one the rebase would make.
var ErrLiveReleaseMoved = errors.New("the live release changed; preview again")

// RebaseOptions are a rebase's choices. IncludeChangedUpstream also executes
// the changed nodes upstream of what the rebase executes. A non-empty
// ExpectedReleaseID must name the live release.
type RebaseOptions struct {
	IncludeChangedUpstream bool
	ExpectedReleaseID      string
}

// Rebase selects a rebase of source onto the live release, over the graph of
// source's schedule there, compared with source's nodes on source's own
// release, and returns the selection with its preview. The live release is
// read first, under the topology lock, and a live release other than
// opts.ExpectedReleaseID is ErrLiveReleaseMoved before anything else is read.
// The selection carries the live release even when it is refused.
func (p Planner) Rebase(ctx context.Context, u uow.UnitOfWork, source *run.Run, opts RebaseOptions) (Selection, runplan.Preview, error) {
	stamp, err := p.LiveStamp(ctx, u)
	if err != nil {
		return Selection{}, runplan.Preview{}, err
	}
	if opts.ExpectedReleaseID != "" && opts.ExpectedReleaseID != stamp.ReleaseID {
		return Selection{Stamp: stamp}, runplan.Preview{}, ErrLiveReleaseMoved
	}
	src, keys, err := p.source(ctx, u, source)
	if err != nil {
		return Selection{Stamp: stamp}, runplan.Preview{}, err
	}
	sourceGraph, err := u.Topology().Graph(ctx, p.TenantID, source.ReleaseID(), keys)
	if err != nil {
		return Selection{Stamp: stamp}, runplan.Preview{}, fmt.Errorf("read the nodes of run %s in release %s: %w", source.ScheduleID(), source.ReleaseID(), err)
	}
	sel, err := p.scheduleGraph(ctx, u, stamp, source.ScheduleName())
	if err != nil {
		return sel, runplan.Preview{}, err
	}
	pv, err := runplan.Rebase{
		Source: src, SourceGraph: sourceGraph, IncludeChangedUpstream: opts.IncludeChangedUpstream,
	}.Preview(sel.Graph)
	sel.Plan = pv.Plan
	return sel, pv, err
}
