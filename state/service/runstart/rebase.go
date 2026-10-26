package runstart

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
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
// source's schedule there, and returns the selection with its preview. Each
// of source's tasks is compared with its node on the release its result was
// built on: source's own release for a task source ran itself, the release of
// the run that ran the node for an inherited one. The live release is read
// first, under the topology lock, and a live release other than
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
	if err := builtOnRootReleases(ctx, u.TaskCollection(p.TenantID), src); err != nil {
		return Selection{Stamp: stamp}, runplan.Preview{}, fmt.Errorf("read the releases that built the results run %s inherited: %w", source.ScheduleID(), err)
	}
	sourceGraphs, err := p.sourceGraphs(ctx, u, src, keys)
	if err != nil {
		return Selection{Stamp: stamp}, runplan.Preview{}, fmt.Errorf("read the nodes of run %s: %w", source.ScheduleID(), err)
	}
	sel, err := p.scheduleGraph(ctx, u, stamp, source.ScheduleName())
	if err != nil {
		return sel, runplan.Preview{}, err
	}
	pv, err := runplan.Rebase{
		Source: src, SourceGraphs: sourceGraphs, IncludeChangedUpstream: opts.IncludeChangedUpstream,
	}.Preview(sel.Graph)
	sel.Plan = pv.Plan
	return sel, pv, err
}

// builtOnRootReleases sets the BuiltOn of each inherited task of src to the
// release of the run its InheritedFrom task belongs to, read through tasks. A
// task whose InheritedFrom task is gone, or whose run names no release, keeps
// the source run's release: nothing records the release that built it, and
// the source run's is the nearest one that held its result.
func builtOnRootReleases(ctx context.Context, tasks run.TaskCollection, src runplan.Source) error {
	var roots []uuid.UUID
	for _, t := range src.Tasks {
		if t.InheritedFrom != uuid.Nil {
			roots = append(roots, t.InheritedFrom)
		}
	}
	if len(roots) == 0 {
		return nil
	}
	releases, err := tasks.RunReleasesOf(ctx, roots)
	if err != nil {
		return err
	}
	for i, t := range src.Tasks {
		if t.InheritedFrom == uuid.Nil {
			continue
		}
		if r, ok := releases[t.InheritedFrom]; ok {
			src.Tasks[i].BuiltOn = r
		}
	}
	return nil
}

// sourceGraphs reads each release a task of src was built on, restricted to
// keys, once.
func (p Planner) sourceGraphs(ctx context.Context, u uow.UnitOfWork, src runplan.Source, keys []topology.NodeKey) (map[string]topology.Graph, error) {
	graphs := make(map[string]topology.Graph)
	for _, t := range src.Tasks {
		if _, ok := graphs[t.BuiltOn]; ok {
			continue
		}
		g, err := u.Topology().Graph(ctx, p.TenantID, t.BuiltOn, keys)
		if err != nil {
			return nil, fmt.Errorf("in release %s: %w", t.BuiltOn, err)
		}
		graphs[t.BuiltOn] = g
	}
	return graphs, nil
}
