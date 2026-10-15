package runreads

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
)

// RebasePreview is what a rebase of a source run would do on the live
// release: the release, the selection with its counts, and the changed
// upstream nodes the selection names, in its order (service, schema, table),
// each with how long its latest successful execution took.
type RebasePreview struct {
	Release run.ReleaseStamp
	Preview runplan.Preview
	Changed []ChangedUpstream
}

// ChangedUpstream is a changed node upstream of what a rebase executes, with
// how long its latest successful execution took; LastSuccess is 0 when the
// node has no successful execution.
type ChangedUpstream struct {
	runplan.ChangedNode
	LastSuccess time.Duration
}

// RebasePreviewer previews rebases with the selection a rebase makes, in a
// transaction it rolls back.
type RebasePreviewer struct {
	planner runstart.Planner
	newUoW  func() uow.UnitOfWork
	reads   repository.RunReadRepository
}

// NewRebasePreviewer returns a RebasePreviewer that selects with planner in a
// unit of work from newUoW and reads durations from reads.
func NewRebasePreviewer(planner runstart.Planner, newUoW func() uow.UnitOfWork, reads repository.RunReadRepository) *RebasePreviewer {
	return &RebasePreviewer{planner: planner, newUoW: newUoW, reads: reads}
}

// Preview returns what a rebase of sourceID with includeChangedUpstream would
// do on the live release. The source must be one a rebase accepts; a schedule
// with an active run is no reason to refuse a preview. The selection is made
// under the topology lock, in a transaction that is rolled back, so a preview
// writes nothing.
func (p *RebasePreviewer) Preview(ctx context.Context, sourceID uuid.UUID, includeChangedUpstream bool) (RebasePreview, error) {
	u := p.newUoW()
	src, err := u.Run().GetRun(ctx, sourceID)
	if err != nil {
		return RebasePreview{}, fmt.Errorf("get source: %w", err)
	}
	if err := src.CanBeRebaseSource(ctx, u.TaskCollection()); err != nil {
		return RebasePreview{}, err
	}

	sel, pv, err := p.selectRebase(ctx, u, src, includeChangedUpstream)
	if err != nil {
		return RebasePreview{}, err
	}
	nodes := make([]run.NodeID, 0, len(pv.Changed))
	for _, c := range pv.Changed {
		nodes = append(nodes, run.NodeID{ServiceName: c.ServiceName, SchemaName: c.SchemaName, TableName: c.TableName})
	}
	last, err := p.reads.LastSuccessDurations(ctx, nodes)
	if err != nil {
		return RebasePreview{}, fmt.Errorf("read the last successful durations: %w", err)
	}
	changed := make([]ChangedUpstream, len(pv.Changed))
	for i, c := range pv.Changed {
		changed[i] = ChangedUpstream{ChangedNode: c, LastSuccess: last[nodes[i]]}
	}
	return RebasePreview{Release: sel.Stamp, Preview: pv, Changed: changed}, nil
}

// selectRebase makes the rebase's selection in a transaction of u that it
// rolls back before returning.
func (p *RebasePreviewer) selectRebase(ctx context.Context, u uow.UnitOfWork, src *run.Run, includeChangedUpstream bool) (runstart.Selection, runplan.Preview, error) {
	if err := u.Begin(ctx); err != nil {
		return runstart.Selection{}, runplan.Preview{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	return p.planner.Rebase(ctx, u, src, runstart.RebaseOptions{IncludeChangedUpstream: includeChangedUpstream})
}
