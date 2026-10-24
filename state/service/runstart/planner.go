// Package runstart chooses the tasks of a new run from the release the run
// reads and starts the run, inside the transaction that creates it. Each kind
// of run has a selection method that reads the slice of the release its
// runplan selector needs; Start then stores the run's tasks and writes the
// dispatch of each ready one to the outbox, so the run is created, planned and
// dispatched in one commit.
package runstart

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ErrSourceHasNoRelease is returned when a rerun, a rebase or a single-node
// run of a source run's snapshot names a source run that names no release, or
// one of whose tasks names no node: its tasks cannot be read in any release.
var ErrSourceHasNoRelease = errors.New("runstart: the source run names no release")

// Selection is what a new run starts with: the release it runs on, the slice
// of that release its tasks were chosen from, and the plan chosen.
type Selection struct {
	Stamp run.ReleaseStamp
	Graph topology.Graph
	Plan  runplan.Plan
}

// Planner chooses and starts the runs of one tenant. Every task it plans gets
// the retry budget of Tasks, and each dispatch the timeout of its node, else
// that of Tasks.
type Planner struct {
	TenantID string
	Tasks    run.TaskDefaults
}

// LiveStamp takes the topology lock in u's transaction and reads the tenant's
// live release, the zero stamp when none is live. A promotion moves the live
// release and the retention pruner deletes releases no committed run names,
// both under that lock, so the release a new run names cannot be pruned before
// the run commits.
func (p Planner) LiveStamp(ctx context.Context, u uow.UnitOfWork) (run.ReleaseStamp, error) {
	if err := u.Topology().LockTopology(ctx); err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("lock the topology for the live release read: %w", err)
	}
	live, err := u.Topology().LivePointer(ctx, p.TenantID)
	if err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("read the live release: %w", err)
	}
	return run.ReleaseStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}, nil
}

// Schedule selects a cron or triggered run of scheduleName with operation op
// on the live release. The selection carries the live release even when the
// selector refuses it.
func (p Planner) Schedule(ctx context.Context, u uow.UnitOfWork, scheduleName string, op model.Operation) (Selection, error) {
	sel, err := p.liveScheduleGraph(ctx, u, scheduleName)
	if err != nil {
		return sel, err
	}
	sel.Plan, err = runplan.WholeDAG{Operation: op}.Select(sel.Graph)
	return sel, err
}

// Rerun selects a rerun of source on source's own release.
func (p Planner) Rerun(ctx context.Context, u uow.UnitOfWork, source *run.Run) (Selection, error) {
	src, keys, err := p.source(ctx, u, source)
	if err != nil {
		return Selection{}, err
	}
	g, err := u.Topology().Graph(ctx, p.TenantID, source.ReleaseID(), keys)
	if err != nil {
		return Selection{}, fmt.Errorf("read the nodes of run %s in release %s: %w", source.ScheduleID(), source.ReleaseID(), err)
	}
	plan, err := runplan.Rerun{Source: src}.Select(g)
	return Selection{Stamp: source.Release(), Graph: g, Plan: plan}, err
}

// SingleNode selects a run of the node target names, with operation op: on
// source's release when source is not nil, else on the live release.
func (p Planner) SingleNode(ctx context.Context, u uow.UnitOfWork, target run.NodeID, op model.Operation, source *run.Run) (Selection, error) {
	var stamp run.ReleaseStamp
	if source != nil {
		if source.ReleaseID() == "" {
			return Selection{}, fmt.Errorf("%w: run %s", ErrSourceHasNoRelease, source.ScheduleID())
		}
		stamp = source.Release()
	} else {
		live, err := p.LiveStamp(ctx, u)
		if err != nil {
			return Selection{}, err
		}
		stamp = live
	}
	key := topology.KeyOf(target.SchemaName, target.TableName)
	selector := runplan.SingleNode{Key: key, ServiceName: target.ServiceName, Operation: op}
	if stamp.ReleaseID == "" {
		return Selection{Stamp: stamp}, fmt.Errorf("%w: no release is live", runplan.ErrTargetNotFound)
	}
	g, err := u.Topology().Graph(ctx, p.TenantID, stamp.ReleaseID, []topology.NodeKey{key})
	if err != nil {
		return Selection{}, fmt.Errorf("read node %s in release %s: %w", key, stamp.ReleaseID, err)
	}
	plan, err := selector.Select(g)
	return Selection{Stamp: stamp, Graph: g, Plan: plan}, err
}

// Seeds selects a promote-seed run of keys on live, the release the caller
// read under the topology lock it holds.
func (p Planner) Seeds(ctx context.Context, u uow.UnitOfWork, live topology.Pointer, keys []topology.NodeKey) (Selection, error) {
	stamp := run.ReleaseStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}
	g, err := u.Topology().Graph(ctx, p.TenantID, live.ReleaseID, keys)
	if err != nil {
		return Selection{}, fmt.Errorf("read %d seeds in release %s: %w", len(keys), live.ReleaseID, err)
	}
	plan, err := runplan.Seeds{Keys: keys}.Select(g)
	return Selection{Stamp: stamp, Graph: g, Plan: plan}, err
}

// Start stores sel's tasks on r, which u's transaction has already saved,
// saves r again and appends r's events to the outbox under msgProcID.
func (p Planner) Start(ctx context.Context, u uow.UnitOfWork, r *run.Run, sel Selection, msgProcID uuid.UUID) error {
	events, err := r.Start(ctx, u.TaskCollection(p.TenantID), p.TenantID, sel.Plan, sel.Graph, p.Tasks, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("start run %s: %w", r.ScheduleID(), err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save started run %s: %w", r.ScheduleID(), err)
	}
	if err := u.Outbox().Append(ctx, events, msgProcID); err != nil {
		return fmt.Errorf("append the events of run %s: %w", r.ScheduleID(), err)
	}
	return nil
}

// liveScheduleGraph reads the graph of scheduleName on the live release.
// Without a live release the selection is empty.
func (p Planner) liveScheduleGraph(ctx context.Context, u uow.UnitOfWork, scheduleName string) (Selection, error) {
	stamp, err := p.LiveStamp(ctx, u)
	if err != nil {
		return Selection{}, err
	}
	return p.scheduleGraph(ctx, u, stamp, scheduleName)
}

// scheduleGraph reads the graph of scheduleName on the live release stamp
// names, which the caller read under the topology lock it holds. Without a
// live release the selection is empty.
func (p Planner) scheduleGraph(ctx context.Context, u uow.UnitOfWork, stamp run.ReleaseStamp, scheduleName string) (Selection, error) {
	sel := Selection{Stamp: stamp}
	if stamp.ReleaseID == "" {
		return sel, fmt.Errorf("%w: no release is live", runplan.ErrEmptySelection)
	}
	g, err := u.Topology().ScheduleGraph(ctx, p.TenantID, stamp.ReleaseID, scheduleName)
	if err != nil {
		return sel, fmt.Errorf("read the graph of schedule %s in release %s: %w", scheduleName, stamp.ReleaseID, err)
	}
	sel.Graph = g
	return sel, nil
}

// source reads source as the runplan source of a derived run, with the keys
// of its tasks. Every task is marked built on source's release; a rebase
// replaces that, for an inherited task, with the release of the run that ran
// the node.
func (p Planner) source(ctx context.Context, u uow.UnitOfWork, source *run.Run) (runplan.Source, []topology.NodeKey, error) {
	if source.ReleaseID() == "" {
		return runplan.Source{}, nil, fmt.Errorf("%w: run %s", ErrSourceHasNoRelease, source.ScheduleID())
	}
	tasks, err := u.TaskCollection(p.TenantID).ListRunTasks(ctx, source.ScheduleID())
	if err != nil {
		return runplan.Source{}, nil, fmt.Errorf("read the tasks of run %s: %w", source.ScheduleID(), err)
	}
	src := runplan.Source{Operation: source.Operation(), Tasks: make([]runplan.SourceTask, 0, len(tasks))}
	keys := make([]topology.NodeKey, 0, len(tasks))
	for _, t := range tasks {
		if t.NodeKey == "" {
			return runplan.Source{}, nil, fmt.Errorf("%w: task %s of run %s names no node", ErrSourceHasNoRelease, t.TaskID, source.ScheduleID())
		}
		st := runplan.SourceTask{Key: t.NodeKey, TaskID: t.TaskID, Succeeded: t.Status == run.TaskStatusSucceeded, BuiltOn: source.ReleaseID()}
		if t.InheritedFromTaskID != nil {
			st.InheritedFrom = *t.InheritedFromTaskID
		}
		src.Tasks = append(src.Tasks, st)
		keys = append(keys, t.NodeKey)
	}
	return src, keys, nil
}
