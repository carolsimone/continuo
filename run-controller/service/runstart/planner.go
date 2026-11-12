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
	"log/slog"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// ErrSourceHasNoRelease is returned when a rerun, a rebase or a single-node
// run of a source run's snapshot names a source run that names no release, or
// one of whose tasks names no node: its tasks cannot be read in any release.
var ErrSourceHasNoRelease = errors.New("runstart: the source run names no release")

// ErrNoWarehouseFamily is returned when a run would load a seed and the
// planner has no warehouse family: what a seed's load drops, and so what the
// run rebuilds, depends on it.
var ErrNoWarehouseFamily = errors.New("runstart: the planner has no warehouse family")

// Selection is what a new run starts with: the release it runs on, the slice
// of that release its tasks were chosen from, and the plan chosen.
type Selection struct {
	Stamp run.ReleaseStamp
	Graph topology.Graph
	Plan  runplan.Plan
}

// Planner chooses and starts runs. A selection reads the releases of the
// tenant it is given, or of the source run it derives from, and Start stores a
// run's tasks under the run's own tenant. Every task it plans gets the retry
// budget of Tasks, and each dispatch the timeout of its node, else that of
// Tasks.
//
// Every selection that loads a seed, in a run whose operation is not test,
// applies the seed reload rules of Family, the install's warehouse family, and
// Engine, its WAREHOUSE_ENGINE: a load that drops the seed's table rebuilds
// the views below it in the same run, and a load no command can apply is
// refused. Start counts each refused task on Metrics and logs it on Logger
// once the run's transaction commits; a nil Metrics or Logger skips that part.
type Planner struct {
	Tasks   run.TaskDefaults
	Family  warehouse.Family
	Engine  string
	Metrics ports.SeedReloadMetrics
	Logger  *slog.Logger
}

// LiveStamp takes the topology lock in u's transaction and reads tenantID's
// live release, the zero stamp when none is live. A promotion moves the live
// release and the retention pruner deletes releases no committed run names,
// both under that lock, so the release a new run names cannot be pruned before
// the run commits.
func (p Planner) LiveStamp(ctx context.Context, u uow.UnitOfWork, tenantID string) (run.ReleaseStamp, error) {
	if err := u.Topology().LockTopology(ctx); err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("lock the topology for the live release read: %w", err)
	}
	live, err := u.Topology().LivePointer(ctx, tenantID)
	if err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("read the live release: %w", err)
	}
	return run.ReleaseStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}, nil
}

// Schedule selects a cron or triggered run of tenantID's scheduleName with
// operation op on the tenant's live release. The selection carries the live
// release even when the selector refuses it.
func (p Planner) Schedule(ctx context.Context, u uow.UnitOfWork, tenantID, scheduleName string, op model.Operation) (Selection, error) {
	sel, err := p.liveScheduleGraph(ctx, u, tenantID, scheduleName)
	if err != nil {
		return sel, err
	}
	sel.Plan, err = runplan.WholeDAG{Operation: op}.Select(sel.Graph)
	if err != nil {
		return sel, err
	}
	return p.withSeedReloads(ctx, u, tenantID, op, seedreload.TriggerLoad, nil, sel)
}

// Rerun selects a rerun of source on source's own release, of source's
// tenant.
func (p Planner) Rerun(ctx context.Context, u uow.UnitOfWork, source *run.Run) (Selection, error) {
	src, keys, err := p.source(ctx, u, source)
	if err != nil {
		return Selection{}, err
	}
	g, err := u.Topology().Graph(ctx, source.TenantID(), source.ReleaseID(), keys)
	if err != nil {
		return Selection{}, fmt.Errorf("read the nodes of run %s in release %s: %w", source.RunID(), source.ReleaseID(), err)
	}
	plan, err := runplan.Rerun{Source: src}.Select(g)
	sel := Selection{Stamp: source.Release(), Graph: g, Plan: plan}
	if err != nil {
		return sel, err
	}
	return p.withSeedReloads(ctx, u, source.TenantID(), source.Operation(), seedreload.TriggerLoad, nil, sel)
}

// SingleNode selects a run of tenantID's node target names, with operation
// op: on source's release when source is not nil, else on the tenant's live
// release. A source is a run of tenantID. A full refresh of a seed is an
// operator's request for it; acknowledged are the objects reading the seed
// that an operator named, which its preflight lets the load drop.
func (p Planner) SingleNode(ctx context.Context, u uow.UnitOfWork, tenantID string, target run.NodeID, op model.Operation, source *run.Run, acknowledged []string) (Selection, error) {
	var stamp run.ReleaseStamp
	if source != nil {
		if source.ReleaseID() == "" {
			return Selection{}, fmt.Errorf("%w: run %s", ErrSourceHasNoRelease, source.RunID())
		}
		stamp = source.Release()
	} else {
		live, err := p.LiveStamp(ctx, u, tenantID)
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
	g, err := u.Topology().Graph(ctx, tenantID, stamp.ReleaseID, []topology.NodeKey{key})
	if err != nil {
		return Selection{}, fmt.Errorf("read node %s in release %s: %w", key, stamp.ReleaseID, err)
	}
	plan, err := selector.Select(g)
	sel := Selection{Stamp: stamp, Graph: g, Plan: plan}
	if err != nil {
		return sel, err
	}
	trigger := seedreload.TriggerLoad
	if op == model.OperationFullRefresh {
		trigger = seedreload.TriggerFullRefresh
	}
	return p.withSeedReloads(ctx, u, tenantID, op, trigger, acknowledged, sel)
}

// Seeds selects a promote-seed run of keys on live, tenantID's live release,
// which the caller read under the topology lock it holds.
func (p Planner) Seeds(ctx context.Context, u uow.UnitOfWork, tenantID string, live topology.Pointer, keys []topology.NodeKey) (Selection, error) {
	stamp := run.ReleaseStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}
	g, err := u.Topology().Graph(ctx, tenantID, live.ReleaseID, keys)
	if err != nil {
		return Selection{}, fmt.Errorf("read %d seeds in release %s: %w", len(keys), live.ReleaseID, err)
	}
	plan, err := runplan.Seeds{Keys: keys}.Select(g)
	sel := Selection{Stamp: stamp, Graph: g, Plan: plan}
	if err != nil {
		return sel, err
	}
	return p.withSeedReloads(ctx, u, tenantID, model.OperationRun, seedreload.TriggerLoad, nil, sel)
}

// Start stores sel's tasks on r, under r's tenant, in the run u's transaction
// has already saved, saves r again and appends r's events to the outbox under
// msgProcID. Each task the plan refused is counted and logged once u's
// transaction commits.
func (p Planner) Start(ctx context.Context, u uow.UnitOfWork, r *run.Run, sel Selection, msgProcID uuid.UUID) error {
	events, err := r.Start(ctx, u.TaskCollection(r.TenantID()), sel.Plan, sel.Graph, p.Tasks, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("start run %s: %w", r.RunID(), err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save started run %s: %w", r.RunID(), err)
	}
	if err := u.Outbox().Append(ctx, events, msgProcID); err != nil {
		return fmt.Errorf("append the events of run %s: %w", r.RunID(), err)
	}
	for _, e := range events {
		if refused, ok := e.(run.TaskRefused); ok {
			u.AfterCommit(func() { p.reportRefusal(r.ReleaseID(), refused) })
		}
	}
	return nil
}

// reportRefusal counts e, a task the plan refused, as a seed load that ended
// refused, with the reason that stopped it, and logs it at WARN.
func (p Planner) reportRefusal(releaseID string, e run.TaskRefused) {
	if p.Metrics != nil {
		action := "load"
		if e.Operation == model.OperationFullRefresh {
			action = "full_refresh"
		}
		p.Metrics.SeedReloadEnded(action, "refused")
		p.Metrics.SeedReloadStopped(string(e.Reason))
	}
	if p.Logger != nil {
		p.Logger.Warn("seed load refused: the task never starts",
			"tenant_id", e.TenantID, "release_id", releaseID, "run_id", e.ID, "node_key", e.Key, "reason", e.Reason)
	}
}

// withSeedReloads applies the seed reload rules to sel, a selection of a run
// of tenantID with operation op, whose seed loads trigger asks for, and
// returns it. A test run loads nothing, and a selection without a seed task to
// run is returned as it is.
//
// Otherwise each seed task is planned from the column fingerprint its table
// last loaded, the one its release carries and whether that release skipped
// validation. The selection's graph is read again over its nodes and the views
// below its seeds, so a node of the selection that reads a rebuilt view waits
// for the rebuild, and every rebuild is a task of the plan with its edges.
func (p Planner) withSeedReloads(ctx context.Context, u uow.UnitOfWork, tenantID string, op model.Operation,
	trigger seedreload.Trigger, acknowledged []string, sel Selection) (Selection, error) {
	if op == model.OperationTest {
		return sel, nil
	}
	var seeds []topology.NodeKey
	for _, t := range sel.Plan.Tasks {
		if n, ok := sel.Graph.Node(t.Key); ok && !t.Inherited && n.NodeType == string(model.NodeTypeDbtSeed) {
			seeds = append(seeds, t.Key)
		}
	}
	if len(seeds) == 0 {
		return sel, nil
	}
	if !p.Family.IsValid() {
		return sel, ErrNoWarehouseFamily
	}
	release := sel.Stamp.ReleaseID
	recorded, err := u.Materializations().LoadedSeedColumns(ctx, tenantID, seeds)
	if err != nil {
		return sel, fmt.Errorf("read the loaded columns of %d seeds: %w", len(seeds), err)
	}
	bootstrap, err := u.Topology().IsBootstrapRelease(ctx, tenantID, release)
	if err != nil {
		return sel, fmt.Errorf("read whether release %s skipped validation: %w", release, err)
	}
	closure, err := u.Topology().ViewClosure(ctx, tenantID, release, seeds)
	if err != nil {
		return sel, fmt.Errorf("read the views below %d seeds in release %s: %w", len(seeds), release, err)
	}
	g, err := u.Topology().Graph(ctx, tenantID, release, append(sel.Graph.Keys(), closure.Keys()...))
	if err != nil {
		return sel, fmt.Errorf("read the selection and the views below its seeds in release %s: %w", release, err)
	}

	reloads := make([]runplan.SeedReload, 0, len(seeds))
	for _, k := range seeds {
		n, ok := g.Node(k)
		if !ok {
			return sel, fmt.Errorf("seed %s is not in release %s", k, release)
		}
		plan := seedreload.PlanSeed(seedreload.Request{
			Seed:        runplan.SeedNode(n),
			Family:      p.Family,
			Engine:      p.Engine,
			Change:      seedreload.ChangeOf(recorded[k], n.SeedColumnsHash),
			Trigger:     trigger,
			Unvalidated: bootstrap,
		}, runplan.SeedReloadGraph(g))
		reloads = append(reloads, runplan.SeedReload{Seed: k, Plan: plan, Preflight: plan.Preflight(acknowledged)})
	}
	sel.Plan = runplan.ApplySeedReloads(sel.Plan, g, op, reloads)
	sel.Graph = g
	return sel, nil
}

// liveScheduleGraph reads the graph of tenantID's scheduleName on the
// tenant's live release. Without a live release the selection is empty.
func (p Planner) liveScheduleGraph(ctx context.Context, u uow.UnitOfWork, tenantID, scheduleName string) (Selection, error) {
	stamp, err := p.LiveStamp(ctx, u, tenantID)
	if err != nil {
		return Selection{}, err
	}
	return p.scheduleGraph(ctx, u, tenantID, stamp, scheduleName)
}

// scheduleGraph reads the graph of tenantID's scheduleName on the live
// release stamp names, which the caller read under the topology lock it
// holds. Without a live release the selection is empty.
func (p Planner) scheduleGraph(ctx context.Context, u uow.UnitOfWork, tenantID string, stamp run.ReleaseStamp, scheduleName string) (Selection, error) {
	sel := Selection{Stamp: stamp}
	if stamp.ReleaseID == "" {
		return sel, fmt.Errorf("%w: no release is live", runplan.ErrEmptySelection)
	}
	g, err := u.Topology().ScheduleGraph(ctx, tenantID, stamp.ReleaseID, scheduleName)
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
		return runplan.Source{}, nil, fmt.Errorf("%w: run %s", ErrSourceHasNoRelease, source.RunID())
	}
	tasks, err := u.TaskCollection(source.TenantID()).ListRunTasks(ctx, source.RunID())
	if err != nil {
		return runplan.Source{}, nil, fmt.Errorf("read the tasks of run %s: %w", source.RunID(), err)
	}
	src := runplan.Source{Operation: source.Operation(), Tasks: make([]runplan.SourceTask, 0, len(tasks))}
	keys := make([]topology.NodeKey, 0, len(tasks))
	for _, t := range tasks {
		if t.NodeKey == "" {
			return runplan.Source{}, nil, fmt.Errorf("%w: task %s of run %s names no node", ErrSourceHasNoRelease, t.TaskID, source.RunID())
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
