package runservice_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/internal/grpc/runservice"
	"github.com/carolsimone/continuo/state/service/runreads"
)

const (
	tenantA = "default"
	tenantB = "acme"
)

// ctxOf is the context of a request that acts for tenant.
func ctxOf(tenant string) context.Context {
	return identity.WithTenant(context.Background(), tenant)
}

var errBoom = errors.New("boom")

// fakeStore holds the rows every fake read port serves, keyed by tenant, and
// records the arguments of the last read of each port.
type fakeStore struct {
	runs      map[string]map[uuid.UUID]projection.RunDetail
	tasks     map[string]map[uuid.UUID][]projection.TaskDetail
	attempts  map[string]map[uuid.UUID][]projection.TaskAttempt
	graphs    map[string]map[uuid.UUID]projection.RunGraph
	drifts    map[string][]projection.ActiveRunDrift
	latest    map[string]int64
	completed map[string][]projection.RunSummary

	// The node and schedule rows below belong to tenantA; a read for another
	// tenant sees none of them.
	nodeTasks    []*projection.NodeRun
	nodes        []*projection.NodeSummary
	nodeNames    []string
	nodeServices []string
	schedules    []string
	lastRuns     map[string]repository.LastRunSummary

	// The live release's schedule views, by tenant: the graph of each schedule
	// and the summaries of all of them.
	scheduleGraphs map[string]map[string]topology.ScheduleGraph
	summaries      map[string][]topology.ScheduleSummary

	failWith error

	// Arguments of the last read.
	taskFilter            *run.TaskStatus
	taskLimit, taskOffset int
	attemptTaskID         *uuid.UUID
	attemptLimit          int
	attemptOffset         int
	runsLimit, runsOffset int
	nodeTasksLimit        int
	nodeTasksOperation    model.Operation
	nodesOperation        model.Operation
	nodesSearch           string
	nodesService          string
	nodesLimit            int
	nodesOffset           int
	nodeNamesService      string
}

func newStore() *fakeStore {
	return &fakeStore{
		runs:      map[string]map[uuid.UUID]projection.RunDetail{},
		tasks:     map[string]map[uuid.UUID][]projection.TaskDetail{},
		attempts:  map[string]map[uuid.UUID][]projection.TaskAttempt{},
		graphs:    map[string]map[uuid.UUID]projection.RunGraph{},
		drifts:    map[string][]projection.ActiveRunDrift{},
		latest:    map[string]int64{},
		completed: map[string][]projection.RunSummary{},
		lastRuns:  map[string]repository.LastRunSummary{},

		scheduleGraphs: map[string]map[string]topology.ScheduleGraph{},
		summaries:      map[string][]topology.ScheduleSummary{},
	}
}

func (s *fakeStore) putScheduleGraph(tenant, schedule string, g topology.ScheduleGraph) {
	if s.scheduleGraphs[tenant] == nil {
		s.scheduleGraphs[tenant] = map[string]topology.ScheduleGraph{}
	}
	s.scheduleGraphs[tenant][schedule] = g
}

func (s *fakeStore) putRun(tenant string, r projection.RunDetail) {
	if s.runs[tenant] == nil {
		s.runs[tenant] = map[uuid.UUID]projection.RunDetail{}
	}
	s.runs[tenant][r.RunID] = r
}

func (s *fakeStore) putTasks(tenant string, runID uuid.UUID, tasks ...projection.TaskDetail) {
	if s.tasks[tenant] == nil {
		s.tasks[tenant] = map[uuid.UUID][]projection.TaskDetail{}
	}
	s.tasks[tenant][runID] = tasks
}

func (s *fakeStore) putAttempts(tenant string, runID uuid.UUID, attempts ...projection.TaskAttempt) {
	if s.attempts[tenant] == nil {
		s.attempts[tenant] = map[uuid.UUID][]projection.TaskAttempt{}
	}
	s.attempts[tenant][runID] = attempts
}

func (s *fakeStore) putGraph(tenant string, runID uuid.UUID, g projection.RunGraph) {
	if s.graphs[tenant] == nil {
		s.graphs[tenant] = map[uuid.UUID]projection.RunGraph{}
	}
	s.graphs[tenant][runID] = g
}

// The fakes below are the read ports of the run reads over fakeStore.

type fakeRunDetails struct{ *fakeStore }

var _ repository.RunDetailReader = fakeRunDetails{}

func (f fakeRunDetails) GetRunDetail(_ context.Context, tenantID string, runID uuid.UUID) (projection.RunDetail, error) {
	if f.failWith != nil {
		return projection.RunDetail{}, f.failWith
	}
	r, ok := f.runs[tenantID][runID]
	if !ok {
		return projection.RunDetail{}, repository.ErrRunNotFound
	}
	return r, nil
}

type fakeTaskDetails struct{ *fakeStore }

var _ repository.TaskDetailReader = fakeTaskDetails{}

func (f fakeTaskDetails) ListRunTasks(_ context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]projection.TaskDetail, int, error) {
	f.taskFilter, f.taskLimit, f.taskOffset = status, limit, offset
	if f.failWith != nil {
		return nil, 0, f.failWith
	}
	var matching []projection.TaskDetail
	for _, t := range f.tasks[tenantID][runID] {
		if status == nil || t.Status == *status {
			matching = append(matching, t)
		}
	}
	total := len(matching)
	if offset >= len(matching) {
		return nil, total, nil
	}
	end := min(offset+limit, len(matching))
	return matching[offset:end], total, nil
}

type fakeAttempts struct{ *fakeStore }

var _ repository.TaskAttemptReader = fakeAttempts{}

func (f fakeAttempts) ListRunAttempts(_ context.Context, tenantID string, runID uuid.UUID, taskID *uuid.UUID, pageSize, pageOffset int) ([]projection.TaskAttempt, int, error) {
	f.attemptTaskID, f.attemptLimit, f.attemptOffset = taskID, pageSize, pageOffset
	if f.failWith != nil {
		return nil, 0, f.failWith
	}
	var matching []projection.TaskAttempt
	for _, a := range f.attempts[tenantID][runID] {
		if taskID == nil || a.TaskID == *taskID {
			matching = append(matching, a)
		}
	}
	return matching, len(matching), nil
}

type fakeRunReads struct{ *fakeStore }

var _ repository.RunReadRepository = fakeRunReads{}

func (f fakeRunReads) GetRunGraph(_ context.Context, tenantID string, runID uuid.UUID) (projection.RunGraph, error) {
	if f.failWith != nil {
		return projection.RunGraph{}, f.failWith
	}
	g, ok := f.graphs[tenantID][runID]
	if !ok {
		return projection.RunGraph{LatestPromotionSeq: f.latest[tenantID]}, nil
	}
	return g, nil
}

func (f fakeRunReads) ListCompletedRuns(_ context.Context, tenantID, scheduleName string, limit, offset int) ([]projection.RunSummary, int, error) {
	f.runsLimit, f.runsOffset = limit, offset
	if f.failWith != nil {
		return nil, 0, f.failWith
	}
	var matching []projection.RunSummary
	for _, r := range f.completed[tenantID] {
		if r.ScheduleName == scheduleName {
			matching = append(matching, r)
		}
	}
	return matching, len(matching), nil
}

func (f fakeRunReads) ListActiveRunDrifts(_ context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error) {
	if f.failWith != nil {
		return 0, nil, f.failWith
	}
	return f.latest[tenantID], f.drifts[tenantID], nil
}

func (f fakeRunReads) LastSuccessDurations(context.Context, string, []run.NodeID) (map[run.NodeID]time.Duration, error) {
	return nil, nil
}

type fakeNodes struct{ *fakeStore }

var _ repository.NodeReader = fakeNodes{}

func (f fakeNodes) ListNodeTasks(_ context.Context, tenantID, _, _, _ string, operation model.Operation, limit int) ([]*projection.NodeRun, error) {
	f.nodeTasksOperation, f.nodeTasksLimit = operation, limit
	if f.failWith != nil {
		return nil, f.failWith
	}
	if tenantID != tenantA {
		return nil, nil
	}
	return f.nodeTasks, nil
}

func (f fakeNodes) ListNodes(_ context.Context, tenantID, search, serviceName string, operation model.Operation, limit, offset int) ([]*projection.NodeSummary, int, error) {
	f.nodesSearch, f.nodesService, f.nodesOperation, f.nodesLimit, f.nodesOffset = search, serviceName, operation, limit, offset
	if f.failWith != nil {
		return nil, 0, f.failWith
	}
	if tenantID != tenantA {
		return nil, 0, nil
	}
	return f.nodes, len(f.nodes), nil
}

func (f fakeNodes) ListNodeNames(_ context.Context, tenantID, serviceName string) ([]string, error) {
	f.nodeNamesService = serviceName
	if f.failWith != nil {
		return nil, f.failWith
	}
	if tenantID != tenantA {
		return nil, nil
	}
	return f.nodeNames, nil
}

func (f fakeNodes) ListNodeServices(_ context.Context, tenantID string) ([]string, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	if tenantID != tenantA {
		return nil, nil
	}
	return f.nodeServices, nil
}

type fakeActiveSchedules struct{ *fakeStore }

var _ repository.ActiveScheduleReader = fakeActiveSchedules{}

func (f fakeActiveSchedules) ListActive(_ context.Context, tenantID string) ([]string, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	if tenantID != tenantA {
		return nil, nil
	}
	names := append([]string(nil), f.schedules...)
	sort.Strings(names)
	return names, nil
}

type fakeLastRuns struct{ *fakeStore }

var _ repository.LastRunReader = fakeLastRuns{}

func (f fakeLastRuns) GetLastRunPerSchedule(_ context.Context, tenantID string) (map[string]repository.LastRunSummary, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	if tenantID != tenantA {
		return nil, nil
	}
	return f.lastRuns, nil
}

// fakeScheduleRows serves the schedule rows it holds to List, each under its
// own tenant only.
type fakeScheduleRows struct {
	repository.ScheduleRepository
	rows []*schedule.Schedule
}

func (f fakeScheduleRows) List(_ context.Context, tenantID string) ([]*schedule.Schedule, error) {
	var out []*schedule.Schedule
	for _, s := range f.rows {
		if s.TenantID() == tenantID {
			out = append(out, s)
		}
	}
	return out, nil
}

type fakeScheduleGraphs struct{ *fakeStore }

var _ repository.ScheduleGraphRepository = fakeScheduleGraphs{}

func (f fakeScheduleGraphs) Graph(_ context.Context, tenantID, scheduleName string) (topology.ScheduleGraph, error) {
	if f.failWith != nil {
		return topology.ScheduleGraph{}, f.failWith
	}
	return f.scheduleGraphs[tenantID][scheduleName], nil
}

func (f fakeScheduleGraphs) Summaries(_ context.Context, tenantID string) ([]topology.ScheduleSummary, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	return f.summaries[tenantID], nil
}

// newServer builds the RunService server over the fakes of store and the
// schedule rows, none when nil, with no commands.
func newServer(store *fakeStore, rows repository.ScheduleRepository) *runservice.Server {
	return runservice.NewServer(readsOf(store, rows), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newCommandServer builds the RunService server over the fakes of store, whose
// commands and rebase previews cmds answers.
func newCommandServer(store *fakeStore, cmds *fakeCommands) *runservice.Server {
	reads := readsOf(store, nil)
	reads.Rebases = cmds
	return runservice.NewServer(reads, cmds, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// readsOf is the read services over the fakes of store and the schedule rows,
// none when nil.
func readsOf(store *fakeStore, rows repository.ScheduleRepository) runservice.Reads {
	if rows == nil {
		rows = fakeScheduleRows{}
	}
	return runservice.Reads{
		Runs:      runreads.NewRunDetails(fakeRunDetails{store}, fakeTaskDetails{store}),
		Attempts:  runreads.NewAttempts(fakeAttempts{store}),
		Nodes:     runreads.NewNodes(fakeNodes{store}, 30*time.Minute),
		Schedules: runreads.NewSchedules(fakeActiveSchedules{store}, fakeLastRuns{store}, rows),
		Graphs:    runreads.New(fakeRunReads{store}),

		ScheduleGraphs: runreads.NewScheduleGraphs(fakeScheduleGraphs{store}),
	}
}
