package uow

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	ports "github.com/carolsimone/continuo/state/service/ports"
)

// FakeUnitOfWork is an in-memory UnitOfWork for handler unit tests.
//
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock)
// default to nil and can be set via the corresponding setter methods.
// Clock() falls back to ports.SystemClock{} when no override is provided.
type FakeUnitOfWork struct {
	MessageProcessing messageprocessing.Repository

	BeginCalled    int
	CommitCalled   int
	RollbackCalled int

	inTx bool
	// afterCommit holds the functions AfterCommit registered in the current
	// transaction.
	afterCommit []func()

	runRepo     repository.RunRepository
	catalogRepo repository.ScheduleCatalogRepository
	outboxPub   ports.OutboxPublisher
	taskColl    run.TaskCollection
	clock       ports.Clock

	// TopologyRepo backs Topology(); set it to observe topology writes in
	// handler tests.
	TopologyRepo repository.TopologyRepository

	// MaterializationRepo backs Materializations().
	MaterializationRepo repository.MaterializationRepository

	// Graph backs TaskGraph(); GraphTenant records the tenant it was last
	// asked for.
	Graph       run.TaskGraph
	GraphTenant string

	// TaskCollectionTenants records the tenant of every TaskCollection call,
	// in call order.
	TaskCollectionTenants []string

	// AttemptRepo backs Attempts().
	AttemptRepo repository.AttemptRepository

	// LeaseRepo backs Leases().
	LeaseRepo repository.LeaseRepository

	// CommandRequestRepo backs CommandRequests().
	CommandRequestRepo repository.CommandRequestRepository

	// ScheduleRepo backs Schedules().
	ScheduleRepo repository.ScheduleRepository
}

func (f *FakeUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	return f.MessageProcessing
}
func (f *FakeUnitOfWork) Begin(_ context.Context) error {
	if f.inTx {
		return errors.New("transaction already in progress")
	}
	f.inTx = true
	f.afterCommit = nil
	f.BeginCalled++
	return nil
}

// Commit ends the transaction and runs the functions AfterCommit registered
// in it, in registration order.
func (f *FakeUnitOfWork) Commit() error {
	if !f.inTx {
		return errors.New("no transaction in progress")
	}
	f.inTx = false
	f.CommitCalled++
	hooks := f.afterCommit
	f.afterCommit = nil
	for _, fn := range hooks {
		fn()
	}
	return nil
}

// AfterCommit registers fn to run at the next Commit; Begin and Rollback
// discard it.
func (f *FakeUnitOfWork) AfterCommit(fn func()) { f.afterCommit = append(f.afterCommit, fn) }

func (f *FakeUnitOfWork) Rollback() error {
	if !f.inTx {
		return nil
	}
	f.inTx = false
	f.afterCommit = nil
	f.RollbackCalled++
	return nil
}

// Aggregate-level accessors. Return whatever was set via the setter methods.

func (f *FakeUnitOfWork) Run() repository.RunRepository { return f.runRepo }
func (f *FakeUnitOfWork) Outbox() ports.OutboxPublisher { return f.outboxPub }
func (f *FakeUnitOfWork) TaskCollection(tenantID string) run.TaskCollection {
	f.TaskCollectionTenants = append(f.TaskCollectionTenants, tenantID)
	return f.taskColl
}

// Catalog returns the aggregate-level repository.ScheduleCatalogRepository set via SetCatalogRepo.
func (f *FakeUnitOfWork) Catalog() repository.ScheduleCatalogRepository { return f.catalogRepo }

// Topology returns the repository.TopologyRepository set via the TopologyRepo field.
func (f *FakeUnitOfWork) Topology() repository.TopologyRepository { return f.TopologyRepo }

// Materializations returns the repository.MaterializationRepository set via
// the MaterializationRepo field.
func (f *FakeUnitOfWork) Materializations() repository.MaterializationRepository {
	return f.MaterializationRepo
}

// TaskGraph returns the run.TaskGraph set via the Graph field and records
// tenantID.
func (f *FakeUnitOfWork) TaskGraph(tenantID string) run.TaskGraph {
	f.GraphTenant = tenantID
	return f.Graph
}

// Attempts returns the repository.AttemptRepository set via the AttemptRepo
// field.
func (f *FakeUnitOfWork) Attempts() repository.AttemptRepository { return f.AttemptRepo }

// Leases returns the repository.LeaseRepository set via the LeaseRepo field.
func (f *FakeUnitOfWork) Leases() repository.LeaseRepository { return f.LeaseRepo }

// CommandRequests returns the repository.CommandRequestRepository set via the
// CommandRequestRepo field.
func (f *FakeUnitOfWork) CommandRequests() repository.CommandRequestRepository {
	return f.CommandRequestRepo
}

// Schedules returns the repository.ScheduleRepository set via the ScheduleRepo
// field.
func (f *FakeUnitOfWork) Schedules() repository.ScheduleRepository { return f.ScheduleRepo }

// Clock returns the configured clock, falling back to ports.SystemClock{} when
// no override has been set.
func (f *FakeUnitOfWork) Clock() ports.Clock {
	if f.clock == nil {
		return ports.SystemClock{}
	}
	return f.clock
}

// Setters allow individual tests to inject only the aggregate-level fakes they need.
func (f *FakeUnitOfWork) SetRunRepo(r repository.RunRepository)                 { f.runRepo = r }
func (f *FakeUnitOfWork) SetCatalogRepo(r repository.ScheduleCatalogRepository) { f.catalogRepo = r }
func (f *FakeUnitOfWork) SetOutboxPublisher(p ports.OutboxPublisher)            { f.outboxPub = p }
func (f *FakeUnitOfWork) SetTaskCollection(tc run.TaskCollection)               { f.taskColl = tc }
func (f *FakeUnitOfWork) SetClock(c ports.Clock)                                { f.clock = c }

// Compile-time interface assertion.
var _ UnitOfWork = (*FakeUnitOfWork)(nil)

// ErrNotFaked is returned by a fake method the test did not set up.
var ErrNotFaked = errors.New("uow fake: method not faked")

// LiveReleaseOnly is a repository.TopologyRepository that answers only
// LivePointer, with Live, and LockTopology, which it grants at once. Handler
// tests that create runs use it.
type LiveReleaseOnly struct{ Live topology.Pointer }

var _ repository.TopologyRepository = (*LiveReleaseOnly)(nil)

func (f *LiveReleaseOnly) LivePointer(context.Context, string) (topology.Pointer, error) {
	return f.Live, nil
}
func (f *LiveReleaseOnly) LivePointers(context.Context) ([]topology.Pointer, error) {
	return nil, ErrNotFaked
}
func (f *LiveReleaseOnly) LockTopology(context.Context) error { return nil }
func (f *LiveReleaseOnly) SaveRelease(context.Context, topology.Release) (bool, error) {
	return false, ErrNotFaked
}
func (f *LiveReleaseOnly) SetLivePointer(context.Context, string, topology.Pointer, time.Time) error {
	return ErrNotFaked
}
func (f *LiveReleaseOnly) ReleaseNodes(context.Context, string, string, string) ([]topology.Node, error) {
	return nil, ErrNotFaked
}
func (f *LiveReleaseOnly) ScheduleGraph(context.Context, string, string, string) (topology.Graph, error) {
	return topology.Graph{}, ErrNotFaked
}
func (f *LiveReleaseOnly) Graph(context.Context, string, string, []topology.NodeKey) (topology.Graph, error) {
	return topology.Graph{}, ErrNotFaked
}

func (f *LiveReleaseOnly) NodeContentHash(context.Context, string, string, topology.NodeKey) (string, bool, error) {
	return "", false, ErrNotFaked
}
func (f *LiveReleaseOnly) PruneReleases(context.Context, time.Time) (int64, error) {
	return 0, ErrNotFaked
}

// FakeLiveScheduleModels is an in-memory repository.LiveScheduleModels. Counts
// holds, per tenant, how many models of the tenant's live release carry each
// schedule name; a tenant or name it does not hold has none. With Err set,
// every read fails with it.
type FakeLiveScheduleModels struct {
	Counts map[string]map[string]int
	Err    error
}

var _ repository.LiveScheduleModels = (*FakeLiveScheduleModels)(nil)

// CountByScheduleName returns a copy of the tenant's counts, leaving out a name
// set to 0.
func (f *FakeLiveScheduleModels) CountByScheduleName(_ context.Context, tenantID string) (map[string]int, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	out := map[string]int{}
	for name, n := range f.Counts[tenantID] {
		if n > 0 {
			out[name] = n
		}
	}
	return out, nil
}

// CountOf returns the tenant's count of scheduleName.
func (f *FakeLiveScheduleModels) CountOf(_ context.Context, tenantID, scheduleName string) (int, error) {
	if f.Err != nil {
		return 0, f.Err
	}
	return f.Counts[tenantID][scheduleName], nil
}
