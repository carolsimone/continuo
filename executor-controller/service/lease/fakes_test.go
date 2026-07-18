package lease_test

import (
	"context"
	"sync"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// fakeClock is a clock a test advances by hand, so lease deadlines and terminal
// instants are exact rather than approximate.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

var _ ports.Clock = (*fakeClock)(nil)

// fakeCommands resolves every service to one argv. It records how many times it
// was asked, so a test can prove a retry ran the argv its first attempt pinned
// rather than resolving a fresh one.
type fakeCommands struct {
	ports.CommandResolver
	argv  []string
	calls int
}

func (c *fakeCommands) NodeCommand(string, pkgmodel.Operation, pkgmodel.NodeType, string) []string {
	c.calls++
	return c.argv
}

func (c *fakeCommands) WrapperCachePolicy(string) ports.WrapperCachePolicy {
	return ports.WrapperCacheOpaque
}

var _ ports.CommandResolver = (*fakeCommands)(nil)

// fakePodVerifier stands in for the cluster lookup that confirms a claiming
// worker's pod. It records the identity it was asked about and answers with err.
type fakePodVerifier struct {
	err        error
	calls      int
	lastPool   string
	lastName   string
	lastPodUID string
}

func (v *fakePodVerifier) VerifyPod(_ context.Context, poolKey, podName, podUID string) error {
	v.calls++
	v.lastPool, v.lastName, v.lastPodUID = poolKey, podName, podUID
	return v.err
}

var _ ports.PodVerifier = (*fakePodVerifier)(nil)

// fakeDeployments is a map-backed DeploymentRepository. Save writes back into the
// same map the reads are served from, matching production's within-transaction
// read-after-write.
//
// Save models the write-once persistence of resolved_argv/execution_path: the
// stored row keeps the first values written, so a later Save cannot change the
// command a task was already attempted with. Reads return the stored row, which
// is how a retry gets its pinned command back.
type fakeDeployments struct {
	// Embeds the port so operations these tests never exercise need no stub.
	repository.DeploymentRepository

	mu         sync.Mutex
	rows       map[uuid.UUID]*storedRow
	due        []uuid.UUID // pool-due rows, oldest first
	activeSlot int
	saves      int

	lockCapacityErr error
	saveErr         error
}

// storedRow is one persisted deployment: the aggregate plus the write-once
// columns the row keeps independently of what a later Save carries.
type storedRow struct {
	dep           *model.Deployment
	resolvedArgv  []string
	executionPath model.ExecutionPath
}

func newFakeDeployments() *fakeDeployments {
	return &fakeDeployments{rows: map[uuid.UUID]*storedRow{}}
}

// add stores dep as a row due for its pool.
func (r *fakeDeployments) add(dep *model.Deployment) {
	r.rows[dep.ID()] = &storedRow{
		dep:           dep,
		resolvedArgv:  dep.ResolvedArgv(),
		executionPath: dep.ExecutionPath(),
	}
	r.due = append(r.due, dep.ID())
}

func (r *fakeDeployments) LockCapacity(context.Context) error { return r.lockCapacityErr }

func (r *fakeDeployments) ActiveSlotCount(context.Context) (int, error) {
	return r.activeSlot, nil
}

func (r *fakeDeployments) GetDueWorkerForPool(_ context.Context, _ string) (*model.Deployment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range r.due {
		row := r.rows[id]
		if row.dep.Status() == model.StatusPending {
			return r.reload(row), nil
		}
	}
	return nil, nil
}

func (r *fakeDeployments) GetByID(_ context.Context, id uuid.UUID) (*model.Deployment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, nil
	}
	return r.reload(row), nil
}

func (r *fakeDeployments) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*model.Deployment, error) {
	return r.GetByID(ctx, id)
}

// reload rebuilds the row's aggregate from what the row stores, so a read serves
// persisted state rather than an in-memory mutation. A task that was already
// attempted therefore comes back carrying the command that attempt pinned.
func (r *fakeDeployments) reload(row *storedRow) *model.Deployment {
	d := row.dep
	return model.Reconstitute(model.ReconstituteInput{
		ID:                  d.ID(),
		MessageProcessingID: d.MessageProcessingID(),
		Mode:                d.Mode(),
		Command:             d.Command(),
		Status:              d.Status(),
		RetryCount:          d.RetryCount(),
		MaxRetries:          d.MaxRetries(),
		NextAttemptAt:       d.NextAttemptAt(),
		CreatedAt:           d.CreatedAt(),
		ExecutionMode:       d.ExecutionMode(),
		PoolKey:             d.PoolKey(),
		ResolvedArgv:        row.resolvedArgv,
		ExecutionPath:       row.executionPath,
		Reservation:         d.Reservation(),
		Attempt:             d.Attempt(),
		Lease:               d.ActiveLease(),
		TerminalResult:      d.TerminalResult(),
	})
}

func (r *fakeDeployments) Save(_ context.Context, d *model.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saves++
	row, ok := r.rows[d.ID()]
	if !ok {
		r.rows[d.ID()] = &storedRow{dep: d, resolvedArgv: d.ResolvedArgv(), executionPath: d.ExecutionPath()}
		return nil
	}
	row.dep = d
	// resolved_argv and execution_path are write-once: the row keeps the first
	// non-empty values it was given, so a later Save cannot alter the command a
	// task was already attempted with.
	if len(row.resolvedArgv) == 0 {
		row.resolvedArgv = d.ResolvedArgv()
	}
	if row.executionPath == "" {
		row.executionPath = d.ExecutionPath()
	}
	return nil
}

// statusOf reports the persisted status of a row.
func (r *fakeDeployments) statusOf(id uuid.UUID) model.Status {
	return r.rows[id].dep.Status()
}

// storedArgv reports the write-once command the row holds.
func (r *fakeDeployments) storedArgv(id uuid.UUID) []string {
	return r.rows[id].resolvedArgv
}

var _ repository.DeploymentRepository = (*fakeDeployments)(nil)

// recordingOutbox captures the rows a transaction writes.
type recordingOutbox struct {
	pkgoutbox.Repository
	entries []*pkgoutbox.Entry
	err     error
}

func (o *recordingOutbox) Create(_ context.Context, e *pkgoutbox.Entry) error {
	if o.err != nil {
		return o.err
	}
	o.entries = append(o.entries, e)
	return nil
}

func (o *recordingOutbox) streamNames() []string {
	out := make([]string, 0, len(o.entries))
	for _, e := range o.entries {
		out = append(out, e.StreamName)
	}
	return out
}

func (o *recordingOutbox) byStream(stream string) *pkgoutbox.Entry {
	for _, e := range o.entries {
		if e.StreamName == stream {
			return e
		}
	}
	return nil
}

func (o *recordingOutbox) countByStream(stream string) int {
	n := 0
	for _, e := range o.entries {
		if e.StreamName == stream {
			n++
		}
	}
	return n
}

var _ pkgoutbox.Repository = (*recordingOutbox)(nil)
