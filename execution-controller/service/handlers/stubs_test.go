package handlers_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
	"github.com/google/uuid"
)

// stubDeploymentsRepo captures Add calls for assertion without a real DB.
// stored holds the production deployments by "<task>/<attempt>": AddAttempt
// stores into it and LoadByTaskAttempt and HasAttemptAfter read it. A
// deployment in storedByRace is stored by another transaction when
// AddAttempt is called for its attempt, so that call stores nothing and
// reports false.
type stubDeploymentsRepo struct {
	mu           sync.Mutex
	added        []*model.Deployment
	stored       map[string]*model.Deployment
	storedByRace map[string]*model.Deployment
	addAttempts  int
	saved        []*model.Deployment
}

func attemptKey(taskID string, attempt int) string { return fmt.Sprintf("%s/%d", taskID, attempt) }

func (r *stubDeploymentsRepo) Add(_ context.Context, d *model.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, d)
	return nil
}
func (r *stubDeploymentsRepo) LoadByJobName(context.Context, string) (*model.Deployment, error) {
	return nil, sql.ErrNoRows
}
func (r *stubDeploymentsRepo) LockOpenOfRun(context.Context, string) ([]*model.Deployment, error) {
	return nil, nil
}

func (r *stubDeploymentsRepo) Save(_ context.Context, d *model.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = append(r.saved, d)
	return nil
}
func (r *stubDeploymentsRepo) GetByReleaseNode(_ context.Context, _, _ string, _ model.Mode) (*model.Deployment, error) {
	return nil, nil
}
func (r *stubDeploymentsRepo) PendingValidationCount(_ context.Context, _ string, _ model.Mode) (int, error) {
	return 0, nil
}
func (r *stubDeploymentsRepo) ListValidationResults(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *stubDeploymentsRepo) ListValidationByRelease(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}

func (r *stubDeploymentsRepo) AddAttempt(_ context.Context, d *model.Deployment) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addAttempts++
	if r.stored == nil {
		r.stored = map[string]*model.Deployment{}
	}
	key := attemptKey(d.Command().TaskID, d.Command().Attempt)
	if other, ok := r.storedByRace[key]; ok {
		r.stored[key] = other
		delete(r.storedByRace, key)
	}
	if _, ok := r.stored[key]; ok {
		return false, nil
	}
	r.stored[key] = d
	return true, nil
}
func (r *stubDeploymentsRepo) LoadByTaskAttempt(_ context.Context, taskID string, attempt int) (*model.Deployment, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.stored[attemptKey(taskID, attempt)]
	return d, ok, nil
}
func (r *stubDeploymentsRepo) HasAttemptAfter(_ context.Context, taskID string, attempt int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.stored {
		if d.Command().TaskID == taskID && d.Command().Attempt > attempt {
			return true, nil
		}
	}
	return false, nil
}

func newFakeUoW(depl repository.DeploymentRepository, cancelled repository.CancelledRunsRepository) *fakes.FakeUnitOfWork {
	return &fakes.FakeUnitOfWork{Deployments: depl, Cancelled: cancelled}
}

// recordingCancelledRepo records the fences inserted and the holds of a run
// taken ("dispatch" or "fence"); Exists answers fenced.
type recordingCancelledRepo struct {
	mu       sync.Mutex
	inserted []uuid.UUID
	holds    []string
	fenced   bool
}

func (r *recordingCancelledRepo) HoldRunForDispatch(_ context.Context, _ uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.holds = append(r.holds, "dispatch")
	return nil
}

func (r *recordingCancelledRepo) HoldRunForFence(_ context.Context, _ uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.holds = append(r.holds, "fence")
	return nil
}

func (r *recordingCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inserted = append(r.inserted, id)
	return nil
}
func (r *recordingCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return r.fenced, nil
}
func (r *recordingCancelledRepo) DeleteOlderThan(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
