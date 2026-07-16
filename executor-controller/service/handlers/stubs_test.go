package handlers_test

import (
	"context"
	"database/sql"
	"sync"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// stubDeploymentsRepo captures Add and Save calls for assertion without a real
// DB. byID serves GetByID lookups the same-row retry path performs, and
// bySchedule serves the schedule-scoped lookup the cancellation path performs.
type stubDeploymentsRepo struct {
	// Embeds the port so operations this test never exercises need no stub.
	repository.DeploymentRepository
	mu         sync.Mutex
	added      []*model.Deployment
	saved      []*model.Deployment
	byID       map[uuid.UUID]*model.Deployment
	bySchedule map[uuid.UUID][]*model.Deployment
}

// GetNonTerminalByScheduleForUpdate returns the schedule's seeded deployments
// that have not settled, mirroring the adapter's status filter.
func (r *stubDeploymentsRepo) GetNonTerminalByScheduleForUpdate(_ context.Context, scheduleID uuid.UUID) ([]*model.Deployment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*model.Deployment
	for _, d := range r.bySchedule[scheduleID] {
		if !d.IsTerminal() {
			out = append(out, d)
		}
	}
	return out, nil
}

func (r *stubDeploymentsRepo) Add(_ context.Context, d *model.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, d)
	return nil
}

func (r *stubDeploymentsRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Deployment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.byID[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return d, nil
}

// stubOutboxRepo captures the announcements a handler writes.
type stubOutboxRepo struct {
	pkgoutbox.Repository
	mu      sync.Mutex
	entries []*pkgoutbox.Entry
}

func (r *stubOutboxRepo) Create(_ context.Context, e *pkgoutbox.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
	return nil
}
func (r *stubDeploymentsRepo) GetDueJobs(_ context.Context, _ int) ([]*model.Deployment, error) {
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
