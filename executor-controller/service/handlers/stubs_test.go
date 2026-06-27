package handlers_test

import (
	"context"
	"sync"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
)

// stubDeploymentsRepo captures Add calls for assertion without a real DB.
type stubDeploymentsRepo struct {
	mu    sync.Mutex
	added []*model.Deployment
}

func (r *stubDeploymentsRepo) Add(_ context.Context, d *model.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, d)
	return nil
}
func (r *stubDeploymentsRepo) GetDueBatch(_ context.Context, _ int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *stubDeploymentsRepo) Save(_ context.Context, _ *model.Deployment) error { return nil }
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
