package handlers_test

import (
	"context"
	"sync"
	"time"

	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/google/uuid"
)

// stubDeploymentsRepo captures Create calls for assertion without a real DB.
type stubDeploymentsRepo struct {
	mu   sync.Mutex
	rows []*deployer.Deployment
}

func (r *stubDeploymentsRepo) Create(_ context.Context, d *deployer.Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, d)
	return nil
}
func (r *stubDeploymentsRepo) GetDueBatch(_ context.Context, _ int) ([]*deployer.Deployment, error) {
	return nil, nil
}
func (r *stubDeploymentsRepo) MarkDeployed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubDeploymentsRepo) Reschedule(_ context.Context, _ uuid.UUID, _ time.Time, _ string) error {
	return nil
}
func (r *stubDeploymentsRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }
