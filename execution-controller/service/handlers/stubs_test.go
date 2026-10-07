package handlers_test

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
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
func (r *stubDeploymentsRepo) GetByJobName(context.Context, string) (*model.Deployment, error) {
	return nil, sql.ErrNoRows
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

// jobRowDeploymentsRepo serves one deployment by its Job name and records
// every save, for tests of the job-status handler's row transitions.
type jobRowDeploymentsRepo struct {
	stubDeploymentsRepo
	row   *model.Deployment
	saved []*model.Deployment
}

func (r *jobRowDeploymentsRepo) GetByJobName(_ context.Context, name string) (*model.Deployment, error) {
	if r.row == nil || r.row.JobName() != name {
		return nil, sql.ErrNoRows
	}
	return r.row, nil
}

func (r *jobRowDeploymentsRepo) Save(_ context.Context, d *model.Deployment) error {
	r.saved = append(r.saved, d)
	return nil
}

// startedProductionRow is a production deployment whose Job jobName was created.
func startedProductionRow(jobName string, taskID, scheduleID uuid.UUID) *model.Deployment {
	now := time.Now()
	return model.Reconstitute(uuid.New(), nil, command.DeployTask{
		TaskID: taskID.String(), ScheduleID: scheduleID.String(), JobName: jobName, NodeType: "dbt-model",
		TaskMaxRetries: 3,
	}, model.StatusStarting, 0, 3, now, now, &now, nil)
}
