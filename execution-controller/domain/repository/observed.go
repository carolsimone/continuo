package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// ObservedDeployments reads, outside any transaction, the deployments whose
// attempt the observer follows: the started ones, whose result is not
// recorded yet (model.Deployment.AwaitingResult).
type ObservedDeployments interface {
	// GetByJobName returns the most recently created deployment that created
	// or adopted a Job of jobName (its DeployedAt is set), without locking it,
	// or sql.ErrNoRows when none exists. As with
	// DeploymentRepository.LoadByJobName, a deployment that only carries the
	// name is never returned.
	GetByJobName(ctx context.Context, jobName string) (*model.Deployment, error)
	// ListObservedJobNames returns the Job names of every started deployment:
	// starting, running or terminating.
	ListObservedJobNames(ctx context.Context) ([]string, error)
	// StateAge returns how long ago deployment id last changed status, by the
	// database's clock.
	StateAge(ctx context.Context, id uuid.UUID) (time.Duration, error)
}
