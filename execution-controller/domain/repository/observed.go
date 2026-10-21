package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// ObservedDeployments reads, outside any transaction, the deployments whose
// attempt the observer follows: the started ones, and a row an older version
// released before recording its result while its Job exists
// (model.Deployment.AwaitingResult).
type ObservedDeployments interface {
	// GetByJobName returns the most recently created deployment whose Job is
	// jobName, without locking it, or sql.ErrNoRows when none exists.
	GetByJobName(ctx context.Context, jobName string) (*model.Deployment, error)
	// ListObservedJobNames returns the Job names of every started deployment:
	// starting, running or terminating.
	ListObservedJobNames(ctx context.Context) ([]string, error)
	// StateAge returns how long ago deployment id last changed status, by the
	// database's clock.
	StateAge(ctx context.Context, id uuid.UUID) (time.Duration, error)
}
