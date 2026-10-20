package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// ObservedDeployments reads, outside any transaction, the deployments whose
// attempt the observer follows: those awaiting their result
// (model.Deployment.AwaitingResult).
type ObservedDeployments interface {
	// FindByJobName returns the most recently created deployment whose Job is
	// jobName, without locking it, or sql.ErrNoRows when none exists.
	FindByJobName(ctx context.Context, jobName string) (*model.Deployment, error)
	// ListObservedJobNames returns the Job names of every deployment awaiting
	// its result.
	ListObservedJobNames(ctx context.Context) ([]string, error)
	// StateAge returns how long ago deployment id last changed status, by the
	// database's clock.
	StateAge(ctx context.Context, id uuid.UUID) (time.Duration, error)
}
