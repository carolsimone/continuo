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
	// EndedUndeployed reports whether the production deployment of attempt
	// attempt of taskID ended failed without ever creating or adopting its
	// Job (its DeployedAt is unset): the Job's creation failed for good, or
	// the dispatch was refused. A Job of that attempt that exists was created
	// by Kubernetes after the create call reported failure. False when no
	// deployment of the attempt exists.
	EndedUndeployed(ctx context.Context, taskID uuid.UUID, attempt int) (bool, error)
	// NameEndedUndeployed is EndedUndeployed for a Job that names no attempt
	// (a release leg's): it judges the newest deployment carrying jobName.
	NameEndedUndeployed(ctx context.Context, jobName string) (bool, error)
	// StateAge returns how long ago deployment id last changed status, by the
	// database's clock.
	StateAge(ctx context.Context, id uuid.UUID) (time.Duration, error)
}
