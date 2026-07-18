// Package lease drives the lifecycle of a task a worker pod claims and runs:
// the claim itself, the worker's start and heartbeat reports, and its terminal
// report. Every transition commits the aggregate's new state together with the
// lifecycle announcements it owes the rest of the system, so a task cannot change
// state without the run hearing about it.
package lease

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/tasklifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// tokenBytes is the size of a raw lease token. 32 bytes of randomness makes a
// token unguessable, so holding one is proof of having claimed the task.
const tokenBytes = 32

// ErrPoolMismatch rejects a worker that holds a task's lease but authenticated
// as a different pool than the task belongs to. Pools are per-service,
// per-image, and per-credential, so a worker acting outside its own pool is
// crossing a boundary whatever token it carries.
//
// It is checked after the lease fence, so a caller that does not hold the lease
// is told only that its lease is stale and cannot use the distinction to
// discover which tasks exist.
var ErrPoolMismatch = errors.New("task belongs to another pool")

// ErrNoSuchDeployment reports that the row a worker's report names is not
// there. It is answered exactly as a stale lease is: a caller presenting a
// lease for a row that does not exist holds no lease over anything, and telling
// it apart from a caller whose lease was superseded would reveal which
// deployments exist to anyone willing to guess an id.
var ErrNoSuchDeployment = errors.New("executor deployment does not exist")

// ErrCancelled tells the lease holder its task was cancelled and it must stop.
// Cancelling a task deletes the pod running it, so a worker sees this only by
// outliving the deletion's termination grace period and reporting once more. It
// is terminal for that report: a worker that gets it abandons the task rather
// than retrying.
var ErrCancelled = errors.New("task was cancelled")

// unretryableErrorClasses are the failures no rerun can fix: the task's runtime
// artifact was rejected or could not be verified, or the dbt node it names cannot
// be resolved to exactly one node. Retrying them burns the task's budget and
// delays the failure an operator has to act on, so they fail on the first report
// however transient the worker judged them.
var unretryableErrorClasses = map[string]bool{
	"runtime_manifest_rejected":   true,
	"runtime_manifest_unverified": true,
	"dbt_unique_id_not_found":     true,
	"dbt_selector_not_unique":     true,
}

// ClaimInput identifies the worker asking for work and the pool it serves.
type ClaimInput struct {
	PoolKey string
	Owner   string
	PodName string
	PodUID  string
}

// Grant is an exclusive hold on one task, handed to the worker that claimed it.
// Token is the raw lease token, returned here exactly once and never stored: the
// worker presents it on every later report, and only its digest reaches the row.
type Grant struct {
	DeploymentID  uuid.UUID
	LeaseID       uuid.UUID
	Token         string
	Attempt       int
	ExpiresAt     time.Time
	ExecutionPath model.ExecutionPath
	Argv          []string
	Command       command.DeployTask
}

// StartInput is a worker's report that its dbt process has started. PoolKey is
// the pool the reporting worker authenticated as, which must own the task.
type StartInput struct {
	PoolKey      string
	DeploymentID uuid.UUID
	LeaseID      uuid.UUID
	Token        string
}

// HeartbeatInput is a worker's request to extend the lease it holds.
type HeartbeatInput struct {
	PoolKey      string
	DeploymentID uuid.UUID
	LeaseID      uuid.UUID
	Token        string
}

// CompleteInput is a worker's terminal report for the task it holds.
type CompleteInput struct {
	PoolKey      string
	DeploymentID uuid.UUID
	LeaseID      uuid.UUID
	Token        string
	Result       model.WorkerResult
}

// TaskInput asks for the identity of the task a lease holds.
type TaskInput struct {
	PoolKey      string
	DeploymentID uuid.UUID
	LeaseID      uuid.UUID
	Token        string
}

// TaskRef is a task's own identity, read from its row. Callers derive a task's
// result locations from this rather than from anything a worker sends, so a
// worker cannot name a task it does not hold.
type TaskRef struct {
	PoolKey string
	Command command.DeployTask
}

// Config wires a Service to its collaborators.
type Config struct {
	// UnitOfWork returns a fresh unit of work, so concurrent worker requests
	// never share transaction state.
	UnitOfWork func() uow.UnitOfWork
	Commands   ports.CommandResolver
	Clock      ports.Clock
	// MaxConcurrentExecutions is the executor's shared execution budget. Worker
	// claims and Kubernetes Job dispatch draw on the same slots.
	MaxConcurrentExecutions int
	// LeaseTTL is how long a claim or heartbeat holds a task before the lease
	// reaper may reassign it.
	LeaseTTL time.Duration
	// RetryBackoff is how long a task that failed retryably waits before its next
	// attempt may claim it.
	RetryBackoff time.Duration
	Logger       *slog.Logger
}

// Service drives the worker lease lifecycle.
type Service struct {
	newUoW                  func() uow.UnitOfWork
	commands                ports.CommandResolver
	clock                   ports.Clock
	fanout                  tasklifecycle.Fanout
	maxConcurrentExecutions int
	leaseTTL                time.Duration
	retryBackoff            time.Duration
	logger                  *slog.Logger
}

// NewService constructs the lease application service.
func NewService(cfg Config) *Service {
	return &Service{
		newUoW:                  cfg.UnitOfWork,
		commands:                cfg.Commands,
		clock:                   cfg.Clock,
		maxConcurrentExecutions: cfg.MaxConcurrentExecutions,
		leaseTTL:                cfg.LeaseTTL,
		retryBackoff:            cfg.RetryBackoff,
		logger:                  cfg.Logger,
	}
}

// Claim gives one due task in poolKey to the asking worker, under an exclusive
// expiring lease that takes an execution slot. It returns no grant — rather than
// an error — when the pool has no due work or the executor's budget is spent:
// both are ordinary conditions a polling worker asks about constantly.
//
// The capacity lock is taken before the slots are counted, so a claim serializes
// against every other transaction that reserves one and two claims cannot take
// the same free slot.
func (s *Service) Claim(ctx context.Context, in ClaimInput) (*Grant, error) {
	var grant *Grant
	err := s.withTx(ctx, func(repo repository.DeploymentRepository, outboxRepo outbox.Repository) error {
		if err := repo.LockCapacity(ctx); err != nil {
			return err
		}
		active, err := repo.ActiveSlotCount(ctx)
		if err != nil {
			return err
		}
		if active >= s.maxConcurrentExecutions {
			return nil
		}
		dep, err := repo.GetDueWorkerForPool(ctx, in.PoolKey)
		if err != nil || dep == nil {
			return err
		}

		argv, path, err := s.pinExecution(dep)
		if err != nil {
			return s.rejectUnrunnable(ctx, repo, outboxRepo, dep, err)
		}

		token, err := newToken()
		if err != nil {
			return err
		}
		now := s.clock.Now()
		leaseID := uuid.New()
		if err := dep.Claim(leaseID, tokenHash(token), in.Owner, in.PodName, in.PodUID,
			now, now.Add(s.leaseTTL), argv, path); err != nil {
			return err
		}
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		grant = &Grant{
			DeploymentID: dep.ID(), LeaseID: leaseID, Token: token,
			Attempt: dep.Attempt(), ExpiresAt: now.Add(s.leaseTTL),
			ExecutionPath: path, Argv: argv, Command: dep.Command(),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return grant, nil
}

// pinExecution returns the command this task runs and how it runs it. A task that
// has already been attempted runs what its row holds: the stored values record
// what the task was actually attempted with, and are write-once, so a
// configuration reload between attempts cannot change the command of a task
// already in flight. Only a task no attempt has pinned resolves its command.
func (s *Service) pinExecution(dep *model.Deployment) ([]string, model.ExecutionPath, error) {
	if argv := dep.ResolvedArgv(); len(argv) > 0 {
		return argv, dep.ExecutionPath(), nil
	}
	return routing.ResolveExecution(s.commands, dep)
}

// rejectUnrunnable settles a task whose command cannot be resolved at all. No
// retry changes that, and leaving the row pending would offer it to every
// subsequent claim while its run waited on a node that could never report, so the
// row is failed and announced instead.
func (s *Service) rejectUnrunnable(
	ctx context.Context,
	repo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	dep *model.Deployment,
	cause error,
) error {
	if !errors.Is(cause, pkgevents.ErrPermanent) {
		return cause
	}
	reason := cause.Error()
	if err := dep.RejectBeforeExecution(reason, s.clock.Now()); err != nil {
		return err
	}
	if err := repo.Save(ctx, dep); err != nil {
		return err
	}
	if err := s.fanout.DispatchRejected(ctx, outboxRepo, dep, reason); err != nil {
		return err
	}
	s.logger.Error("worker task cannot be run — failing it permanently",
		"deployment_id", dep.ID(), "task_id", dep.Command().TaskID, "reason", reason)
	return nil
}

// authorize decides whether a worker's report may act on this task at all. The
// lease fence runs first and dominates: a caller that does not hold the lease
// learns only that its lease is stale, never whether the task exists or which
// pool owns it. Only a caller holding the genuine lease is then checked against
// the pool it authenticated as.
//
// Every lease-bearing transition fences again inside the aggregate; this is the
// application's own check, and the one that adds pool ownership.
func (s *Service) authorize(dep *model.Deployment, leaseID uuid.UUID, token, poolKey string) error {
	if !dep.ActiveLease().Authorizes(leaseID, tokenHash(token)) {
		return model.ErrStaleLease
	}
	if dep.PoolKey() != poolKey {
		return ErrPoolMismatch
	}
	return nil
}

// Task returns the identity of the task a lease holds. It is the only way a
// caller learns a task's schedule and task IDs, so the locations derived from
// them describe the task the worker actually holds.
func (s *Service) Task(ctx context.Context, in TaskInput) (TaskRef, error) {
	var ref TaskRef
	err := s.withTx(ctx, func(repo repository.DeploymentRepository, _ outbox.Repository) error {
		dep, err := s.load(ctx, repo, in.DeploymentID)
		if err != nil {
			return err
		}
		if err := s.authorize(dep, in.LeaseID, in.Token, in.PoolKey); err != nil {
			return err
		}
		ref = TaskRef{PoolKey: dep.PoolKey(), Command: dep.Command()}
		return nil
	})
	if err != nil {
		return TaskRef{}, err
	}
	return ref, nil
}

// Start records that the lease holder's dbt process is running and announces it.
// A worker that retries a start report it never saw acknowledged neither errors
// nor announces RUNNING twice.
func (s *Service) Start(ctx context.Context, in StartInput) error {
	return s.withTx(ctx, func(repo repository.DeploymentRepository, outboxRepo outbox.Repository) error {
		dep, err := s.load(ctx, repo, in.DeploymentID)
		if err != nil {
			return err
		}
		if err := s.authorize(dep, in.LeaseID, in.Token, in.PoolKey); err != nil {
			return err
		}
		changed, err := dep.AcknowledgeStart(in.LeaseID, tokenHash(in.Token), s.clock.Now())
		if err != nil || !changed {
			return err
		}
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return s.fanout.Started(ctx, outboxRepo, dep)
	})
}

// Heartbeat extends the deadline of the lease its holder presents. Identical
// repeated heartbeats are safe; a worker whose lease no longer holds the task is
// fenced, so it cannot keep a reassigned task's lease alive.
//
// A heartbeat on a cancelled task returns ErrCancelled to the holder. Cancelling
// a task deletes the pod running it, which Kubernetes serves with a termination
// grace period; this answers the worker that outlives that period and reports
// once more, telling it to abandon the task rather than failing as an unexpected
// status it would retry against.
func (s *Service) Heartbeat(ctx context.Context, in HeartbeatInput) error {
	return s.withTx(ctx, func(repo repository.DeploymentRepository, _ outbox.Repository) error {
		dep, err := s.load(ctx, repo, in.DeploymentID)
		if err != nil {
			return err
		}
		if err := s.authorize(dep, in.LeaseID, in.Token, in.PoolKey); err != nil {
			return err
		}
		if dep.Status() == model.StatusCancelled {
			return ErrCancelled
		}
		now := s.clock.Now()
		if err := dep.Heartbeat(in.LeaseID, tokenHash(in.Token), now, now.Add(s.leaseTTL)); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	})
}

// Complete applies the lease holder's terminal report and announces the outcome.
// A success settles the task; a failure is classified and either parked for
// requeue or failed permanently. The transition and its announcements commit
// together, so a task never settles silently.
//
// A terminal report on a cancelled task returns ErrCancelled, as a heartbeat on
// one does. Cancelling keeps the lease, so a worker that finishes its dbt run
// inside the deletion's grace period still authorizes; without this it would
// reach the aggregate's status guard and be answered a fault it would retry
// against for as long as its pod lived. The check follows the lease fence for
// the same reason Heartbeat's does: a caller that does not hold the lease is
// told only that its lease is stale, and cannot learn from a distinct answer
// that the task it guessed at exists and was cancelled.
func (s *Service) Complete(ctx context.Context, in CompleteInput) error {
	return s.withTx(ctx, func(repo repository.DeploymentRepository, outboxRepo outbox.Repository) error {
		dep, err := s.load(ctx, repo, in.DeploymentID)
		if err != nil {
			return err
		}
		if err := s.authorize(dep, in.LeaseID, in.Token, in.PoolKey); err != nil {
			return err
		}
		if dep.Status() == model.StatusCancelled {
			return ErrCancelled
		}
		if in.Result.Succeeded {
			return s.succeed(ctx, repo, outboxRepo, dep, in)
		}
		return s.fail(ctx, repo, outboxRepo, dep, in)
	})
}

// succeed settles a successful report. The aggregate absorbs a redelivery of the
// same result, which announces nothing a second time.
func (s *Service) succeed(
	ctx context.Context,
	repo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	dep *model.Deployment,
	in CompleteInput,
) error {
	now := s.clock.Now()
	if settled(dep) {
		// A redelivered report of the result already recorded. The aggregate
		// accepts it, but the task is announced once.
		return dep.Complete(in.LeaseID, tokenHash(in.Token), in.Result, now)
	}
	exec := execution(dep, in.LeaseID, now)
	if err := dep.Complete(in.LeaseID, tokenHash(in.Token), in.Result, now); err != nil {
		return err
	}
	if err := repo.Save(ctx, dep); err != nil {
		return err
	}
	return s.fanout.Succeeded(ctx, outboxRepo, dep, exec, in.Result)
}

// fail applies a failed report through the fenced ReportFailure seam, which
// releases the task's execution slot as part of the transition it selects. The
// aggregate absorbs a redelivery of a permanent failure, which announces nothing
// a second time.
//
// A redelivered retryable failure returns ErrStaleLease instead: parking a task
// for requeue drops its lease, so the redelivery arrives on a task that carries
// no lease and may already have been re-claimed at a higher attempt. The
// executor cannot tell that report apart from one sent by a worker whose task
// was reaped and reassigned, so it fences both. ErrStaleLease is terminal for
// the report: the executor has already recorded the outcome, and the worker
// re-sending it can only be told to stop, never that the report was applied
// again.
//
// The execution is captured before the transition: parking a task for requeue
// drops the lease that identifies the execution being reported and holds when it
// started.
func (s *Service) fail(
	ctx context.Context,
	repo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	dep *model.Deployment,
	in CompleteInput,
) error {
	now := s.clock.Now()
	retryable := s.retryable(dep, in.Result)
	if settled(dep) {
		// A redelivered report of the result already recorded. The aggregate
		// accepts it, but the task is announced once.
		return dep.ReportFailure(in.LeaseID, tokenHash(in.Token), in.Result, retryable,
			now, s.retryBackoff)
	}
	exec := execution(dep, in.LeaseID, now)
	if err := dep.ReportFailure(in.LeaseID, tokenHash(in.Token), in.Result, retryable,
		now, s.retryBackoff); err != nil {
		return err
	}
	if err := repo.Save(ctx, dep); err != nil {
		return err
	}
	if retryable {
		return s.fanout.RetryableFailure(ctx, outboxRepo, dep, exec, in.Result)
	}
	return s.fanout.PermanentFailure(ctx, outboxRepo, dep, exec, in.Result)
}

// retryable decides whether a failed report earns another attempt. It narrows the
// worker's hint: the worker reports what it observed, but the executor owns the
// error-class policy and the retry budget. The final allowed attempt is permanent
// however transient the worker judged the failure.
//
// TaskMaxRetries counts retries, so a task runs its initial attempt plus up to
// that many more: TaskRetryCount is the current attempt's zero-based index, and
// another attempt is earned while it is below the retry budget. This mirrors the
// Jobs path, where k8s-controller retries a failed Job while retry_count <
// max_retries, so a task settles after the same total number of attempts on
// either path.
func (s *Service) retryable(dep *model.Deployment, result model.WorkerResult) bool {
	if !result.Retryable {
		return false
	}
	if unretryableErrorClasses[result.ErrorClass] {
		return false
	}
	cmd := dep.Command()
	return cmd.TaskRetryCount < cmd.TaskMaxRetries
}

// settled reports whether this task already recorded a terminal result, which is
// what a redelivered report finds.
func settled(dep *model.Deployment) bool {
	lease := dep.ActiveLease()
	return lease != nil && lease.FinishedAt != nil
}

// execution describes the worker run a terminal report announces: the lease that
// ran it, when that lease's dbt process started, and when the executor recorded
// its outcome. It is read before the transition the report drives, because that
// transition may drop the lease it reads from.
func execution(dep *model.Deployment, leaseID uuid.UUID, completedAt time.Time) tasklifecycle.Execution {
	exec := tasklifecycle.Execution{ID: leaseID, CompletedAt: &completedAt}
	if lease := dep.ActiveLease(); lease != nil && lease.StartedAt != nil {
		startedAt := *lease.StartedAt
		exec.StartedAt = &startedAt
	}
	return exec
}

// load reads the row a worker's report names, locked for the transition this
// transaction makes.
func (s *Service) load(
	ctx context.Context,
	repo repository.DeploymentRepository,
	id uuid.UUID,
) (*model.Deployment, error) {
	dep, err := repo.GetByIDForUpdate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load executor deployment %s: %w", id, err)
	}
	if dep == nil {
		return nil, fmt.Errorf("load executor deployment %s: %w", id, ErrNoSuchDeployment)
	}
	return dep, nil
}

// withTx runs fn inside one transaction, committing the aggregate's new state and
// the announcements it owes together. A failure rolls both back.
func (s *Service) withTx(
	ctx context.Context,
	fn func(repository.DeploymentRepository, outbox.Repository) error,
) error {
	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := u.Rollback(); err != nil {
			s.logger.Error("rollback failed", "error", err)
		}
	}()
	if err := fn(u.DeploymentsRepo(), u.OutboxRepo()); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// newToken mints a raw lease token: 32 bytes of randomness, URL-safe so it rides
// an HTTP header unencoded.
func newToken() (string, error) {
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// tokenHash digests a raw lease token. Only the digest is ever stored, so a read
// of the row cannot recover a token that would authorize a transition.
func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
