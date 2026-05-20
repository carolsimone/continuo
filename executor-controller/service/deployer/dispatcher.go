// Package deployer holds the application service that drains the
// executor_deployments command queue: it deploys K8s Jobs (capped by a live
// in-flight count) and, once a deploy resolves, writes the canonical
// announcement rows to executor_outbox. It depends only on domain ports.
package deployer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RepoFactory builds a DeploymentRepository bound to a specific executor (the
// *sqlx.Tx the dispatcher opens per batch). Injecting it keeps the concrete
// Postgres adapter out of this package.
type RepoFactory func(exec outbox.Executor) repository.DeploymentRepository

// DispatcherConfig groups the optional knobs.
type DispatcherConfig struct {
	Tick        time.Duration // poll interval; default 5s
	BatchSize   int           // max rows per batch (also clamped by headroom); default 50
	BackoffBase time.Duration // first retry delay; default 5s
	BackoffCap  time.Duration // max retry delay; default 2m
}

// Dispatcher drains executor_deployments under a concurrency cap. The K8s
// deploy is a command effect kept off the outbox so every outbox Publisher
// stays a uniform marshal-and-XADD.
type Dispatcher struct {
	db            *sqlx.DB
	deployer      deploy.Deployer
	newRepo       RepoFactory
	maxConcurrent int
	logger        *slog.Logger
	tick          time.Duration
	batchSize     int
	backoff       model.BackoffPolicy
	now           func() time.Time
}

func NewDispatcher(
	db *sqlx.DB,
	deployer deploy.Deployer,
	newRepo RepoFactory,
	maxConcurrent int,
	logger *slog.Logger,
	cfg DispatcherConfig,
) *Dispatcher {
	if cfg.Tick == 0 {
		cfg.Tick = 5 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 50
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffCap == 0 {
		cfg.BackoffCap = 2 * time.Minute
	}
	return &Dispatcher{
		db:            db,
		deployer:      deployer,
		newRepo:       newRepo,
		maxConcurrent: maxConcurrent,
		logger:        logger,
		tick:          cfg.Tick,
		batchSize:     cfg.BatchSize,
		backoff:       model.BackoffPolicy{Base: cfg.BackoffBase, Cap: cfg.BackoffCap},
		now:           time.Now,
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.tick)
	defer ticker.Stop()
	d.logger.Info("Starting deploy dispatcher", "tick", d.tick, "max_concurrent", d.maxConcurrent)
	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Deploy dispatcher stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := d.ProcessBatch(ctx); err != nil {
				d.logger.Error("Deploy dispatch batch failed", "error", err)
			}
		}
	}
}

// ProcessBatch runs one cycle. The concurrency cap is evaluated once, then up
// to headroom deployments are processed — each in its OWN transaction so a
// failure on one deployment never rolls back another, and the K8s deploy holds
// only a single row's lock. Exported for tests.
func (d *Dispatcher) ProcessBatch(ctx context.Context) error {
	active, err := d.deployer.CountActive(ctx)
	if err != nil {
		return fmt.Errorf("count active deploys: %w", err)
	}
	headroom := d.maxConcurrent - active
	if headroom <= 0 {
		d.logger.Info("Deploy cap reached — deferring pending deployments",
			"active", active, "max_concurrent", d.maxConcurrent)
		return nil
	}
	if headroom > d.batchSize {
		headroom = d.batchSize
	}

	for i := 0; i < headroom; i++ {
		processed, err := d.processOne(ctx)
		if err != nil {
			return fmt.Errorf("process deployment: %w", err)
		}
		if !processed {
			break // no more due deployments this cycle
		}
	}
	return nil
}

// processOne claims and processes at most one due deployment inside its own
// transaction. It returns false when no due deployment is available.
func (d *Dispatcher) processOne(ctx context.Context) (bool, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	repo := d.newRepo(tx)
	outboxRepo := outbox.NewPostgresRepository(tx, "executor_outbox", d.logger)

	due, err := repo.GetDueBatch(ctx, 1)
	if err != nil {
		return false, fmt.Errorf("get due deployment: %w", err)
	}
	if len(due) == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit empty tx: %w", err)
		}
		committed = true
		return false, nil
	}

	if err := d.dispatchOne(ctx, repo, outboxRepo, due[0]); err != nil {
		return false, fmt.Errorf("dispatch deployment %s: %w", due[0].ID(), err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return true, nil
}

func (d *Dispatcher) dispatchOne(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, dep *model.Deployment) error {
	now := d.now()

	// A row whose job_params could not be deserialized is unrunnable; fail it
	// permanently with a routable announcement built from its recovered identity.
	if !dep.IsDeployable() {
		dep.RegisterFailure(now, true, "deployment job_params not deployable", d.backoff)
		if err := d.writeFailedAnnouncements(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	deployErr := d.deployer.Deploy(ctx, dep.Command().ToJobSpec())
	if deployErr == nil {
		if err := d.writeDeployedAnnouncements(ctx, outboxRepo, dep); err != nil {
			return err
		}
		if err := dep.MarkDeployed(now); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	if dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff) {
		d.logger.Error("Deploy terminal failure", "deployment_id", dep.ID(), "cause", deployErr)
		if err := d.writeFailedAnnouncements(ctx, outboxRepo, dep); err != nil {
			return err
		}
	} else {
		d.logger.Warn("Deploy transient failure — rescheduling",
			"deployment_id", dep.ID(), "retry_count", dep.RetryCount(), "next_attempt_at", dep.NextAttemptAt(), "error", deployErr)
	}
	return repo.Save(ctx, dep)
}

func (d *Dispatcher) writeDeployedAnnouncements(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	cmd := dep.Command()
	if err := d.createOutbox(ctx, outboxRepo, dep, "task_status_updated", streams.TaskStatusUpdatedV1,
		pkgevents.TaskStatusUpdated{TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: "RUNNING", RetryCount: int32(cmd.TaskRetryCount)}); err != nil {
		return fmt.Errorf("write RUNNING announcement: %w", err)
	}
	deployed := event.JobDeployed{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
		JobName: cmd.JobName, NodeType: cmd.NodeType, ImageTag: cmd.ImageTag,
		TaskRetryCount: cmd.TaskRetryCount, MaxRetries: cmd.TaskMaxRetries,
	}
	if err := d.createOutbox(ctx, outboxRepo, dep, "node_deployed", streams.NodeDeployedV1, deployed); err != nil {
		return fmt.Errorf("write node_deployed announcement: %w", err)
	}
	return nil
}

func (d *Dispatcher) writeFailedAnnouncements(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	cmd := dep.Command()
	if err := d.createOutbox(ctx, outboxRepo, dep, "task_status_updated", streams.TaskStatusUpdatedV1,
		pkgevents.TaskStatusUpdated{TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: "FAILED", RetryCount: int32(cmd.TaskRetryCount)}); err != nil {
		return fmt.Errorf("write FAILED task_status announcement: %w", err)
	}
	nodeFailed := event.NodeUpdated{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName, Status: "FAILED",
	}
	if err := d.createOutbox(ctx, outboxRepo, dep, "node_updated", streams.NodeUpdatedV1, nodeFailed); err != nil {
		return fmt.Errorf("write FAILED node_updated announcement: %w", err)
	}
	return nil
}

func (d *Dispatcher) createOutbox(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment, eventType, stream string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	aggregateID, _ := uuid.Parse(dep.Command().TaskID)
	return outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: dep.MessageProcessingID(),
		AggregateType:       "task",
		AggregateID:         aggregateID,
		EventType:           eventType,
		Payload:             body,
		StreamName:          stream,
		MaxRetries:          3,
	})
}
