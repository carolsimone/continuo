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
	"github.com/carolsimone/continuo/executor-controller/service/validation"
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

// ValidationAggRepoFactory builds a ValidationAggregateRepository bound to the
// per-batch executor, mirroring RepoFactory so the concrete Postgres sentinel
// adapter stays out of this package.
type ValidationAggRepoFactory func(exec outbox.Executor) repository.ValidationAggregateRepository

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
	newAggRepo    ValidationAggRepoFactory
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
	newAggRepo ValidationAggRepoFactory,
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
		newAggRepo:    newAggRepo,
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
	aggRepo := d.newAggRepo(tx)
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

	if err := d.dispatchOne(ctx, repo, outboxRepo, aggRepo, due[0]); err != nil {
		return false, fmt.Errorf("dispatch deployment %s: %w", due[0].ID(), err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return true, nil
}

func (d *Dispatcher) dispatchOne(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment) error {
	if dep.Mode() == model.ModeValidation {
		return d.dispatchValidation(ctx, repo, outboxRepo, aggRepo, dep)
	}
	if dep.Mode() == model.ModeSeedBuild {
		return d.dispatchSeedBuild(ctx, repo, outboxRepo, aggRepo, dep)
	}
	if dep.Mode() == model.ModeCompile {
		return d.dispatchCompile(ctx, repo, outboxRepo, aggRepo, dep)
	}

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

// dispatchValidation handles a mode=validation row. On success it marks the row
// deployed and emits a node.deployed:v1 trigger so k8s-controller status-checks
// the validation Job (it never polls); it skips the production-only
// task_status_updated announcement. The per-node terminal outcome ("ok"/"failed")
// arrives later via validation.node.completed:v1 and is attached by the outcome
// handler, which then triggers the aggregate emit. A validation row that cannot be dispatched
// (not deployable, or a permanent pre-deploy deployer error) is failed terminally
// here via FailValidation — which sets a "failed" outcome from pending without
// requiring StatusDeployed — and we then settle the node: its blocked descendants
// are skipped and the per-release aggregate is emitted (under the advisory lock),
// so a release whose node fails at dispatch never strands its downstream rows in
// "blocked" and still announces its (failed) result.
func (d *Dispatcher) dispatchValidation(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment) error {
	now := d.now()

	if !dep.IsDeployable() {
		if err := dep.FailValidation("validation deployment not deployable", now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate. The gate reads
		// PendingValidationCount and must see this node as terminal (its own
		// uncommitted write is visible within this transaction); otherwise it
		// counts the row as still pending, skips the emission, and no later
		// validation.node.completed event will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedValidation(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeployValidation(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkDeployed(now); err != nil {
			return err
		}
		if err := d.writeValidationDeployedTrigger(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	if dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff) { // terminal
		d.logger.Error("Validation deploy terminal failure",
			"deployment_id", dep.ID(), "release_id", dep.ReleaseID(), "node_id", dep.NodeID(), "cause", deployErr)
		if err := dep.FailValidation(deployErr.Error(), now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate so PendingValidationCount
		// sees this node as terminal (see the not-deployable branch above).
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedValidation(ctx, repo, outboxRepo, aggRepo, dep, now)
	}
	d.logger.Warn("Validation deploy transient failure — rescheduling",
		"deployment_id", dep.ID(), "retry_count", dep.RetryCount(), "next_attempt_at", dep.NextAttemptAt(), "error", deployErr)
	return repo.Save(ctx, dep)
}

// settleFailedValidation settles a validation node that failed AT dispatch: it
// runs the shared per-release gating-propagation + aggregate-emit gate under the
// per-release advisory lock so the failed node's blocked descendants are skipped
// and the aggregate can fire. The logic lives in service/validation so the
// validation.node.completed:v1 handler runs the identical gate under its own
// Unit-of-Work. The failed node's own outcome is already persisted before this
// call; "failed" drives the transitive skip of its blocked downstreams.
func (d *Dispatcher) settleFailedValidation(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment, now time.Time) error {
	return validation.SettleNodeTerminal(
		ctx, repo, outboxRepo, aggRepo, validation.DedupNamespace,
		dep.ReleaseID(), dep.NodeID(), "failed", now)
}

// dispatchSeedBuild handles a mode=seed_build row. It is structurally identical
// to dispatchValidation: on success it marks the row deployed and emits a
// node.deployed:v1 trigger so k8s-controller status-checks the seed-build Job;
// on terminal failure it fails the row and settles the per-release seed-build
// aggregate. Seeds are flat roots with no blocked downstreams so
// SettleSeedBuildNodeTerminal's propagateGating call is a no-op — but the
// aggregate gate still fires to emit seed.build.completed:v1.
func (d *Dispatcher) dispatchSeedBuild(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment) error {
	now := d.now()

	if !dep.IsDeployable() {
		if err := dep.FailSeedBuild("seed-build deployment not deployable", now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate. The gate reads
		// PendingValidationCount and must see this node as terminal (its own
		// uncommitted write is visible within this transaction); otherwise it
		// counts the row as still pending, skips the emission, and no later
		// seed.build.node.completed event will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedSeedBuild(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeploySeedBuild(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkDeployed(now); err != nil {
			return err
		}
		if err := d.writeValidationDeployedTrigger(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	if dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff) { // terminal
		d.logger.Error("Seed-build deploy terminal failure",
			"deployment_id", dep.ID(), "release_id", dep.ReleaseID(), "node_id", dep.NodeID(), "cause", deployErr)
		if err := dep.FailSeedBuild(deployErr.Error(), now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate so PendingValidationCount
		// sees this node as terminal (see the not-deployable branch above).
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedSeedBuild(ctx, repo, outboxRepo, aggRepo, dep, now)
	}
	d.logger.Warn("Seed-build deploy transient failure — rescheduling",
		"deployment_id", dep.ID(), "retry_count", dep.RetryCount(), "next_attempt_at", dep.NextAttemptAt(), "error", deployErr)
	return repo.Save(ctx, dep)
}

// settleFailedSeedBuild settles a seed-build node that failed AT dispatch: it
// runs the per-release aggregate-emit gate under the per-release advisory lock
// so the aggregate can fire. Seeds are flat roots so propagateGating is a no-op,
// but the aggregate gate still emits seed.build.completed:v1 once all rows
// for the release are terminal. The failed node's own outcome is already
// persisted before this call.
func (d *Dispatcher) settleFailedSeedBuild(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment, now time.Time) error {
	return validation.SettleSeedBuildNodeTerminal(
		ctx, repo, outboxRepo, aggRepo,
		dep.ReleaseID(), dep.NodeID(), "failed", now)
}

// dispatchCompile handles a mode=compile row. It is structurally identical to
// dispatchSeedBuild: on success it marks the row deployed and emits a
// node.deployed:v1 trigger so k8s-controller status-checks the compile Job; on
// terminal failure it fails the row and settles the per-release compile
// aggregate via SettleCompileNodeTerminal. Compile is a single root node (no
// in-leg upstreams) so the gating propagation in SettleCompileNodeTerminal is a
// no-op — but the aggregate gate fires and emits compile.completed:v1 via
// compileEmit (wired in A6/A7).
func (d *Dispatcher) dispatchCompile(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment) error {
	now := d.now()

	if !dep.IsDeployable() {
		if err := dep.FailCompile("compile deployment not deployable", now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate. The gate reads
		// PendingValidationCount and must see this node as terminal (its own
		// uncommitted write is visible within this transaction); otherwise it
		// counts the row as still pending, skips the emission, and no later
		// compile.node.completed event will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedCompile(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeployCompile(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkDeployed(now); err != nil {
			return err
		}
		if err := d.writeValidationDeployedTrigger(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	if dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff) { // terminal
		d.logger.Error("Compile deploy terminal failure",
			"deployment_id", dep.ID(), "release_id", dep.ReleaseID(), "node_id", dep.NodeID(), "cause", deployErr)
		if err := dep.FailCompile(deployErr.Error(), now); err != nil {
			return err
		}
		// Persist outcome='failed' BEFORE the aggregate gate so PendingValidationCount
		// sees this node as terminal (see the not-deployable branch above).
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedCompile(ctx, repo, outboxRepo, aggRepo, dep, now)
	}
	d.logger.Warn("Compile deploy transient failure — rescheduling",
		"deployment_id", dep.ID(), "retry_count", dep.RetryCount(), "next_attempt_at", dep.NextAttemptAt(), "error", deployErr)
	return repo.Save(ctx, dep)
}

// settleFailedCompile settles a compile node that failed AT dispatch: it runs
// the per-release aggregate-emit gate under the per-release advisory lock so the
// aggregate can fire. Compile is a single root node so propagateGating is a
// no-op, but the aggregate gate runs and emits compile.completed:v1 via
// SettleCompileNodeTerminal. The failed node's own outcome is already
// persisted before this call.
func (d *Dispatcher) settleFailedCompile(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment, now time.Time) error {
	return validation.SettleCompileNodeTerminal(
		ctx, repo, outboxRepo, aggRepo,
		dep.ReleaseID(), dep.NodeID(), "failed", now)
}

func (d *Dispatcher) writeDeployedAnnouncements(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	cmd := dep.Command()
	// The deploy path emits only the node_deployed trigger that starts k8s polling.
	// k8s-controller is the sole producer of the running/terminal pod lifecycle and
	// announces RUNNING the first time it observes the Job running.
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

// writeValidationDeployedTrigger emits the node.deployed:v1 trigger after a
// validation Job is created. k8s-controller is event-driven and never polls, so
// without this row the validation Job would never be status-checked and the
// release would hang in "validating". This is NOT the production success path:
// it writes only the node_deployed check trigger and skips the production-only
// task_status_updated / RUNNING announcement (validation rows have no real
// task/schedule, so there is no UI task status to advance). The per-node
// terminal outcome still arrives later via validation.node.completed:v1.
//
// The trigger carries the deterministic synthetic task/schedule UUIDs derived
// from (release_id, node_id). They are inert carriers for validation: the
// k8s-controller routes the resulting check by the Job's mode=validation label,
// not by these IDs — they only need to be valid UUIDs so ParseNodeDeployed
// accepts the message, and to satisfy the outbox AggregateID.
func (d *Dispatcher) writeValidationDeployedTrigger(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	vc := dep.ValidationCommand()
	taskID, scheduleID := model.ValidationSyntheticIDs(dep.ReleaseID(), dep.NodeID())
	deployed := event.JobDeployed{
		TaskID:         taskID.String(),
		ScheduleID:     scheduleID.String(),
		ScheduleName:   "", // validation has no schedule
		ServiceName:    vc.ServiceName,
		SchemaName:     vc.SchemaName,
		TableName:      vc.TableName,
		JobName:        vc.JobName,
		NodeType:       vc.NodeType,
		ImageTag:       vc.ImageTag,
		TaskRetryCount: 0,
		MaxRetries:     0,
	}
	body, err := json.Marshal(deployed)
	if err != nil {
		return fmt.Errorf("marshal validation node_deployed payload: %w", err)
	}
	if err := outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: dep.MessageProcessingID(),
		AggregateType:       "task",
		AggregateID:         taskID,
		EventType:           "node_deployed",
		Payload:             body,
		StreamName:          streams.NodeDeployedV1,
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write validation node_deployed trigger: %w", err)
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
