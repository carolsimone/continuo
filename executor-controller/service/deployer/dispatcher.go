// Package deployer holds the application service that drains the
// executor_deployments command queue: it deploys K8s Jobs (capped by the
// execution slots it reserves) and, once a deploy resolves, writes the canonical
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
	"github.com/carolsimone/continuo/executor-controller/service/tasklifecycle"
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
	Tick time.Duration // poll interval; default 5s
	// BatchSize caps the rows one batch may dispatch. Zero resolves to the
	// executor's concurrency cap, which is the most a batch could ever start.
	BatchSize   int
	BackoffBase time.Duration // first retry delay; default 5s
	BackoffCap  time.Duration // max retry delay; default 2m
	// DispatchRecoveryAfter is how long a reservation may sit in 'dispatching'
	// before a tick re-drives it; default 2m. It must exceed the time a healthy
	// Job create takes, so a dispatch that is merely slow is not re-driven
	// alongside the one still working on it.
	DispatchRecoveryAfter time.Duration
}

// Dispatcher drains executor_deployments under a concurrency cap. The K8s
// deploy is a command effect kept off the outbox so every outbox Publisher
// stays a uniform marshal-and-XADD.
type Dispatcher struct {
	db              *sqlx.DB
	deployer        deploy.Deployer
	newRepo         RepoFactory
	newAggRepo      ValidationAggRepoFactory
	maxConcurrent   int
	logger          *slog.Logger
	tick            time.Duration
	batchSize       int
	backoff         model.BackoffPolicy
	recoveryAfter   time.Duration
	now             func() time.Time
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
	// A batch can never start more work than the concurrency cap allows, so the
	// cap is the natural bound. It is configured, never defaulted to a literal.
	if cfg.BatchSize == 0 {
		cfg.BatchSize = maxConcurrent
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffCap == 0 {
		cfg.BackoffCap = 2 * time.Minute
	}
	if cfg.DispatchRecoveryAfter == 0 {
		cfg.DispatchRecoveryAfter = 2 * time.Minute
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
		recoveryAfter: cfg.DispatchRecoveryAfter,
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

// ProcessBatch runs one cycle: it re-drives one stranded reservation, then
// dispatches up to batchSize deployments. Each deployment takes its slot in one
// transaction, creates its Kubernetes Job outside any transaction, and records
// the outcome in a second — so a slow Job create never holds a row lock, and a
// crash between the two cannot lose track of a Job that is already running.
// Exported for tests.
func (d *Dispatcher) ProcessBatch(ctx context.Context) error {
	if err := d.recoverOneStaleDispatch(ctx); err != nil {
		return fmt.Errorf("recover stale dispatch: %w", err)
	}

	for i := 0; i < d.batchSize; i++ {
		processed, err := d.processOne(ctx)
		if err != nil {
			return fmt.Errorf("process deployment: %w", err)
		}
		if !processed {
			break // no headroom, or no more due deployments this cycle
		}
	}
	return nil
}

// recoverOneStaleDispatch re-drives a single reservation left in 'dispatching'
// by a crash between the Job create and the transaction that records it. The
// slot stays reserved throughout: the Job may well be running, and releasing the
// slot to re-reserve it would let the executor start work beyond its cap in the
// meantime. The Job create is idempotent by name, so repeating it either adopts
// the existing Job or creates the one the crash lost.
func (d *Dispatcher) recoverOneStaleDispatch(ctx context.Context) error {
	dep, err := d.claimStaleDispatch(ctx)
	if err != nil || dep == nil {
		return err
	}
	d.logger.Warn("Re-driving a dispatch stranded before it was recorded",
		"deployment_id", dep.ID(), "mode", dep.Mode())
	return d.deployAndSettle(ctx, dep)
}

// claimStaleDispatch returns one deployment whose reservation has sat in
// 'dispatching' past the recovery window, or nil when none has. The row lock is
// released at commit rather than held across the Job create; two dispatchers
// that both re-drive the same row create the same idempotent Job and apply the
// same transition, so the race is benign.
func (d *Dispatcher) claimStaleDispatch(ctx context.Context) (*model.Deployment, error) {
	var dep *model.Deployment
	err := d.inTx(ctx, func(repo repository.DeploymentRepository, _ outbox.Repository, _ repository.ValidationAggregateRepository) error {
		var err error
		dep, err = repo.GetStaleDispatchingForUpdate(ctx, d.now().Add(-d.recoveryAfter))
		return err
	})
	if err != nil {
		return nil, err
	}
	return dep, nil
}

// processOne reserves and dispatches at most one due deployment. It returns
// false when the executor has no spare capacity or no deployment is due.
func (d *Dispatcher) processOne(ctx context.Context) (bool, error) {
	dep, err := d.reserveOne(ctx)
	if err != nil {
		return false, err
	}
	if dep == nil {
		return false, nil
	}
	if err := d.deployAndSettle(ctx, dep); err != nil {
		return false, fmt.Errorf("dispatch deployment %s: %w", dep.ID(), err)
	}
	return true, nil
}

// reserveOne takes an execution slot for one due deployment and parks it in
// 'dispatching'. The capacity lock is held for the whole transaction, so the
// count it reads cannot be stale by the time the slot is taken — that
// serialization is what keeps Jobs and worker claims from both spending the same
// free slot. It returns nil when the cap is reached or nothing is due.
func (d *Dispatcher) reserveOne(ctx context.Context) (*model.Deployment, error) {
	var reserved *model.Deployment
	err := d.inTx(ctx, func(repo repository.DeploymentRepository, _ outbox.Repository, _ repository.ValidationAggregateRepository) error {
		if err := repo.LockCapacity(ctx); err != nil {
			return err
		}
		active, err := repo.ActiveSlotCount(ctx)
		if err != nil {
			return fmt.Errorf("count active execution slots: %w", err)
		}
		if active >= d.maxConcurrent {
			d.logger.Info("Execution cap reached — deferring pending deployments",
				"active", active, "max_concurrent", d.maxConcurrent)
			return nil
		}
		due, err := repo.GetDueJobs(ctx, 1)
		if err != nil {
			return fmt.Errorf("get due deployment: %w", err)
		}
		if len(due) == 0 {
			return nil
		}
		dep := due[0]
		if err := dep.ReserveForDispatch(d.now()); err != nil {
			return fmt.Errorf("reserve execution slot for deployment %s: %w", dep.ID(), err)
		}
		if err := repo.Save(ctx, dep); err != nil {
			return fmt.Errorf("save reserved deployment %s: %w", dep.ID(), err)
		}
		reserved = dep
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reserved, nil
}

// deployAndSettle creates the deployment's Kubernetes Job outside any
// transaction, then records the outcome in one. Every settle path releases the
// reserved slot or leaves the Job to release it on its terminal status.
func (d *Dispatcher) deployAndSettle(ctx context.Context, dep *model.Deployment) error {
	return d.inTx(ctx, func(repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository) error {
		return d.dispatchOne(ctx, repo, outboxRepo, aggRepo, dep)
	})
}

// inTx runs fn against repositories bound to a single transaction, committing
// when it returns nil and rolling back otherwise.
func (d *Dispatcher) inTx(
	ctx context.Context,
	fn func(repository.DeploymentRepository, outbox.Repository, repository.ValidationAggregateRepository) error,
) error {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := fn(
		d.newRepo(tx),
		outbox.NewPostgresRepository(tx, "executor_outbox", d.logger),
		d.newAggRepo(tx),
	); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return nil
}

// jobSpec projects a production deployment onto the Deployer port's spec,
// naming the row that holds the Job's execution slot so its terminal status can
// release it, and pinning the command when the row already resolved one.
func jobSpec(dep *model.Deployment) deploy.JobSpec {
	spec := dep.Command().ToJobSpec()
	spec.ExecutorDeploymentID = dep.ID().String()
	spec.ResolvedArgv = dep.ResolvedArgv()
	return spec
}

// validationJobSpec projects a validation, seed-build, or compile deployment
// onto the Deployer port's spec, naming the row that holds the Job's execution
// slot so its terminal status can release it.
func validationJobSpec(dep *model.Deployment) deploy.ValidationJobSpec {
	spec := dep.ValidationCommand().ToValidationJobSpec()
	spec.ExecutorDeploymentID = dep.ID().String()
	return spec
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
	isPromoteSeed := dep.Command().Mode == pkgevents.ModePromoteSeed

	// A row whose job_params could not be deserialized is unrunnable; fail it
	// permanently with a routable announcement built from its recovered identity.
	// promote_seed is fire-and-forget: no state run to load, so skip announcements.
	if !dep.IsDeployable() {
		dep.RegisterFailure(now, true, "deployment job_params not deployable", d.backoff)
		if !isPromoteSeed {
			if err := d.writeFailedAnnouncements(ctx, outboxRepo, dep); err != nil {
				return err
			}
		}
		return repo.Save(ctx, dep)
	}

	deployErr := d.deployer.Deploy(ctx, jobSpec(dep))
	if deployErr == nil {
		// Every mode emits the node_deployed trigger, promote_seed included:
		// k8s-controller never polls, so without it the Job would never be
		// status-checked and the execution slot it holds would never be
		// released. The trigger starts observation only; the state-bound
		// lifecycle events stay suppressed for promote_seed further down the
		// chain, keyed off the mode this trigger carries.
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
		if !isPromoteSeed {
			if err := d.writeFailedAnnouncements(ctx, outboxRepo, dep); err != nil {
				return err
			}
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

	deployErr := d.deployer.DeployValidation(ctx, validationJobSpec(dep))
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

	deployErr := d.deployer.DeploySeedBuild(ctx, validationJobSpec(dep))
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

	deployErr := d.deployer.DeployCompile(ctx, validationJobSpec(dep))
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
	// announces RUNNING the first time it observes the Job running. The trigger
	// names this row and its mode so the Job's terminal check can release the
	// execution slot and route the result without re-reading the Job.
	deployed := event.JobDeployed{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
		JobName: cmd.JobName, NodeType: cmd.NodeType, ImageTag: cmd.ImageTag,
		Operation:            cmd.Operation,
		ExecutorDeploymentID: dep.ID().String(),
		Mode:                 cmd.Mode,
		RuntimeManifestRef:   cmd.RuntimeManifestRef,
		TaskRetryCount:       cmd.TaskRetryCount, MaxRetries: cmd.TaskMaxRetries,
	}
	if err := d.createOutbox(ctx, outboxRepo, dep, "node_deployed", streams.NodeDeployedV1, deployed); err != nil {
		return fmt.Errorf("write node_deployed announcement: %w", err)
	}
	return nil
}

// writeValidationDeployedTrigger emits the node.deployed:v1 trigger after a
// validation, seed-build, or compile Job is created. k8s-controller is
// event-driven and never polls, so without this row the Job would never be
// status-checked and the release would hang. This is NOT the production success
// path: it writes only the node_deployed check trigger and skips the
// production-only task_status_updated / RUNNING announcement (these rows have
// no real task/schedule, so there is no UI task status to advance). The
// per-node terminal outcome still arrives later via the mode-specific
// node.completed:v1 event.
//
// The trigger carries the deterministic synthetic task/schedule UUIDs derived
// from (release_id, node_id). They are inert carriers: k8s-controller routes
// the resulting check by the Job's own mode label (mode=validation,
// mode=seed_build, or mode=compile), not by these IDs — they only need to be
// valid UUIDs so ParseNodeDeployed accepts the message, and to satisfy the
// outbox AggregateID.
func (d *Dispatcher) writeValidationDeployedTrigger(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	vc := dep.ValidationCommand()
	taskID, scheduleID := model.ValidationSyntheticIDs(dep.ReleaseID(), dep.NodeID())
	deployed := event.JobDeployed{
		TaskID:               taskID.String(),
		ScheduleID:           scheduleID.String(),
		ScheduleName:         "", // validation has no schedule
		ServiceName:          vc.ServiceName,
		SchemaName:           vc.SchemaName,
		TableName:            vc.TableName,
		JobName:              vc.JobName,
		NodeType:             vc.NodeType,
		ImageTag:             vc.ImageTag,
		ExecutorDeploymentID: dep.ID().String(),
		Mode:                 string(dep.Mode()),
		TaskRetryCount:       0,
		MaxRetries:           0,
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

// writeFailedAnnouncements settles a production task the executor will never
// run. It is reached three ways: the row is undeployable, its deploy failed
// permanently, or its deploy exhausted its attempt budget. A task that will
// never run announces the same terminal pair however it got there, so this
// shares the fanout with the rejection path rather than shaping the two events a
// second time.
func (d *Dispatcher) writeFailedAnnouncements(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	reason := ""
	if msg := dep.ErrorMessage(); msg != nil {
		reason = *msg
	}
	return tasklifecycle.Fanout{}.DispatchRejected(ctx, outboxRepo, dep, reason)
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
