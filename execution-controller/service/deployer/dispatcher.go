// Package deployer holds the application service that drains the deployments
// command queue: it admits queued deployments to execution slots under a
// concurrency cap, creates their K8s Jobs and, once a deploy resolves, writes
// the canonical announcement rows to execution_outbox. It depends only on
// domain ports.
package deployer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RepoFactory builds a DeploymentRepository bound to a specific executor (the
// *sqlx.Tx the dispatcher opens per launch). Injecting it keeps the concrete
// Postgres adapter out of this package.
type RepoFactory func(exec outbox.Executor) repository.DeploymentRepository

// ValidationAggRepoFactory builds a ValidationAggregateRepository bound to the
// per-launch executor, mirroring RepoFactory so the concrete Postgres sentinel
// adapter stays out of this package.
type ValidationAggRepoFactory func(exec outbox.Executor) repository.ValidationAggregateRepository

// AdmissionRepoFactory builds an AdmissionRepository bound to the executor of a
// claim or launch transaction, keeping the concrete Postgres adapter out of
// this package.
type AdmissionRepoFactory func(exec outbox.Executor) repository.AdmissionRepository

// DispatcherConfig groups the optional knobs.
type DispatcherConfig struct {
	Tick        time.Duration // fallback poll interval; default 5s
	BatchSize   int           // max deployments reserved per claim; default 50
	BackoffBase time.Duration // first retry delay; default 5s
	BackoffCap  time.Duration // max retry delay; default 2m
	// CheckDelay is the cadence between two status checks of a Job that is
	// still running; default 10s. The job-status handler owns the re-checks,
	// the dispatcher only carries the value so both sides read one setting.
	CheckDelay time.Duration
	// FirstCheckDelay is how long after a Job is created its first status
	// check is due; default 1s. The first check is what announces RUNNING, so
	// it must land before a fast Job completes or the task is never seen
	// running.
	FirstCheckDelay time.Duration
	// Waker wakes the dispatcher when a deployment is accepted or a slot is
	// released; Tick is then the fallback for retries coming due and for
	// notifications lost while the listener reconnects. Without a Waker the
	// dispatcher runs only on Tick.
	Waker outbox.Waker
}

// Dispatcher admits deployments under a concurrency cap. The K8s deploy is a
// command effect kept off the outbox so every outbox Publisher stays a uniform
// marshal-and-XADD.
type Dispatcher struct {
	db              *sqlx.DB
	deployer        deploy.Deployer
	newRepo         RepoFactory
	newAggRepo      ValidationAggRepoFactory
	newAdmission    AdmissionRepoFactory
	waker           outbox.Waker
	maxConcurrent   int
	logger          *slog.Logger
	tick            time.Duration
	batchSize       int
	backoff         model.BackoffPolicy
	now             func() time.Time
	checkDelay      time.Duration
	firstCheckDelay time.Duration
}

func NewDispatcher(
	db *sqlx.DB,
	deployer deploy.Deployer,
	newRepo RepoFactory,
	newAggRepo ValidationAggRepoFactory,
	newAdmission AdmissionRepoFactory,
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
	if cfg.CheckDelay == 0 {
		cfg.CheckDelay = 10 * time.Second
	}
	if cfg.FirstCheckDelay == 0 {
		cfg.FirstCheckDelay = 1 * time.Second
	}
	return &Dispatcher{
		db:              db,
		deployer:        deployer,
		newRepo:         newRepo,
		newAggRepo:      newAggRepo,
		newAdmission:    newAdmission,
		waker:           cfg.Waker,
		maxConcurrent:   maxConcurrent,
		logger:          logger,
		tick:            cfg.Tick,
		batchSize:       cfg.BatchSize,
		backoff:         model.BackoffPolicy{Base: cfg.BackoffBase, Cap: cfg.BackoffCap},
		now:             time.Now,
		checkDelay:      cfg.CheckDelay,
		firstCheckDelay: cfg.FirstCheckDelay,
	}
}

// Run admits deployments when it starts, on every wake and on every tick, until
// ctx is done.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.tick)
	defer ticker.Stop()
	var wake <-chan struct{}
	if d.waker != nil {
		wake = d.waker.Wake()
	}
	d.logger.Info("Starting deploy dispatcher", "tick", d.tick, "max_concurrent", d.maxConcurrent)
	for {
		if err := d.ProcessBatch(ctx); err != nil && ctx.Err() == nil {
			d.logger.Error("Deploy dispatch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			d.logger.Info("Deploy dispatcher stopped")
			return ctx.Err()
		case <-ticker.C:
		case <-wake:
		}
	}
}

// ProcessBatch claims free slots for due deployments and launches each claimed
// deployment, repeating while a claim fills a whole batch. Exported for tests.
func (d *Dispatcher) ProcessBatch(ctx context.Context) error {
	for {
		ids, err := d.claim(ctx)
		if err != nil {
			return fmt.Errorf("claim deployments: %w", err)
		}
		var errs []error
		for _, id := range ids {
			if err := d.launch(ctx, id); err != nil {
				errs = append(errs, fmt.Errorf("launch deployment %s: %w", id, err))
			}
		}
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		if len(ids) < d.batchSize {
			return nil
		}
	}
}

// claim reserves slots in one transaction: it locks the global capacity
// record, counts the deployments holding a slot, and reserves up to the
// headroom in fair order. Every dispatcher serialises on that lock, so together
// they never hold more than maxConcurrent slots. The lock is released at
// commit, before any Job is created.
func (d *Dispatcher) claim(ctx context.Context) ([]uuid.UUID, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	adm := d.newAdmission(tx)
	if err := adm.LockScope(ctx, model.ScopeGlobal); err != nil {
		return nil, err
	}
	inFlight, err := adm.CountInFlight(ctx)
	if err != nil {
		return nil, err
	}
	n := model.Headroom(d.maxConcurrent, inFlight, d.batchSize)
	var ids []uuid.UUID
	if n > 0 {
		if ids, err = adm.ReserveNext(ctx, n); err != nil {
			return nil, err
		}
	} else {
		d.logger.Debug("Deploy cap reached — deployments wait for a slot",
			"in_flight", inFlight, "max_concurrent", d.maxConcurrent)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return ids, nil
}

// launch creates the Job of one reserved deployment in its own transaction,
// so a failure on one deployment never rolls back another and the Kubernetes
// call holds only that row's lock. It returns nil when the deployment is no
// longer reserved or another launcher holds it. Job names are deterministic
// and an existing Job counts as created, so launching a deployment whose Job
// an interrupted launch already created is safe.
func (d *Dispatcher) launch(ctx context.Context, id uuid.UUID) error {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	dep, err := d.newAdmission(tx).GetReserved(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	repo := d.newRepo(tx)
	outboxRepo := outbox.NewPostgresRepository(tx, "execution_outbox", d.logger)
	if err := d.dispatchOne(ctx, repo, outboxRepo, d.newAggRepo(tx), dep); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit launch: %w", err)
	}
	return nil
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

	// Legacy promote-seed work queued by a previous version carries no run in
	// state, so announcing its lifecycle would address a task state cannot load
	// and wedge that consumer. Current promoted-seed work is an ordinary run and
	// never sets this. See events.ModePromoteSeed.
	isLegacyPromoteSeed := dep.Command().ToJobSpec().Mode == pkgevents.ModePromoteSeed

	// A row whose job_params could not be deserialized is unrunnable; fail it
	// permanently with a routable announcement built from its recovered identity.
	if !dep.IsDeployable() {
		if _, err := dep.RegisterFailure(now, true, "deployment job_params not deployable", d.backoff); err != nil {
			return err
		}
		if !isLegacyPromoteSeed {
			if err := d.writeFailedAnnouncements(ctx, outboxRepo, dep); err != nil {
				return err
			}
		}
		return repo.Save(ctx, dep)
	}

	deployErr := d.deployer.Deploy(ctx, dep.Command().ToJobSpec())
	if deployErr == nil {
		if !isLegacyPromoteSeed {
			if err := d.writeFirstCheck(ctx, outboxRepo, dep); err != nil {
				return err
			}
		}
		if err := dep.MarkStarted(now); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	terminal, err := dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff)
	if err != nil {
		return err
	}
	if terminal {
		d.logger.Error("Deploy terminal failure", "deployment_id", dep.ID(), "cause", deployErr)
		if !isLegacyPromoteSeed {
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
// started and writes the first check_delayed ticket so the job-status handler
// status-checks the validation Job (it never polls); it skips the production-only
// task_status_updated announcement. The per-node terminal outcome ("ok"/"failed")
// arrives later when the job-status handler observes the terminal Job and records
// it via outcomes.Recorder, which then triggers the aggregate emit. A validation
// row that cannot be dispatched
// (not deployable, or a permanent pre-deploy deployer error) is failed terminally
// here via FailValidation — which sets a "failed" outcome on a reserved row whose
// Job was never created — and we then settle the node: its blocked descendants
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
		// terminal observation will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedValidation(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeployValidation(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkStarted(now); err != nil {
			return err
		}
		if err := d.writeFirstCheck(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	terminal, err := dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff)
	if err != nil {
		return err
	}
	if terminal {
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
// job-status handler's outcomes.Recorder runs the identical gate under its own
// Unit-of-Work when it settles a node after dispatch. The failed node's own
// outcome is already persisted before this call; "failed" drives the transitive
// skip of its blocked downstreams.
func (d *Dispatcher) settleFailedValidation(ctx context.Context, repo repository.DeploymentRepository, outboxRepo outbox.Repository, aggRepo repository.ValidationAggregateRepository, dep *model.Deployment, now time.Time) error {
	return validation.SettleNodeTerminal(
		ctx, repo, outboxRepo, aggRepo, validation.DedupNamespace,
		dep.ReleaseID(), dep.NodeID(), "failed", now)
}

// dispatchSeedBuild handles a mode=seed_build row. It is structurally identical
// to dispatchValidation: on success it marks the row started and writes the
// first check_delayed ticket so the job-status handler status-checks the
// seed-build Job; on terminal failure it fails the row and settles the per-release seed-build
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
		// terminal observation will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedSeedBuild(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeploySeedBuild(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkStarted(now); err != nil {
			return err
		}
		if err := d.writeFirstCheck(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	terminal, err := dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff)
	if err != nil {
		return err
	}
	if terminal {
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
// dispatchSeedBuild: on success it marks the row started and writes the first
// check_delayed ticket so the job-status handler status-checks the compile Job; on
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
		// terminal observation will re-run the gate for this release.
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		return d.settleFailedCompile(ctx, repo, outboxRepo, aggRepo, dep, now)
	}

	deployErr := d.deployer.DeployCompile(ctx, dep.ValidationCommand().ToValidationJobSpec())
	if deployErr == nil {
		if err := dep.MarkStarted(now); err != nil {
			return err
		}
		if err := d.writeFirstCheck(ctx, outboxRepo, dep); err != nil {
			return err
		}
		return repo.Save(ctx, dep)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	terminal, err := dep.RegisterFailure(now, permanent, deployErr.Error(), d.backoff)
	if err != nil {
		return err
	}
	if terminal {
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

// writeFirstCheck schedules the first status check of a Job the dispatcher has
// just created. It is due after firstCheckDelay so even a Job lasting a couple
// of seconds is observed running before it completes.
func (d *Dispatcher) writeFirstCheck(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	entry, err := checkTicket(dep, d.now().Add(d.firstCheckDelay), false)
	if err != nil {
		return err
	}
	if err := outboxRepo.Create(ctx, entry); err != nil {
		return fmt.Errorf("write first check ticket: %w", err)
	}
	return nil
}

// checkTicket builds a check_delayed outbox row for dep's Job: the publisher
// parks it in the delay queue and the promoter moves it onto check.k8s:v1 at
// checkAfter. runningAnnounced tells the job-status handler whether RUNNING was
// already announced for this attempt. A candidate Job carries the
// deterministic synthetic task and schedule UUIDs derived from (release_id,
// node_id); the handler routes its result by the Job's mode label, so those ids
// only need to be valid UUIDs and satisfy the outbox aggregate id.
func checkTicket(dep *model.Deployment, checkAfter time.Time, runningAnnounced bool) (*outbox.Entry, error) {
	var req event.JobCheckRequest
	var aggregateID uuid.UUID
	if dep.Mode() == model.ModeProduction {
		cmd := dep.Command()
		aggregateID, _ = uuid.Parse(cmd.TaskID)
		req = event.JobCheckRequest{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
			ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
			JobName: cmd.JobName, NodeType: cmd.NodeType, ImageTag: cmd.ImageTag, SecretRef: cmd.SecretRef, Operation: cmd.Operation,
			RetryCount: cmd.TaskRetryCount, MaxRetries: cmd.TaskMaxRetries,
		}
	} else {
		// SecretRef is omitted: validation Jobs never mount an API Secret.
		vc := dep.ValidationCommand()
		taskID, scheduleID := model.ValidationSyntheticIDs(dep.ReleaseID(), dep.NodeID())
		aggregateID = taskID
		req = event.JobCheckRequest{
			TaskID: taskID.String(), ScheduleID: scheduleID.String(),
			ServiceName: vc.ServiceName, SchemaName: vc.SchemaName, TableName: vc.TableName,
			JobName: vc.JobName, NodeType: vc.NodeType, ImageTag: vc.ImageTag,
		}
	}
	req.CheckAfter = checkAfter.Unix()
	req.RunningAnnounced = runningAnnounced
	body, err := json.Marshal(serialization.JobCheckRequestFromDomain(req))
	if err != nil {
		return nil, fmt.Errorf("marshal check ticket: %w", err)
	}
	return &outbox.Entry{
		MessageProcessingID: dep.MessageProcessingID(),
		AggregateType:       "task",
		AggregateID:         aggregateID,
		EventType:           event.EventTypeCheckDelayed,
		Payload:             body,
		StreamName:          streams.CheckK8sV1,
	}, nil
}

// writeFailedAnnouncements announces a production task whose deploy failed
// terminally: task_status_updated (FAILED) and node_updated (FAILED). A deploy
// failure gets no retry Job, so the FAILED status reports the task's retry
// budget as spent: state finalizes a run only once no failed task has attempts
// left (retry_count < max_retries), and a lower count would hold the run open
// until the watchdog cancels it. A command decoded from corrupt job_params
// carries no budget, so the default budget applies.
func (d *Dispatcher) writeFailedAnnouncements(ctx context.Context, outboxRepo outbox.Repository, dep *model.Deployment) error {
	cmd := dep.Command()
	budget := cmd.TaskMaxRetries
	if budget <= 0 {
		budget = int(pkgevents.DefaultTaskMaxRetries)
	}
	retryCount, err := num.Int32(max(cmd.TaskRetryCount, budget), "task_retry_count")
	if err != nil {
		return fmt.Errorf("write FAILED task_status announcement: %w", err)
	}
	if err := d.createOutbox(ctx, outboxRepo, dep, event.EventTypeTaskStatusUpdated, streams.TaskStatusUpdatedV1,
		pkgevents.TaskStatusUpdated{TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: "FAILED", RetryCount: retryCount}); err != nil {
		return fmt.Errorf("write FAILED task_status announcement: %w", err)
	}
	nodeFailed := event.NodeUpdated{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName, Status: "FAILED",
	}
	if err := d.createOutbox(ctx, outboxRepo, dep, event.EventTypeNodeUpdated, streams.NodeUpdatedV1, serialization.NodeUpdatedFromDomain(nodeFailed)); err != nil {
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
	})
}
