package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// deploymentsRepository is the Postgres adapter implementing
// repository.DeploymentRepository over the executor_deployments table.
type deploymentsRepository struct {
	exec   outbox.Executor
	logger *slog.Logger
}

var _ repository.DeploymentRepository = (*deploymentsRepository)(nil)

// NewDeploymentsRepository constructs a repository.DeploymentRepository over
// executor_deployments. Pass *sqlx.DB for autocommit or *sqlx.Tx for
// transactional use; outbox.Executor abstracts both.
func NewDeploymentsRepository(exec outbox.Executor, logger *slog.Logger) repository.DeploymentRepository {
	return &deploymentsRepository{exec: exec, logger: logger}
}

// deploymentRow is the adapter-internal scan struct. The Deployment aggregate
// itself carries no persistence tags; mapping happens here.
type deploymentRow struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	TaskID              uuid.UUID  `db:"task_id"`
	ScheduleID          uuid.UUID  `db:"schedule_id"`
	JobParams           []byte     `db:"job_params"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	NextAttemptAt       time.Time  `db:"next_attempt_at"`
	CreatedAt           time.Time  `db:"created_at"`
	DeployedAt          *time.Time `db:"deployed_at"`
	ErrorMessage        *string    `db:"error_message"`
	Mode                string     `db:"mode"`
	ReleaseID           *string    `db:"release_id"`
	NodeID              *string    `db:"node_id"`
	Outcome             *string    `db:"outcome"`
	DBTLogURI           *string    `db:"dbt_log_uri"`
	RunResultsURI       *string    `db:"run_results_uri"`
	OutcomeAt           *time.Time `db:"outcome_at"`
	ExecutionMode       string     `db:"execution_mode"`
	PoolKey             *string    `db:"pool_key"`
	ResolvedArgv        []byte     `db:"resolved_argv"`
	ExecutionPath       *string    `db:"execution_path"`
	SlotReservedAt      *time.Time `db:"slot_reserved_at"`
	SlotReleasedAt      *time.Time `db:"slot_released_at"`
	LeaseID             *uuid.UUID `db:"lease_id"`
	LeaseTokenSHA256    *string    `db:"lease_token_sha256"`
	LeaseOwner          *string    `db:"lease_owner"`
	LeasePodName        *string    `db:"lease_pod_name"`
	LeasePodUID         *string    `db:"lease_pod_uid"`
	Attempt             int        `db:"attempt"`
	LeaseExpiresAt      *time.Time `db:"lease_expires_at"`
	HeartbeatAt         *time.Time `db:"heartbeat_at"`
	StartedAt           *time.Time `db:"started_at"`
	FinishedAt          *time.Time `db:"finished_at"`
	TerminalResult      []byte     `db:"terminal_result"`
}

// selectColumns is the full column list every getter reconstitutes a Deployment
// from. Mirrors the order deploymentRow's db tags expect.
const selectColumns = `
	id, message_processing_id, task_id, schedule_id, job_params,
	status, retry_count, max_retries, next_attempt_at,
	created_at, deployed_at, error_message,
	mode, release_id, node_id, outcome, dbt_log_uri, outcome_at, run_results_uri,
	execution_mode, pool_key, resolved_argv, execution_path,
	slot_reserved_at, slot_released_at,
	lease_id, lease_token_sha256, lease_owner, lease_pod_name, lease_pod_uid,
	attempt, lease_expires_at, heartbeat_at, started_at, finished_at, terminal_result`

func (r *deploymentsRepository) Add(ctx context.Context, d *model.Deployment) error {
	var (
		jobParams          []byte
		taskID, scheduleID uuid.UUID
		releaseID, nodeID  *string
		err                error
	)
	if d.Mode() == model.ModeValidation || d.Mode() == model.ModeSeedBuild || d.Mode() == model.ModeCompile {
		vcmd := d.ValidationCommand()
		if jobParams, err = json.Marshal(vcmd); err != nil {
			return fmt.Errorf("marshal validation deploy command: %w", err)
		}
		// task_id/schedule_id are NOT NULL but validation rows have no real
		// task/schedule identity; derive stable synthetic UUIDs from (release_id,
		// node_id) so re-adds map to the same row.
		taskID, scheduleID = model.ValidationSyntheticIDs(vcmd.ReleaseID, vcmd.NodeID)
		rid, nid := vcmd.ReleaseID, vcmd.NodeID
		releaseID, nodeID = &rid, &nid
	} else {
		cmd := d.Command()
		if jobParams, err = json.Marshal(cmd); err != nil {
			return fmt.Errorf("marshal deploy command: %w", err)
		}
		if taskID, scheduleID, err = commandIDs(cmd); err != nil {
			return err
		}
	}

	const query = `
		INSERT INTO executor_deployments (
			id, message_processing_id, task_id, schedule_id, job_params,
			status, retry_count, max_retries, next_attempt_at, created_at,
			mode, release_id, node_id, execution_mode, pool_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	if _, err := r.exec.ExecContext(ctx, query,
		d.ID(), d.MessageProcessingID(), taskID, scheduleID, jobParams,
		string(d.Status()), d.RetryCount(), d.MaxRetries(), d.NextAttemptAt(), d.CreatedAt(),
		string(d.Mode()), releaseID, nodeID, string(d.ExecutionMode()), nullableStr(d.PoolKey()),
	); err != nil {
		return fmt.Errorf("insert executor_deployments row: %w", err)
	}
	return nil
}

// GetDueJobs returns due pending Jobs-mode deployments. Worker-mode rows are
// excluded: they are claimed by a worker through GetDueWorkerForPool, not
// dispatched as Kubernetes Jobs.
func (r *deploymentsRepository) GetDueJobs(ctx context.Context, limit int) ([]*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE status = 'pending' AND next_attempt_at <= NOW()
		  AND execution_mode = 'jobs'
		ORDER BY next_attempt_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, limit); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get due deployments batch: %w", err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

func (r *deploymentsRepository) Save(ctx context.Context, d *model.Deployment) error {
	argv, err := encodeArgv(d.ResolvedArgv())
	if err != nil {
		return fmt.Errorf("save deployment %s: %w", d.ID(), err)
	}
	terminalResult, err := encodeTerminalResult(d.TerminalResult())
	if err != nil {
		return fmt.Errorf("save deployment %s: %w", d.ID(), err)
	}
	lease := newLeaseRow(d.ActiveLease())

	// resolved_argv and execution_path are write-once: they record what a task was
	// actually attempted with, so once stored they must survive every later Save.
	// COALESCE keeps the first non-NULL value, which means a config reload cannot
	// change the argv or path of a task that has already been resolved.
	const query = `
		UPDATE executor_deployments
		SET status = $2, retry_count = $3, next_attempt_at = $4, deployed_at = $5, error_message = $6,
		    outcome = $7, dbt_log_uri = $8, outcome_at = $9, run_results_uri = $10,
		    execution_mode = $11, pool_key = $12,
		    resolved_argv = COALESCE(resolved_argv, $13), execution_path = COALESCE(execution_path, $14),
		    slot_reserved_at = $15, slot_released_at = $16,
		    lease_id = $17, lease_token_sha256 = $18, lease_owner = $19,
		    lease_pod_name = $20, lease_pod_uid = $21, attempt = $22,
		    lease_expires_at = $23, heartbeat_at = $24, started_at = $25, finished_at = $26,
		    terminal_result = $27
		WHERE id = $1`
	res, err := r.exec.ExecContext(ctx, query,
		d.ID(), string(d.Status()), d.RetryCount(), d.NextAttemptAt(), d.DeployedAt(), d.ErrorMessage(),
		nullableStr(d.Outcome()), nullableStr(d.DBTLogURI()), d.OutcomeAt(), nullableStr(d.DBTRunResultsURI()),
		string(d.ExecutionMode()), nullableStr(d.PoolKey()), argv, nullableStr(string(d.ExecutionPath())),
		d.Reservation().ReservedAt, d.Reservation().ReleasedAt,
		lease.ID, lease.TokenSHA256, lease.Owner, lease.PodName, lease.PodUID, d.Attempt(),
		lease.ExpiresAt, lease.HeartbeatAt, lease.StartedAt, lease.FinishedAt,
		terminalResult,
	)
	if err != nil {
		return fmt.Errorf("save deployment %s: %w", d.ID(), err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for save", d.ID())
	}
	return nil
}

func (r *deploymentsRepository) GetByReleaseNode(ctx context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE mode = $3 AND release_id = $1 AND node_id = $2`
	dep, err := r.getOne(ctx, query, releaseID, nodeID, string(mode))
	if err != nil {
		return nil, fmt.Errorf("get validation deployment for release %s node %s: %w", releaseID, nodeID, err)
	}
	if dep == nil {
		return nil, sql.ErrNoRows
	}
	return dep, nil
}

// GetByID returns one Deployment, or sql.ErrNoRows when it does not exist.
func (r *deploymentsRepository) GetByID(ctx context.Context, id uuid.UUID) (*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE id = $1`
	dep, err := r.getOne(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("get deployment %s: %w", id, err)
	}
	if dep == nil {
		return nil, sql.ErrNoRows
	}
	return dep, nil
}

// getOne runs a query expected to yield at most one row and reconstitutes it,
// returning (nil, nil) when it matched nothing.
func (r *deploymentsRepository) getOne(ctx context.Context, query string, args ...interface{}) (*model.Deployment, error) {
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, args...); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return r.toAggregate(rows[0]), nil
}

func (r *deploymentsRepository) PendingValidationCount(ctx context.Context, releaseID string, mode model.Mode) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1
		  AND status IN ('pending','blocked','deployed') AND outcome IS NULL`
	var n int
	if err := r.exec.QueryRowContext(ctx, query, releaseID, string(mode)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending validations for release %s: %w", releaseID, err)
	}
	return n, nil
}

func (r *deploymentsRepository) ListValidationResults(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1 AND outcome IS NOT NULL
		ORDER BY outcome_at ASC`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, releaseID, string(mode)); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("list validation results for release %s: %w", releaseID, err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

func (r *deploymentsRepository) ListValidationByRelease(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE mode = $2 AND release_id = $1
		ORDER BY created_at ASC`
	var rows []*deploymentRow
	if err := r.exec.SelectContext(ctx, &rows, query, releaseID, string(mode)); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("list validation deployments for release %s: %w", releaseID, err)
	}
	out := make([]*model.Deployment, len(rows))
	for i, row := range rows {
		out[i] = r.toAggregate(row)
	}
	return out, nil
}

// toAggregate reconstitutes a Deployment from a row. If job_params cannot be
// deserialized (data corruption — it is written from a valid command) the
// task/schedule identity is recovered from the dedicated columns so the
// dispatcher can still fail the deployment with a routable announcement; the
// aggregate's IsDeployable() will report false.
func (r *deploymentsRepository) toAggregate(row *deploymentRow) *model.Deployment {
	in := model.ReconstituteInput{
		ID:                  row.ID,
		MessageProcessingID: row.MessageProcessingID,
		Mode:                model.Mode(row.Mode),
		Status:              model.Status(row.Status),
		RetryCount:          row.RetryCount,
		MaxRetries:          row.MaxRetries,
		NextAttemptAt:       row.NextAttemptAt,
		CreatedAt:           row.CreatedAt,
		DeployedAt:          row.DeployedAt,
		ErrorMessage:        row.ErrorMessage,
		ExecutionMode:       model.ExecutionMode(row.ExecutionMode),
		PoolKey:             derefStr(row.PoolKey),
		ExecutionPath:       model.ExecutionPath(derefStr(row.ExecutionPath)),
		Reservation: model.Reservation{
			ReservedAt: row.SlotReservedAt,
			ReleasedAt: row.SlotReleasedAt,
		},
		Attempt:        row.Attempt,
		Lease:          row.toLease(),
		TerminalResult: r.decodeTerminalResult(row),
	}
	if argv := decodeArgv(r.logger, row); argv != nil {
		in.ResolvedArgv = argv
	}

	switch in.Mode {
	case model.ModeValidation, model.ModeSeedBuild, model.ModeCompile:
		var vcmd command.ValidationDeployTask
		if err := json.Unmarshal(row.JobParams, &vcmd); err != nil {
			r.logger.Error("deployment job_params unparseable — recovering identity from columns",
				"deployment_id", row.ID, "mode", row.Mode, "error", err)
			vcmd = command.ValidationDeployTask{
				ReleaseID: derefStr(row.ReleaseID),
				NodeID:    derefStr(row.NodeID),
			}
		}
		in.ValidationCommand = vcmd
		in.Outcome = derefStr(row.Outcome)
		in.DBTLogURI = derefStr(row.DBTLogURI)
		in.DBTRunResultsURI = derefStr(row.RunResultsURI)
		in.OutcomeAt = row.OutcomeAt
	default:
		var cmd command.DeployTask
		if err := json.Unmarshal(row.JobParams, &cmd); err != nil {
			r.logger.Error("deployment job_params unparseable — recovering identity from columns",
				"deployment_id", row.ID, "error", err)
			cmd = command.DeployTask{TaskID: row.TaskID.String(), ScheduleID: row.ScheduleID.String()}
		}
		in.Command = cmd
	}
	return model.Reconstitute(in)
}

// toLease rebuilds the row's worker lease, or nil when no worker holds it.
func (row *deploymentRow) toLease() *model.Lease {
	if row.LeaseID == nil {
		return nil
	}
	return &model.Lease{
		ID:          *row.LeaseID,
		TokenSHA256: derefStr(row.LeaseTokenSHA256),
		Owner:       derefStr(row.LeaseOwner),
		PodName:     derefStr(row.LeasePodName),
		PodUID:      derefStr(row.LeasePodUID),
		Attempt:     row.Attempt,
		ExpiresAt:   derefTime(row.LeaseExpiresAt),
		HeartbeatAt: derefTime(row.HeartbeatAt),
		StartedAt:   row.StartedAt,
		FinishedAt:  row.FinishedAt,
	}
}

// decodeTerminalResult recovers the worker's terminal report. An unparseable
// value is audit data only, so it is logged and dropped rather than failing the
// read of a row whose lifecycle state is intact.
func (r *deploymentsRepository) decodeTerminalResult(row *deploymentRow) *model.WorkerResult {
	if len(row.TerminalResult) == 0 {
		return nil
	}
	var result model.WorkerResult
	if err := json.Unmarshal(row.TerminalResult, &result); err != nil {
		r.logger.Error("deployment terminal_result unparseable — dropping worker result",
			"deployment_id", row.ID, "error", err)
		return nil
	}
	return &result
}

// decodeArgv recovers the command a worker pinned for this task.
func decodeArgv(logger *slog.Logger, row *deploymentRow) []string {
	if len(row.ResolvedArgv) == 0 {
		return nil
	}
	var argv []string
	if err := json.Unmarshal(row.ResolvedArgv, &argv); err != nil {
		logger.Error("deployment resolved_argv unparseable — dropping pinned command",
			"deployment_id", row.ID, "error", err)
		return nil
	}
	return argv
}

// capacityLockKey serializes the executor's capacity accounting. Every
// transaction that reserves an execution slot takes this transaction-scoped
// advisory lock before counting, so Jobs dispatch and worker claims cannot
// both read the same free slot and overshoot MAX_CONCURRENT_EXECUTIONS.
const capacityLockKey = 2147483001

// LockCapacity takes the transaction-scoped capacity lock. It must be called
// inside a transaction, before ActiveSlotCount.
func (r *deploymentsRepository) LockCapacity(ctx context.Context) error {
	if _, err := r.exec.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, capacityLockKey); err != nil {
		return fmt.Errorf("lock executor capacity: %w", err)
	}
	return nil
}

// ActiveSlotCount counts the execution slots currently held, by Jobs-mode and
// worker-mode work alike.
func (r *deploymentsRepository) ActiveSlotCount(ctx context.Context) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM executor_deployments
		WHERE slot_reserved_at IS NOT NULL AND slot_released_at IS NULL`
	var n int
	if err := r.exec.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count active execution slots: %w", err)
	}
	return n, nil
}

// ReleaseSlot frees the execution slot held by a Jobs-mode deployment whose
// Kubernetes Job reached a terminal status. Worker transitions release their own
// slot inside the aggregate, so they never call this. Returns false when the row
// held no slot, making a duplicate terminal event a no-op.
func (r *deploymentsRepository) ReleaseSlot(ctx context.Context, id uuid.UUID, now time.Time) (bool, error) {
	const query = `
		UPDATE executor_deployments
		SET slot_released_at = $2
		WHERE id = $1 AND slot_reserved_at IS NOT NULL AND slot_released_at IS NULL`
	res, err := r.exec.ExecContext(ctx, query, id, now)
	if err != nil {
		return false, fmt.Errorf("release execution slot for deployment %s: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// GetDueWorkerForPool returns one due pending worker-mode deployment for
// poolKey, locked FOR UPDATE SKIP LOCKED, or nil when the pool has no due work.
// Must be called inside the transaction that claims it.
func (r *deploymentsRepository) GetDueWorkerForPool(ctx context.Context, poolKey string) (*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE execution_mode = 'workers' AND status = 'pending'
		  AND pool_key = $1 AND next_attempt_at <= NOW()
		ORDER BY created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	dep, err := r.getOne(ctx, query, poolKey)
	if err != nil {
		return nil, fmt.Errorf("get due worker deployment for pool %s: %w", poolKey, err)
	}
	return dep, nil
}

// GetExpiredLeaseForUpdate returns one worker deployment whose lease deadline
// has passed, locked FOR UPDATE SKIP LOCKED, or nil when none has expired.
func (r *deploymentsRepository) GetExpiredLeaseForUpdate(ctx context.Context, now time.Time) (*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE status IN ('leased','running') AND lease_expires_at < $1
		ORDER BY lease_expires_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	dep, err := r.getOne(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("get expired lease: %w", err)
	}
	return dep, nil
}

// GetStaleDispatchingForUpdate returns one deployment that has held a
// 'dispatching' reservation since before the given instant, locked FOR UPDATE
// SKIP LOCKED, or nil when none is stale. It recovers the window between
// creating a Kubernetes Job and committing the transition.
func (r *deploymentsRepository) GetStaleDispatchingForUpdate(ctx context.Context, before time.Time) (*model.Deployment, error) {
	const query = `
		SELECT` + selectColumns + `
		FROM executor_deployments
		WHERE status = 'dispatching' AND slot_reserved_at < $1
		ORDER BY slot_reserved_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	dep, err := r.getOne(ctx, query, before)
	if err != nil {
		return nil, fmt.Errorf("get stale dispatching deployment: %w", err)
	}
	return dep, nil
}

// poolDemandRow is the adapter-internal scan struct for ListPoolDemand.
type poolDemandRow struct {
	PoolKey       string     `db:"pool_key"`
	ServiceName   string     `db:"service_name"`
	ImageTag      string     `db:"image_tag"`
	ManifestURI   string     `db:"runtime_manifest_uri"`
	ManifestSHA   string     `db:"runtime_manifest_sha256"`
	DBTVersion    string     `db:"runtime_manifest_dbt_version"`
	ParseContext  string     `db:"runtime_manifest_parse_context_sha256"`
	Pending       int        `db:"pending"`
	ActiveLeases  int        `db:"active_leases"`
	OldestReadyAt *time.Time `db:"oldest_ready_at"`
}

// ListPoolDemand reports each registered worker pool's backlog and in-flight
// load at now, so the pool reconciler can size its replicas. Pools with no work
// are included with zero counts.
func (r *deploymentsRepository) ListPoolDemand(ctx context.Context, now time.Time) ([]model.PoolDemand, error) {
	const query = `
		SELECT p.pool_key, p.service_name, p.image_tag,
		       p.runtime_manifest_uri, p.runtime_manifest_sha256,
		       p.runtime_manifest_dbt_version, p.runtime_manifest_parse_context_sha256,
		       COUNT(*) FILTER (
		           WHERE d.status = 'pending' AND d.next_attempt_at <= $1
		       ) AS pending,
		       COUNT(*) FILTER (WHERE d.status IN ('leased','running')) AS active_leases,
		       MIN(d.next_attempt_at) FILTER (
		           WHERE d.status = 'pending' AND d.next_attempt_at <= $1
		       ) AS oldest_ready_at
		FROM executor_worker_pools p
		LEFT JOIN executor_deployments d
		       ON d.pool_key = p.pool_key AND d.execution_mode = 'workers'
		GROUP BY p.pool_key, p.service_name, p.image_tag,
		         p.runtime_manifest_uri, p.runtime_manifest_sha256,
		         p.runtime_manifest_dbt_version, p.runtime_manifest_parse_context_sha256
		ORDER BY p.pool_key ASC`
	var rows []*poolDemandRow
	if err := r.exec.SelectContext(ctx, &rows, query, now); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("list pool demand: %w", err)
	}
	out := make([]model.PoolDemand, len(rows))
	for i, row := range rows {
		out[i] = model.PoolDemand{
			PoolKey:     row.PoolKey,
			ServiceName: row.ServiceName,
			ImageTag:    row.ImageTag,
			RuntimeManifest: pkgmodel.RuntimeManifestRef{
				RuntimeManifestURI:                row.ManifestURI,
				RuntimeManifestSHA256:             row.ManifestSHA,
				RuntimeManifestDBTVersion:         row.DBTVersion,
				RuntimeManifestParseContextSHA256: row.ParseContext,
			},
			Pending:       row.Pending,
			ActiveLeases:  row.ActiveLeases,
			OldestReadyAt: derefTime(row.OldestReadyAt),
		}
	}
	return out, nil
}

// DemotePendingPoolToJobs converts a pool's not-yet-started work back to the
// Kubernetes Job path, returning how many rows moved. Any argv already pinned is
// preserved so a Job runs the same command. A row awaiting requeue after a
// retryable failure is promoted to 'pending' as it moves, because retry_pending
// is a worker-path state the Jobs dispatcher never reads; it keeps its recorded
// next_attempt_at, so a task that just failed retryably serves out its backoff
// on the Jobs path instead of retrying at once. A row that was already pending
// has any future next_attempt_at pulled back to now, making it due immediately.
// Leased and running work is never converted: it must finish, or be cancelled
// and fenced first.
func (r *deploymentsRepository) DemotePendingPoolToJobs(ctx context.Context, poolKey string, now time.Time) (int64, error) {
	const query = `
		UPDATE executor_deployments
		SET status = 'pending', execution_mode = 'jobs', pool_key = NULL,
		    next_attempt_at = CASE
		        WHEN status = 'retry_pending' THEN next_attempt_at
		        ELSE LEAST(next_attempt_at, $2)
		    END
		WHERE execution_mode = 'workers' AND pool_key = $1
		  AND status IN ('pending','retry_pending')`
	res, err := r.exec.ExecContext(ctx, query, poolKey, now)
	if err != nil {
		return 0, fmt.Errorf("demote pool %s to jobs: %w", poolKey, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// CancelSchedule marks a schedule's not-yet-terminal deployments cancelled and
// returns the leases that were active, so the caller can terminate exactly those
// worker pods. Pending rows are cancelled outright; leased and running rows are
// reported so their pods are fenced and deleted before the cancellation commits.
func (r *deploymentsRepository) CancelSchedule(ctx context.Context, scheduleID uuid.UUID, now time.Time) ([]model.ActiveLease, error) {
	const query = `
		UPDATE executor_deployments
		SET status = 'cancelled',
		    error_message = 'schedule cancelled',
		    slot_released_at = CASE
		        WHEN slot_reserved_at IS NOT NULL AND slot_released_at IS NULL THEN $2
		        ELSE slot_released_at
		    END
		WHERE schedule_id = $1
		  AND status IN ('pending','blocked','dispatching','deployed','leased','running','retry_pending')
		RETURNING id, lease_id, lease_pod_name, lease_pod_uid`
	type cancelledRow struct {
		ID      uuid.UUID  `db:"id"`
		LeaseID *uuid.UUID `db:"lease_id"`
		PodName *string    `db:"lease_pod_name"`
		PodUID  *string    `db:"lease_pod_uid"`
	}
	var rows []*cancelledRow
	if err := r.exec.SelectContext(ctx, &rows, query, scheduleID, now); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("cancel schedule %s: %w", scheduleID, err)
	}
	var out []model.ActiveLease
	for _, row := range rows {
		if row.LeaseID == nil {
			continue
		}
		out = append(out, model.ActiveLease{
			DeploymentID: row.ID,
			LeaseID:      *row.LeaseID,
			PodName:      derefStr(row.PodName),
			PodUID:       derefStr(row.PodUID),
		})
	}
	return out, nil
}

// leaseRow is the nullable column set a Lease maps onto. A deployment with no
// lease writes NULL to every lease column. The attempt column is written from
// the Deployment's own counter, which outlives any single lease.
type leaseRow struct {
	ID          *uuid.UUID
	TokenSHA256 *string
	Owner       *string
	PodName     *string
	PodUID      *string
	ExpiresAt   *time.Time
	HeartbeatAt *time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

func newLeaseRow(l *model.Lease) leaseRow {
	if l == nil {
		return leaseRow{}
	}
	id, expires, heartbeat := l.ID, l.ExpiresAt, l.HeartbeatAt
	return leaseRow{
		ID:          &id,
		TokenSHA256: nullableStr(l.TokenSHA256),
		Owner:       nullableStr(l.Owner),
		PodName:     nullableStr(l.PodName),
		PodUID:      nullableStr(l.PodUID),
		ExpiresAt:   &expires,
		HeartbeatAt: &heartbeat,
		StartedAt:   l.StartedAt,
		FinishedAt:  l.FinishedAt,
	}
}

// encodeArgv serializes a pinned command, or NULL when none is pinned.
func encodeArgv(argv []string) ([]byte, error) {
	if argv == nil {
		return nil, nil
	}
	b, err := json.Marshal(argv)
	if err != nil {
		return nil, fmt.Errorf("marshal resolved argv: %w", err)
	}
	return b, nil
}

// encodeTerminalResult serializes a worker's terminal report, or NULL when the
// task has not reported one.
func encodeTerminalResult(result *model.WorkerResult) ([]byte, error) {
	if result == nil {
		return nil, nil
	}
	b, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal terminal result: %w", err)
	}
	return b, nil
}

// derefTime returns the pointed-to time, or the zero time when nil.
func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

// nullableStr maps an empty string to NULL so columns guarded by an
// IN (...) OR IS NULL check accept the "not yet set" state.
func nullableStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// derefStr returns the pointed-to string, or "" when the pointer is nil.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// commandIDs parses the command's task/schedule identity into the UUID column
// values. The command holds them as strings (its wire form); the table stores
// them as typed UUID columns for indexing and identity recovery.
func commandIDs(cmd command.DeployTask) (uuid.UUID, uuid.UUID, error) {
	taskID, err := uuid.Parse(cmd.TaskID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("parse task_id %q: %w", cmd.TaskID, err)
	}
	scheduleID, err := uuid.Parse(cmd.ScheduleID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("parse schedule_id %q: %w", cmd.ScheduleID, err)
	}
	return taskID, scheduleID, nil
}
