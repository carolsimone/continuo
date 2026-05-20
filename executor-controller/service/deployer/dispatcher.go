package deployer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
)

const dbtJobLabelSelector = "app=dbt-job"

// K8sDeployer is the slice of the K8s client the dispatcher needs.
type K8sDeployer interface {
	CreateQueryJob(ctx context.Context, params k8s.JobParams) error
	CountActiveJobs(ctx context.Context, namespace, labelSelector string) (int, error)
}

// DispatcherConfig groups the optional knobs.
type DispatcherConfig struct {
	Tick        time.Duration // poll interval; default 5s
	BatchSize   int           // max rows per batch (also clamped by headroom); default 50
	BackoffBase time.Duration // first retry delay; default 5s
	BackoffCap  time.Duration // max retry delay; default 2m
}

// Dispatcher drains executor_deployments: it deploys K8s Jobs (capped by
// maxConcurrent live Jobs) and writes the canonical announcement rows to
// executor_outbox. The K8s deploy is a command effect kept off the outbox so
// every outbox Publisher stays a uniform marshal-and-XADD.
// DeploymentsRepoFactory builds a Repository bound to a specific executor
// (a *sqlx.Tx the dispatcher opens per batch). Injecting it keeps the concrete
// Postgres adapter out of this package — the dispatcher depends only on the
// Repository port.
type DeploymentsRepoFactory func(exec outbox.Executor) Repository

type Dispatcher struct {
	db            *sqlx.DB
	k8s           K8sDeployer
	newDeplRepo   DeploymentsRepoFactory
	namespace     string
	maxConcurrent int
	logger        *slog.Logger
	tick          time.Duration
	batchSize     int
	backoffBase   time.Duration
	backoffCap    time.Duration
}

func NewDispatcher(
	db *sqlx.DB,
	k8sDeployer K8sDeployer,
	newDeplRepo DeploymentsRepoFactory,
	namespace string,
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
		db: db, k8s: k8sDeployer, newDeplRepo: newDeplRepo,
		namespace: namespace, maxConcurrent: maxConcurrent,
		logger: logger, tick: cfg.Tick, batchSize: cfg.BatchSize,
		backoffBase: cfg.BackoffBase, backoffCap: cfg.BackoffCap,
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

// ProcessBatch runs one cycle. Exported for tests.
func (d *Dispatcher) ProcessBatch(ctx context.Context) error {
	active, err := d.k8s.CountActiveJobs(ctx, d.namespace, dbtJobLabelSelector)
	if err != nil {
		return fmt.Errorf("count active jobs: %w", err)
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

	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	deplRepo := d.newDeplRepo(tx)
	outboxRepo := outbox.NewPostgresRepository(tx, "executor_outbox", d.logger)

	rows, err := deplRepo.GetDueBatch(ctx, headroom)
	if err != nil {
		return fmt.Errorf("get due batch: %w", err)
	}
	if len(rows) == 0 {
		return tx.Commit()
	}

	for _, row := range rows {
		if err := d.dispatchRow(ctx, deplRepo, outboxRepo, row); err != nil {
			return fmt.Errorf("dispatch row %s: %w", row.ID, err)
		}
	}

	err = tx.Commit()
	tx = nil
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (d *Dispatcher) dispatchRow(ctx context.Context, deplRepo Repository, outboxRepo outbox.Repository, row *Deployment) error {
	var job DeployJob
	if err := json.Unmarshal(row.JobParams, &job); err != nil {
		// job_params is written from a valid DeployJob, so a failure here is
		// corruption. Populate identity from the row's typed columns so the
		// FAILED announcements still route correctly downstream.
		job.TaskID = row.TaskID.String()
		job.ScheduleID = row.ScheduleID.String()
		return d.writeFailed(ctx, deplRepo, outboxRepo, row, job, fmt.Sprintf("unmarshal job_params: %v", err))
	}

	params, err := job.ToJobParams(d.namespace)
	if err != nil {
		return d.writeFailed(ctx, deplRepo, outboxRepo, row, job, fmt.Sprintf("invalid job_params: %v", err))
	}

	deployErr := d.k8s.CreateQueryJob(ctx, params)
	if deployErr == nil {
		return d.writeDeployed(ctx, deplRepo, outboxRepo, row, job)
	}

	permanent := errors.Is(deployErr, pkgevents.ErrPermanent)
	if !permanent && row.RetryCount+1 < row.MaxRetries {
		next := time.Now().Add(d.backoff(row.RetryCount))
		d.logger.Warn("Deploy transient failure — rescheduling",
			"deployment_id", row.ID, "retry_count", row.RetryCount, "next_attempt_at", next, "error", deployErr)
		return deplRepo.Reschedule(ctx, row.ID, next, deployErr.Error())
	}
	return d.writeFailed(ctx, deplRepo, outboxRepo, row, job, deployErr.Error())
}

func (d *Dispatcher) writeDeployed(ctx context.Context, deplRepo Repository, outboxRepo outbox.Repository, row *Deployment, job DeployJob) error {
	running := pkgevents.TaskStatusUpdated{
		TaskID: job.TaskID, ScheduleID: job.ScheduleID, Status: "RUNNING",
		RetryCount: int32(job.TaskRetryCount),
	}
	runningPayload, err := json.Marshal(running)
	if err != nil {
		return fmt.Errorf("marshal RUNNING: %w", err)
	}
	if err := outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: row.MessageProcessingID,
		AggregateType:       "task",
		AggregateID:         row.TaskID,
		EventType:           "task_status_updated",
		Payload:             runningPayload,
		StreamName:          streams.TaskStatusUpdatedV1,
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write RUNNING announcement: %w", err)
	}

	deployed := event.JobDeployed{
		TaskID: job.TaskID, ScheduleID: job.ScheduleID, ScheduleName: job.ScheduleName,
		ServiceName: job.ServiceName, SchemaName: job.SchemaName, TableName: job.TableName,
		JobName: job.JobName, NodeType: job.NodeType, ImageTag: job.ImageTag,
		TaskRetryCount: job.TaskRetryCount, MaxRetries: job.TaskMaxRetries,
	}
	deployedPayload, err := json.Marshal(deployed)
	if err != nil {
		return fmt.Errorf("marshal node_deployed: %w", err)
	}
	if err := outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: row.MessageProcessingID,
		AggregateType:       "task",
		AggregateID:         row.TaskID,
		EventType:           "node_deployed",
		Payload:             deployedPayload,
		StreamName:          streams.NodeDeployedV1,
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write node_deployed announcement: %w", err)
	}

	return deplRepo.MarkDeployed(ctx, row.ID)
}

func (d *Dispatcher) writeFailed(ctx context.Context, deplRepo Repository, outboxRepo outbox.Repository, row *Deployment, job DeployJob, cause string) error {
	failed := pkgevents.TaskStatusUpdated{
		TaskID: job.TaskID, ScheduleID: job.ScheduleID, Status: "FAILED",
		RetryCount: int32(job.TaskRetryCount),
	}
	failedPayload, err := json.Marshal(failed)
	if err != nil {
		return fmt.Errorf("marshal FAILED status: %w", err)
	}
	if err := outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: row.MessageProcessingID,
		AggregateType:       "task",
		AggregateID:         row.TaskID,
		EventType:           "task_status_updated",
		Payload:             failedPayload,
		StreamName:          streams.TaskStatusUpdatedV1,
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write FAILED task_status announcement: %w", err)
	}

	nodeFailed := event.NodeUpdated{
		TaskID: job.TaskID, ScheduleID: job.ScheduleID, ScheduleName: job.ScheduleName,
		ServiceName: job.ServiceName, SchemaName: job.SchemaName, TableName: job.TableName,
		Status: "FAILED",
	}
	nodeFailedPayload, err := json.Marshal(nodeFailed)
	if err != nil {
		return fmt.Errorf("marshal FAILED node_updated: %w", err)
	}
	if err := outboxRepo.Create(ctx, &outbox.Entry{
		MessageProcessingID: row.MessageProcessingID,
		AggregateType:       "task",
		AggregateID:         row.TaskID,
		EventType:           "node_updated",
		Payload:             nodeFailedPayload,
		StreamName:          streams.NodeUpdatedV1,
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write FAILED node_updated announcement: %w", err)
	}

	d.logger.Error("Deploy terminal failure", "deployment_id", row.ID, "task_id", job.TaskID, "cause", cause)
	return deplRepo.MarkFailed(ctx, row.ID, cause)
}

// backoff returns base * 2^retryCount capped at backoffCap.
func (d *Dispatcher) backoff(retryCount int) time.Duration {
	delay := d.backoffBase << retryCount
	if delay > d.backoffCap || delay <= 0 { // <=0 guards shift overflow
		return d.backoffCap
	}
	return delay
}
