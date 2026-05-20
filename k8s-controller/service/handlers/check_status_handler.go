package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	postgresadapter "github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// K8sStatusChecker defines interface for checking K8s job status
type K8sStatusChecker interface {
	GetJobStatus(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error)
	GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (fullLog, tail string, err error)
}

// HandlerConfig contains handler configuration
type HandlerConfig struct {
	K8sNamespace          string
	CheckDelaySeconds     int
	ErrorMessageMaxLen    int
	LogTailLines          int64
	DefaultTaskMaxRetries int // used when max_retries is absent from the inbound message
}

// CheckStatusHandler handles CheckJobStatus commands
type CheckStatusHandler struct {
	k8sClient          K8sStatusChecker
	txRunner           uow.TransactionRunner
	logUploader        s3adapter.LogUploader
	config             *HandlerConfig
	cancelledSchedules postgresadapter.CancelledSchedulesRepository
	logger             *slog.Logger
}

// NewCheckStatusHandler creates a new CheckStatusHandler
func NewCheckStatusHandler(
	k8sClient K8sStatusChecker,
	txRunner uow.TransactionRunner,
	logUploader s3adapter.LogUploader,
	config *HandlerConfig,
	cancelledSchedules postgresadapter.CancelledSchedulesRepository,
	logger *slog.Logger,
) *CheckStatusHandler {
	return &CheckStatusHandler{
		k8sClient:          k8sClient,
		txRunner:           txRunner,
		logUploader:        logUploader,
		config:             config,
		cancelledSchedules: cancelledSchedules,
		logger:             logger,
	}
}

// Handle processes a CheckJobStatus command
func (h *CheckStatusHandler) Handle(ctx context.Context, cmd command.CheckJobStatus) error {
	h.logger.Info("Checking K8s job status",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
	)

	// Step 1: Query K8s API for job/pod status
	result, err := h.k8sClient.GetJobStatus(ctx, h.config.K8sNamespace, cmd.JobName)
	if err != nil {
		return fmt.Errorf("failed to get job status: %w", err)
	}

	// Step 2: Use retry_count and max_retries carried in the command (no gRPC needed).
	// max_retries defaults to the configured value when absent from the inbound message
	// (backward-compat: old executor-controller messages won't have it).
	retryCount := cmd.RetryCount
	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		maxRetries = int32(h.config.DefaultTaskMaxRetries)
	}

	// Step 3: Run SQL work inside one fresh transaction so dedup and outbox writes
	// are atomic without storing transaction state on the shared handler.
	return h.txRunner.WithinTransaction(ctx, func(tx uow.Transaction) error {
		// Step 4: Dedup guard — per-stream message dedup via pkg/messageprocessing.
		// Atomically inserts (message_id, stream_name) keyed row; duplicate means skip.
		// Must run inside the transaction so a later failure rolls back the insert.
		_, duplicate, err := messageprocessing.Dedup(ctx, tx.MessageProcessingRepo(), h.logger, cmd.MessageID, cmd.StreamName, cmd.Payload)
		if err != nil {
			return fmt.Errorf("dedup: %w", err)
		}
		if duplicate {
			h.logger.Info("Duplicate message — skipping",
				"msg_id", cmd.MessageID,
				"stream", cmd.StreamName,
				"task_id", cmd.TaskID,
			)
			return nil
		}

		// Guard: if the schedule was cancelled, absorb the result silently.
		// Running pods are left to complete naturally; results are suppressed here.
		cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
		if err != nil {
			return fmt.Errorf("cancelled schedules check: %w", err)
		}
		if cancelled {
			h.logger.Info("Schedule cancelled — absorbing job result",
				"schedule_id", cmd.ScheduleID, "job_name", cmd.JobName, "status", result.Status)
			return nil
		}

		// Step 5: Write outbox entries with repositories scoped to this transaction.
		switch result.Status {
		case model.JobStatusSucceeded:
			return h.handleSucceeded(ctx, tx, cmd, result)
		case model.JobStatusFailed:
			if retryCount >= maxRetries {
				return h.handleFailedPermanent(ctx, tx, cmd, result, retryCount)
			}
			return h.handleFailedWithRetry(ctx, tx, cmd, result, retryCount, maxRetries)
		case model.JobStatusRunning:
			return h.handleRunning(ctx, tx, cmd)
		default:
			return h.handleUnknown(ctx, tx, cmd, result)
		}
	})
}

// handleSucceeded handles successful job completion.
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (SUCCEEDED)
//   - task_execution_recorded
//   - node_status_updated (→ node.updated:v1)
func (h *CheckStatusHandler) handleSucceeded(ctx context.Context, tx uow.Transaction, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	repo := tx.OutboxRepo()
	executionID := uuid.New()

	// Row 1: task_status_updated
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "SUCCEEDED", 0); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded
	if err := h.writeTaskExecutionRecordedWithLogS3Key(ctx, repo, cmd, executionID, result, "", ""); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// Row 3: node_status_updated → node.updated:v1
	if err := h.writeNodeStatusUpdated(ctx, repo, cmd, "SUCCEEDED"); err != nil {
		return fmt.Errorf("node_status_updated: %w", err)
	}

	h.logger.Info("Job succeeded — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"execution_time", result.ExecutionSeconds,
	)

	return nil
}

// fetchAndUploadLogs fetches pod logs and uploads the full log to S3.
// Returns the log tail (for error_message), the S3 key, and a pre-generated execution ID.
// On S3 upload failure it logs a warning and returns an empty key (soft fail).
func (h *CheckStatusHandler) fetchAndUploadLogs(
	ctx context.Context,
	cmd command.CheckJobStatus,
) (executionID uuid.UUID, logS3Key, tail string) {
	executionID = uuid.New()

	fullLog, logTail, err := h.k8sClient.GetPodLogs(ctx, h.config.K8sNamespace, cmd.JobName, h.config.LogTailLines)
	if err != nil {
		h.logger.Warn("Failed to fetch pod logs",
			"job_name", cmd.JobName,
			"error", err,
		)
		return executionID, "", ""
	}

	tail = logTail

	if fullLog == "" {
		h.logger.Warn("Pod log is empty, skipping S3 upload", "job_name", cmd.JobName)
		return executionID, "", tail
	}

	key := fmt.Sprintf("logs/task-executions/%s/%s/%s/%s.log",
		cmd.ServiceName, cmd.SchemaName, cmd.TableName, executionID.String())

	if err := h.logUploader.UploadLog(ctx, key, fullLog); err != nil {
		h.logger.Warn("Failed to upload pod log to S3 — continuing without full log",
			"job_name", cmd.JobName,
			"key", key,
			"error", err,
		)
		return executionID, "", tail
	}

	h.logger.Info("Uploaded pod log to S3", "key", key, "job_name", cmd.JobName)
	return executionID, key, tail
}

// handleFailedPermanent handles permanently failed jobs (retry_count >= max_retries).
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (FAILED)
//   - task_execution_recorded
//   - node_status_updated (→ node.updated:v1)
func (h *CheckStatusHandler) handleFailedPermanent(ctx context.Context, tx uow.Transaction, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount int32) error {
	repo := tx.OutboxRepo()
	newRetryCount := retryCount

	executionID, logS3Key, logTail := h.fetchAndUploadLogs(ctx, cmd)

	errorMsg := h.truncateErrorMessage(logTail)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(result.TerminationMsg)
	}

	// Row 1: task_status_updated (FAILED)
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", int32(newRetryCount)); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded
	if err := h.writeTaskExecutionRecordedWithLogS3Key(ctx, repo, cmd, executionID, result, errorMsg, logS3Key); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// Row 3: node_status_updated → node.updated:v1
	if err := h.writeNodeStatusUpdated(ctx, repo, cmd, "FAILED"); err != nil {
		return fmt.Errorf("node_status_updated: %w", err)
	}

	h.logger.Warn("Job failed permanently — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"retry_count", newRetryCount,
		"error", errorMsg,
		"log_s3_key", logS3Key,
	)
	return nil
}

// retryJobName generates a unique K8s job name for a retry attempt.
// The base job name is truncated so the suffix fits within 63 chars.
func retryJobName(baseJobName string, retryCount int32) string {
	suffix := fmt.Sprintf("-r%d", retryCount)
	maxBase := 63 - len(suffix)
	if len(baseJobName) > maxBase {
		baseJobName = baseJobName[:maxBase]
	}
	return baseJobName + suffix
}

// handleFailedWithRetry handles failed jobs that can be retried.
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (FAILED)
//   - task_execution_recorded
//   - task_retry (→ retry.task:v1)
func (h *CheckStatusHandler) handleFailedWithRetry(ctx context.Context, tx uow.Transaction, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount, maxRetries int32) error {
	repo := tx.OutboxRepo()
	newRetryCount := retryCount + 1

	executionID, logS3Key, logTail := h.fetchAndUploadLogs(ctx, cmd)

	errorMsg := h.truncateErrorMessage(logTail)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(result.TerminationMsg)
	}
	newJobName := retryJobName(cmd.JobName, newRetryCount)

	// Row 1: task_status_updated (FAILED, with new retry count)
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", newRetryCount); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded (for the failed attempt)
	if err := h.writeTaskExecutionRecordedWithLogS3Key(ctx, repo, cmd, executionID, result, errorMsg, logS3Key); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// Row 3: task_retry → retry.task:v1
	retryPayload, err := json.Marshal(event.TaskRetry{
		TaskID:       cmd.TaskID.String(),
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      newJobName,
		ImageTag:     cmd.ImageTag,
		RetryCount:   int(newRetryCount),
		MaxRetries:   int(maxRetries),
		NodeType:     cmd.NodeType,
	})
	if err != nil {
		return fmt.Errorf("marshal task_retry: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     "task_retry",
		Payload:       retryPayload,
		StreamName:    streams.RetryTaskV1,
	}); err != nil {
		return fmt.Errorf("create task_retry row: %w", err)
	}

	h.logger.Warn("Job failed, scheduling retry — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"retry_count", newRetryCount,
		"log_s3_key", logS3Key,
	)

	return nil
}

// handleRunning handles still-running jobs.
// Writes 1 canonical outbox row: check_delayed (→ check.k8s:v1)
func (h *CheckStatusHandler) handleRunning(ctx context.Context, tx uow.Transaction, cmd command.CheckJobStatus) error {
	repo := tx.OutboxRepo()
	checkAfter := time.Now().Add(time.Duration(h.config.CheckDelaySeconds) * time.Second)

	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		maxRetries = int32(h.config.DefaultTaskMaxRetries)
	}

	// Determine the outbox entry ID to carry forward for future dedup; use a new UUID
	// so each check-delayed row has its own identity in the check.k8s:v1 stream.
	outboxEntryID := uuid.New()

	checkPayload, err := json.Marshal(event.JobCheckRequest{
		OutboxEntryID: outboxEntryID.String(),
		TaskID:        cmd.TaskID.String(),
		ScheduleID:    cmd.ScheduleID.String(),
		ScheduleName:  cmd.ScheduleName,
		ServiceName:   cmd.ServiceName,
		SchemaName:    cmd.SchemaName,
		TableName:     cmd.TableName,
		JobName:       cmd.JobName,
		CheckAfter:    checkAfter.Unix(),
		NodeType:      cmd.NodeType,
		ImageTag:      cmd.ImageTag,
		RetryCount:    int(cmd.RetryCount),
		MaxRetries:    int(maxRetries),
	})
	if err != nil {
		return fmt.Errorf("marshal check_delayed: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{
		ID:            outboxEntryID,
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     "check_delayed",
		Payload:       checkPayload,
		StreamName:    streams.CheckK8sV1,
	}); err != nil {
		return fmt.Errorf("create check_delayed row: %w", err)
	}

	h.logger.Debug("Job still running, scheduling re-check — outbox entry created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"check_after", checkAfter,
	)

	return nil
}

// handleUnknown handles unknown job statuses (treated as permanent failure).
// Writes 2 canonical outbox rows in the transaction:
//   - task_status_updated (FAILED)
//   - task_failed (→ task.failed:v1)
func (h *CheckStatusHandler) handleUnknown(ctx context.Context, tx uow.Transaction, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	repo := tx.OutboxRepo()
	errorMsg := h.truncateErrorMessage(result.TerminationMsg)
	if errorMsg == "" {
		errorMsg = "Job not found or unknown status"
	}

	newRetryCount := cmd.RetryCount

	// Row 1: task_status_updated (FAILED)
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", newRetryCount); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_failed → task.failed:v1
	failedPayload, err := json.Marshal(event.TaskFailed{
		TaskID:       cmd.TaskID.String(),
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,
		ErrorMessage: errorMsg,
		RetryCount:   int(newRetryCount),
	})
	if err != nil {
		return fmt.Errorf("marshal task_failed: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     "task_failed",
		Payload:       failedPayload,
		StreamName:    streams.TaskFailedV1,
	}); err != nil {
		return fmt.Errorf("create task_failed row: %w", err)
	}

	h.logger.Error("Job status unknown — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"error", errorMsg,
	)

	return nil
}

// writeTaskStatusUpdated writes a task_status_updated canonical outbox row.
func (h *CheckStatusHandler) writeTaskStatusUpdated(
	ctx context.Context,
	repo pkgoutbox.Repository,
	taskID, scheduleID uuid.UUID,
	status string,
	retryCount int32,
) error {
	payload, err := json.Marshal(pkgevents.TaskStatusUpdated{
		TaskID:     taskID.String(),
		ScheduleID: scheduleID.String(),
		Status:     status,
		RetryCount: retryCount,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   taskID,
		EventType:     "task_status_updated",
		Payload:       payload,
		StreamName:    streams.TaskStatusUpdatedV1,
	})
}

// writeTaskExecutionRecordedWithLogS3Key writes a task_execution_recorded canonical outbox row.
func (h *CheckStatusHandler) writeTaskExecutionRecordedWithLogS3Key(
	ctx context.Context,
	repo pkgoutbox.Repository,
	cmd command.CheckJobStatus,
	executionID uuid.UUID,
	result *model.K8sPodResult,
	errorMsg, logS3Key string,
) error {
	exec := pkgevents.TaskExecutionRecorded{
		ExecutionID:      executionID.String(),
		TaskID:           cmd.TaskID.String(),
		JobName:          cmd.JobName,
		ExecutionSeconds: result.ExecutionSeconds,
		ErrorMessage:     errorMsg,
		LogS3Key:         logS3Key,
	}
	if result.StartedAt != nil {
		exec.StartedAt = result.StartedAt.UTC().Format(time.RFC3339)
	}
	if result.CompletedAt != nil {
		exec.CompletedAt = result.CompletedAt.UTC().Format(time.RFC3339)
	}

	payload, err := json.Marshal(exec)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     "task_execution_recorded",
		Payload:       payload,
		StreamName:    streams.TaskExecutionRecordedV1,
	})
}

// writeNodeStatusUpdated writes a node_status_updated canonical outbox row (→ node.updated:v1).
func (h *CheckStatusHandler) writeNodeStatusUpdated(
	ctx context.Context,
	repo pkgoutbox.Repository,
	cmd command.CheckJobStatus,
	status string,
) error {
	payload, err := json.Marshal(event.NodeStatusUpdated{
		TaskID:       cmd.TaskID.String(),
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		Status:       status,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     "node_status_updated",
		Payload:       payload,
		StreamName:    streams.NodeUpdatedV1,
	})
}

// truncateErrorMessage truncates error messages to configured max length
func (h *CheckStatusHandler) truncateErrorMessage(msg string) string {
	if len(msg) > h.config.ErrorMessageMaxLen {
		return msg[:h.config.ErrorMessageMaxLen] + "...[truncated]"
	}
	return msg
}
