package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
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
	k8sClient   K8sStatusChecker
	uow         uow.UnitOfWork
	logUploader s3adapter.LogUploader
	config      *HandlerConfig
	logger      *slog.Logger
}

// NewCheckStatusHandler creates a new CheckStatusHandler
func NewCheckStatusHandler(
	k8sClient K8sStatusChecker,
	uow uow.UnitOfWork,
	logUploader s3adapter.LogUploader,
	config *HandlerConfig,
	logger *slog.Logger,
) *CheckStatusHandler {
	return &CheckStatusHandler{
		k8sClient:   k8sClient,
		uow:         uow,
		logUploader: logUploader,
		config:      config,
		logger:      logger,
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

	// Step 3: Begin transaction (wraps outbox writes + processed_events insert)
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Step 4: Dedup guard — atomic claim via INSERT ... ON CONFLICT DO NOTHING.
	// Must run inside the transaction so a later failure rolls back the insert.
	if cmd.OutboxEntryID != nil {
		duplicate, err := h.uow.ProcessedEventsRepo().TryMarkProcessed(ctx, *cmd.OutboxEntryID)
		if err != nil {
			return fmt.Errorf("dedup check failed: %w", err)
		}
		if duplicate {
			h.logger.Info("Duplicate message — skipping",
				"outbox_entry_id", cmd.OutboxEntryID,
				"task_id", cmd.TaskID,
			)
			// Rollback is handled by defer; consumer will XACK.
			return nil
		}
	}

	// Step 5: Write outbox entries (sub-handlers use h.uow.OutboxRepo() — already in-tx)
	switch result.Status {
	case model.JobStatusSucceeded:
		if err := h.handleSucceeded(ctx, cmd, result); err != nil {
			return err
		}
	case model.JobStatusFailed:
		if retryCount >= maxRetries {
			if err := h.handleFailedPermanent(ctx, cmd, result, retryCount); err != nil {
				return err
			}
		} else {
			if err := h.handleFailedWithRetry(ctx, cmd, result, retryCount); err != nil {
				return err
			}
		}
	case model.JobStatusRunning:
		if err := h.handleRunning(ctx, cmd); err != nil {
			return err
		}
	default:
		if err := h.handleUnknown(ctx, cmd, result); err != nil {
			return err
		}
	}

	// Step 6: Commit (outbox entries + processed_events land atomically)
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// handleSucceeded handles successful job completion
func (h *CheckStatusHandler) handleSucceeded(ctx context.Context, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	// Create outbox entry for succeeded task
	// Note: task.succeeded events don't get published to Redis in current design
	// They're just recorded in state-service
	entry := &model.K8sStatusOutboxEntry{
		EventType:    "task_succeeded",
		StreamName:   "", // No Redis event for success
		TaskID:       cmd.TaskID,
		ScheduleID:   cmd.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,

		UpdateTaskStatus:     true,
		NewTaskStatus:        "succeeded",
		NewRetryCount:        0,
		CreateExecution:      true,
		ExecutionStartedAt:   result.StartedAt,
		ExecutionCompletedAt: result.CompletedAt,
		ExecutionSeconds:     result.ExecutionSeconds,
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	// Second outbox entry: notify orchestrator of node status change
	notifyEntry := &model.K8sStatusOutboxEntry{
		EventType:        "node_status_updated",
		StreamName:       "node.updated:v1",
		TaskID:           cmd.TaskID,
		ScheduleID:       cmd.ScheduleID,
		ScheduleName:     cmd.ScheduleName,
		ServiceName:      cmd.ServiceName,
		SchemaName:       cmd.SchemaName,
		TableName:        cmd.TableName,
		JobName:          cmd.JobName,
		NewTaskStatus:    "SUCCEEDED",
		UpdateTaskStatus: false,
		CreateExecution:  false,
	}

	if err := h.uow.OutboxRepo().Create(ctx, notifyEntry); err != nil {
		return fmt.Errorf("failed to create notify outbox entry: %w", err)
	}

	h.logger.Info("Job succeeded - outbox entries created",
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

// handleFailedPermanent handles permanently failed jobs (retry_count >= max_retries)
func (h *CheckStatusHandler) handleFailedPermanent(ctx context.Context, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount int32) error {
	newRetryCount := retryCount

	executionID, logS3Key, logTail := h.fetchAndUploadLogs(ctx, cmd)

	errorMsg := h.truncateErrorMessage(logTail)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(result.TerminationMsg)
	}

	entry := &model.K8sStatusOutboxEntry{
		EventType:    "task_failed",
		StreamName:   "task.failed:v1",
		TaskID:       cmd.TaskID,
		ScheduleID:   cmd.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,

		ErrorMessage:   errorMsg,
		TaskRetryCount: int(newRetryCount),
		ExecutionID:    executionID,
		LogS3Key:       logS3Key,

		UpdateTaskStatus:     true,
		NewTaskStatus:        "failed",
		NewRetryCount:        int(newRetryCount),
		CreateExecution:      true,
		ExecutionStartedAt:   result.StartedAt,
		ExecutionCompletedAt: result.CompletedAt,
		ExecutionSeconds:     result.ExecutionSeconds,
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	// Second outbox entry: notify orchestrator
	notifyEntry := &model.K8sStatusOutboxEntry{
		EventType:        "node_status_updated",
		StreamName:       "node.updated:v1",
		TaskID:           cmd.TaskID,
		ScheduleID:       cmd.ScheduleID,
		ScheduleName:     cmd.ScheduleName,
		ServiceName:      cmd.ServiceName,
		SchemaName:       cmd.SchemaName,
		TableName:        cmd.TableName,
		JobName:          cmd.JobName,
		NewTaskStatus:    "FAILED",
		UpdateTaskStatus: false,
		CreateExecution:  false,
	}
	if err := h.uow.OutboxRepo().Create(ctx, notifyEntry); err != nil {
		return fmt.Errorf("failed to create notify outbox entry: %w", err)
	}

	h.logger.Warn("Job failed permanently - outbox entries created",
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

// handleFailedWithRetry handles failed jobs that can be retried
func (h *CheckStatusHandler) handleFailedWithRetry(ctx context.Context, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount int32) error {
	newRetryCount := retryCount + 1

	executionID, logS3Key, logTail := h.fetchAndUploadLogs(ctx, cmd)

	errorMsg := h.truncateErrorMessage(logTail)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(result.TerminationMsg)
	}
	newJobName := retryJobName(cmd.JobName, newRetryCount)

	entry := &model.K8sStatusOutboxEntry{
		EventType:    "task_retry",
		StreamName:   "retry.task:v1",
		TaskID:       cmd.TaskID,
		ScheduleID:   cmd.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      newJobName,
		NodeType:     cmd.NodeType,

		ErrorMessage:   errorMsg,
		TaskRetryCount: int(retryCount),
		ExecutionID:    executionID,
		LogS3Key:       logS3Key,

		UpdateTaskStatus:     true,
		NewTaskStatus:        "failed",
		NewRetryCount:        int(newRetryCount),
		CreateExecution:      true,
		ExecutionStartedAt:   result.StartedAt,
		ExecutionCompletedAt: result.CompletedAt,
		ExecutionSeconds:     result.ExecutionSeconds,
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	h.logger.Warn("Job failed, scheduling retry - outbox entry created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"retry_count", newRetryCount,
		"log_s3_key", logS3Key,
	)

	return nil
}

// handleRunning handles still-running jobs
func (h *CheckStatusHandler) handleRunning(ctx context.Context, cmd command.CheckJobStatus) error {
	checkAfter := time.Now().Add(time.Duration(h.config.CheckDelaySeconds) * time.Second)

	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		maxRetries = int32(h.config.DefaultTaskMaxRetries)
	}

	// Create outbox entry for delayed check
	entry := &model.K8sStatusOutboxEntry{
		EventType:    "check_delayed",
		StreamName:   "check.k8s:v1", // Publish back to check stream for re-check
		TaskID:       cmd.TaskID,
		ScheduleID:   cmd.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,
		NodeType:     cmd.NodeType,

		TaskRetryCount: int(cmd.RetryCount),
		TaskMaxRetries: int(maxRetries),
		CheckAfter:     checkAfter.Unix(),

		UpdateTaskStatus: false, // Don't update task status for running jobs
		CreateExecution:  false, // Don't create execution for running jobs
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	h.logger.Debug("Job still running, scheduling re-check - outbox entry created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"check_after", checkAfter,
	)

	return nil
}

// handleUnknown handles unknown job statuses
func (h *CheckStatusHandler) handleUnknown(ctx context.Context, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	errorMsg := h.truncateErrorMessage(result.TerminationMsg)
	if errorMsg == "" {
		errorMsg = "Job not found or unknown status"
	}

	newRetryCount := cmd.RetryCount + 1

	// Create outbox entry for unknown status (treat as failed permanently)
	entry := &model.K8sStatusOutboxEntry{
		EventType:    "task_failed",
		StreamName:   "task.failed:v1",
		TaskID:       cmd.TaskID,
		ScheduleID:   cmd.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,

		ErrorMessage:   errorMsg,
		TaskRetryCount: int(newRetryCount),

		UpdateTaskStatus: true,
		NewTaskStatus:    "failed",
		NewRetryCount:    int(newRetryCount),
		CreateExecution:  true,
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	h.logger.Error("Job status unknown - outbox entry created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"error", errorMsg,
	)

	return nil
}

// truncateErrorMessage truncates error messages to configured max length
func (h *CheckStatusHandler) truncateErrorMessage(msg string) string {
	if len(msg) > h.config.ErrorMessageMaxLen {
		return msg[:h.config.ErrorMessageMaxLen] + "...[truncated]"
	}
	return msg
}
