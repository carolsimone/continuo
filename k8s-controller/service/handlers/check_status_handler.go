package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	"github.com/carolsimone/continuo/k8s-controller/service/ports"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// validationLabelNamespace is the immutable UUIDv5 namespace used to derive the
// validation_node_completed outbox row's AggregateID from the release-id label.
// It must never change: deriving the aggregate ID deterministically lets a
// re-observed terminal Job map to the same aggregate for downstream dedup.
var validationLabelNamespace = uuid.MustParse("a4f1c2e6-8b3d-4f7a-9c1e-2d6b5a0f3e8c")

// K8sStatusChecker defines interface for checking K8s job status
type K8sStatusChecker interface {
	GetJobStatus(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error)
	GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (fullLog, tail string, err error)
	// GetJobMeta returns the Job's labels and annotations. The `mode` label routes
	// validation vs production; the raw release/node identity for a validation Job
	// is read from the annotations (labels are sanitized and would desync the
	// executor's outcome lookup).
	GetJobMeta(ctx context.Context, namespace, jobName string) (labels, annotations map[string]string, err error)
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
	logUploader        ports.LogUploader
	config             *HandlerConfig
	cancelledSchedules repository.CancelledSchedulesRepository
	logger             *slog.Logger
}

// NewCheckStatusHandler creates a new CheckStatusHandler
func NewCheckStatusHandler(
	k8sClient K8sStatusChecker,
	logUploader ports.LogUploader,
	config *HandlerConfig,
	cancelledSchedules repository.CancelledSchedulesRepository,
	logger *slog.Logger,
) *CheckStatusHandler {
	return &CheckStatusHandler{
		k8sClient:          k8sClient,
		logUploader:        logUploader,
		config:             config,
		cancelledSchedules: cancelledSchedules,
		logger:             logger,
	}
}

// Handle checks a K8s job's status and writes the resulting outbox rows using
// the transaction-scoped repositories on u. The binding owns the transaction
// lifecycle and has already run dedup; msgProcID is accepted for signature
// parity with the standardized handler shape and is currently unused.
func (h *CheckStatusHandler) Handle(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, msgProcID uuid.UUID) error {
	h.logger.Info("Checking K8s job status", "task_id", cmd.TaskID, "job_name", cmd.JobName)

	result, err := h.k8sClient.GetJobStatus(ctx, h.config.K8sNamespace, cmd.JobName)
	if err != nil {
		return fmt.Errorf("failed to get job status: %w", err)
	}

	retryCount := cmd.RetryCount
	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		maxRetries = int32(h.config.DefaultTaskMaxRetries)
	}

	cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled schedules check: %w", err)
	}
	if cancelled {
		h.logger.Info("Schedule cancelled — absorbing job result",
			"schedule_id", cmd.ScheduleID, "job_name", cmd.JobName, "status", result.Status)
		return nil
	}

	// A still-running Job is mode-agnostic: re-poll it by writing a check.k8s:v1
	// ticket. On the next check the Job's mode is re-read and routing recurs, so a
	// validation Job (always Running on the first node.deployed-triggered check) is
	// polled until terminal instead of being checked once and dropped. The Job
	// metadata is only needed to route a terminal result, so it is fetched after
	// this check — a Job spends most of its checks Running, and skipping the extra
	// Get there keeps the re-poll loop to a single API call.
	if result.Status == model.JobStatusRunning {
		return h.handleRunning(ctx, u, cmd)
	}

	labels, annotations, err := h.k8sClient.GetJobMeta(ctx, h.config.K8sNamespace, cmd.JobName)
	if err != nil {
		return fmt.Errorf("fetch job meta: %w", err)
	}

	if labels["mode"] == "validation" {
		return h.handleValidationTerminal(ctx, u, cmd, result, annotations)
	}

	// Empty metadata means the Job is gone (deleted/TTL-reaped): GetJobMeta maps
	// NotFound to empty maps. A vanished Job has no mode label, so it falls through
	// to the production task-status path below — correct for a production Job
	// (whose NotFound→Failed status must still drive the retry/permanent handlers).
	// A vanished *validation* Job cannot be identified here (no annotations to
	// recover release_id/node_id), so its per-node outcome is not emitted; surface
	// it for operators rather than silently writing production rows for it.
	if len(labels) == 0 {
		h.logger.Warn("Job metadata unavailable on terminal check — routing as production; a vanished validation Job will not emit its per-node outcome",
			"job_name", cmd.JobName, "status", result.Status)
	}

	switch result.Status {
	case model.JobStatusSucceeded:
		return h.handleSucceeded(ctx, u, cmd, result)
	case model.JobStatusFailed:
		if retryCount >= maxRetries {
			return h.handleFailedPermanent(ctx, u, cmd, result, retryCount)
		}
		return h.handleFailedWithRetry(ctx, u, cmd, result, retryCount, maxRetries)
	default:
		return h.handleUnknown(ctx, u, cmd, result)
	}
}

// handleSucceeded handles successful job completion.
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (SUCCEEDED)
//   - task_execution_recorded
//   - node_status_updated (→ node.updated:v1)
func (h *CheckStatusHandler) handleSucceeded(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	repo := u.OutboxRepo()
	executionID := uuid.New()

	// Row 1: task_status_updated. Stamp the attempt that ran (cmd.RetryCount)
	// so the SUCCEEDED carries the same retry_count as that attempt's RUNNING;
	// state's attempt-monotonic guard relies on RUNNING and its terminal
	// sharing one attempt number.
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "SUCCEEDED", cmd.RetryCount); err != nil {
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

// handleValidationTerminal emits the per-node validation result for a Job carrying
// the mode=validation label. It writes a single validation_node_completed outbox row
// (→ validation.node.completed:v1) instead of the three production task-status rows.
// release_id and node_id are read from the Job annotations (raw, unsanitized) so
// they match the executor's executor_deployments key; outcome is derived from the
// terminal status. An Unknown status is not terminal — the handler re-polls via the
// shared check.k8s:v1 ticket so a Job that is briefly Unknown (e.g. pods not yet
// scheduled) is re-checked rather than emitting a premature failure. Running is
// handled by the shared re-poll before this function is reached.
func (h *CheckStatusHandler) handleValidationTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	result *model.K8sPodResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd) // not terminal yet; re-poll
	}

	_, logS3Key, _ := h.fetchAndUploadLogs(ctx, cmd)

	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}

	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]
	payload, err := json.Marshal(map[string]any{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": logS3Key,
	})
	if err != nil {
		return fmt.Errorf("marshal validation_node_completed payload: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		AggregateType: "release",
		AggregateID:   uuid.NewSHA1(validationLabelNamespace, []byte("release:"+releaseID)),
		EventType:     "validation_node_completed",
		Payload:       payload,
		StreamName:    streams.ValidationNodeCompletedV1,
		MaxRetries:    3,
	}); err != nil {
		return fmt.Errorf("create validation_node_completed row: %w", err)
	}

	h.logger.Info("Validation Job terminal — validation_node_completed outbox entry created",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
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
func (h *CheckStatusHandler) handleFailedPermanent(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount int32) error {
	repo := u.OutboxRepo()
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
func (h *CheckStatusHandler) handleFailedWithRetry(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount, maxRetries int32) error {
	repo := u.OutboxRepo()
	newRetryCount := retryCount + 1

	executionID, logS3Key, logTail := h.fetchAndUploadLogs(ctx, cmd)

	errorMsg := h.truncateErrorMessage(logTail)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(result.TerminationMsg)
	}
	newJobName := retryJobName(cmd.JobName, newRetryCount)

	// Row 1: task_status_updated (FAILED). Stamp the attempt that just ran
	// (retryCount), not the next attempt — this terminal must carry the same
	// retry_count as that attempt's RUNNING so state's attempt-monotonic guard
	// treats the upcoming retry's RUNNING (newRetryCount = retryCount+1) as a
	// strictly newer attempt and un-fills the slot. The retry itself is
	// dispatched at newRetryCount via the task_retry row below.
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", retryCount); err != nil {
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
func (h *CheckStatusHandler) handleRunning(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus) error {
	repo := u.OutboxRepo()
	checkAfter := time.Now().Add(time.Duration(h.config.CheckDelaySeconds) * time.Second)

	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		maxRetries = int32(h.config.DefaultTaskMaxRetries)
	}

	// Determine the outbox entry ID to carry forward for future dedup; use a new UUID
	// so each check-delayed row has its own identity in the check.k8s:v1 stream.
	outboxEntryID := uuid.New()

	checkPayload, err := json.Marshal(event.JobCheckRequest{
		TaskID:       cmd.TaskID.String(),
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,
		CheckAfter:   checkAfter.Unix(),
		NodeType:     cmd.NodeType,
		ImageTag:     cmd.ImageTag,
		RetryCount:   int(cmd.RetryCount),
		MaxRetries:   int(maxRetries),
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
func (h *CheckStatusHandler) handleUnknown(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.K8sPodResult) error {
	repo := u.OutboxRepo()
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
