package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/parsecache"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/validationresult"
	"github.com/google/uuid"
)

// DefaultLogIOTimeout bounds the best-effort pod-log fetch and its S3 uploads
// when JobStatusConfig leaves LogIOTimeout unset. It must stay comfortably below
// the consumer's per-handler deadline: the log I/O runs before the terminal
// outbox writes and shares their context, so whatever it spends is taken from
// the budget those writes need to persist a task's outcome.
const DefaultLogIOTimeout = 20 * time.Second

// JobStatusConfig contains handler configuration
type JobStatusConfig struct {
	K8sNamespace       string
	CheckDelaySeconds  int
	ErrorMessageMaxLen int
	LogTailLines       int64
	// LogIOTimeout bounds the pod-log fetch and its S3 uploads. Zero selects
	// DefaultLogIOTimeout.
	LogIOTimeout time.Duration
}

// JobStatusHandler handles CheckJobStatus commands
type JobStatusHandler struct {
	k8sClient          ports.JobObserver
	logUploader        ports.LogUploader
	config             *JobStatusConfig
	cancelledSchedules repository.CancelledSchedulesRepository
	outcomes           *outcomes.Recorder
	logger             *slog.Logger
}

// NewJobStatusHandler creates a new JobStatusHandler
func NewJobStatusHandler(
	k8sClient ports.JobObserver,
	logUploader ports.LogUploader,
	config *JobStatusConfig,
	cancelledSchedules repository.CancelledSchedulesRepository,
	recorder *outcomes.Recorder,
	logger *slog.Logger,
) *JobStatusHandler {
	return &JobStatusHandler{
		k8sClient:          k8sClient,
		logUploader:        logUploader,
		config:             config,
		cancelledSchedules: cancelledSchedules,
		outcomes:           recorder,
		logger:             logger,
	}
}

// Handle checks a K8s job's status and writes the resulting outbox rows using
// the transaction-scoped repositories on u. The binding owns the transaction
// lifecycle and has already run dedup; msgProcID is accepted for signature
// parity with the standardized handler shape and is currently unused.
//
// The handler also records the attempt's outcome on the Job's deployment row,
// releasing the admission slot the row held, in the same transaction as the
// announcements. A row that already has an outcome makes the check a no-op, so
// each Job's result is reported exactly once.
func (h *JobStatusHandler) Handle(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, msgProcID uuid.UUID) error {
	h.logger.Info("Checking K8s job status", "task_id", cmd.TaskID, "job_name", cmd.JobName)

	result, err := h.k8sClient.GetJobStatus(ctx, h.config.K8sNamespace, cmd.JobName)
	if err != nil {
		return fmt.Errorf("failed to get job status: %w", err)
	}

	// The deployment row whose Job this is, locked for this transaction. Its
	// recorded outcome is what makes the Job's result reported exactly once:
	// a second status check for the same Job (a duplicate ticket, or one the
	// admission reconciler issued) finds it and writes nothing.
	dep, err := h.deploymentFor(ctx, u, cmd.JobName)
	if err != nil {
		return err
	}
	if dep != nil && dep.HasOutcome() {
		h.logger.Info("Job outcome already recorded — ignoring the status check",
			"job_name", cmd.JobName, "deployment_id", dep.ID())
		return nil
	}

	retryCount := cmd.RetryCount
	maxRetries := taskRetryBudget(cmd.MaxRetries)

	cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled schedules check: %w", err)
	}
	if cancelled {
		return h.handleCancelled(ctx, u, cmd, result, dep)
	}

	// A still-running Job is mode-agnostic: re-poll it by writing a check.k8s:v1
	// ticket. On the next check the Job's mode is re-read and routing recurs, so a
	// validation Job (always Running on the dispatcher's first check) is
	// polled until terminal instead of being checked once and dropped. The Job
	// metadata is only needed to route a terminal result, so it is fetched after
	// this check — a Job spends most of its checks Running, and skipping the extra
	// Get there keeps the re-poll loop to a single API call.
	if result.Status == model.JobStatusRunning {
		return h.handleRunning(ctx, u, cmd, dep, true)
	}

	labels, annotations, err := h.k8sClient.GetJobMeta(ctx, h.config.K8sNamespace, cmd.JobName)
	if err != nil {
		return fmt.Errorf("fetch job meta: %w", err)
	}
	mode := labels["mode"]
	// A vanished Job (deleted, or TTL-reaped) has no labels: GetJobMeta maps
	// NotFound to empty maps. Its row still knows what it ran, so a candidate
	// Job's outcome reaches its release.
	if len(labels) == 0 && dep != nil && dep.Mode() != model.ModeProduction {
		mode, annotations = candidateRoute(dep)
	}

	switch mode {
	case pkgevents.ModeValidation:
		return h.handleValidationTerminal(ctx, u, cmd, dep, result, annotations)
	case pkgevents.ModeSeedBuild:
		return h.handleSeedBuildTerminal(ctx, u, cmd, dep, result, annotations)
	case pkgevents.ModeCompile:
		return h.handleCompileTerminal(ctx, u, cmd, dep, result, annotations)
	case pkgevents.ModePromoteSeed:
		// Legacy promote-seed Jobs queued by a previous version have synthetic
		// task IDs with no run in state, so their lifecycle stays suppressed;
		// only the slot is released. See events.ModePromoteSeed.
		h.logger.Info("Legacy promote-seed Job terminal — no lifecycle events emitted",
			"job_name", cmd.JobName, "status", result.Status)
		return h.finishAttempt(ctx, u, dep, attemptOutcome(result))
	}

	// A Job with neither labels nor a row cannot be identified; production is
	// the only mode whose handlers can act on it.
	if len(labels) == 0 && dep == nil {
		h.logger.Warn("Job metadata and deployment row unavailable on terminal check — routing as production",
			"job_name", cmd.JobName, "status", result.Status)
	}

	switch result.Status {
	case model.JobStatusSucceeded:
		return h.handleSucceeded(ctx, u, cmd, dep, result)
	case model.JobStatusFailed:
		if retryCount >= maxRetries {
			return h.handleFailedPermanent(ctx, u, cmd, dep, result, retryCount)
		}
		return h.handleFailedWithRetry(ctx, u, cmd, dep, result, retryCount, maxRetries)
	default:
		return h.handleUnknown(ctx, u, cmd, dep, result, retryCount, maxRetries)
	}
}

// deploymentFor returns the deployment row of jobName, or nil when no row
// names that Job.
func (h *JobStatusHandler) deploymentFor(ctx context.Context, u uow.UnitOfWork, jobName string) (*model.Deployment, error) {
	dep, err := u.DeploymentsRepo().GetByJobName(ctx, jobName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load deployment for job %s: %w", jobName, err)
	}
	return dep, nil
}

// candidateRoute rebuilds the mode label and release/node annotations of a
// candidate deployment's Job from its row.
func candidateRoute(dep *model.Deployment) (string, map[string]string) {
	modes := map[model.Mode]string{
		model.ModeValidation: pkgevents.ModeValidation,
		model.ModeSeedBuild:  pkgevents.ModeSeedBuild,
		model.ModeCompile:    pkgevents.ModeCompile,
	}
	return modes[dep.Mode()], map[string]string{
		pkgmodel.AnnotationReleaseID: dep.ReleaseID(),
		pkgmodel.AnnotationNodeID:    dep.NodeID(),
	}
}

// attemptOutcome is the outcome a production attempt's row records.
func attemptOutcome(result *model.JobResult) string {
	if result.Status == model.JobStatusSucceeded {
		return "ok"
	}
	return "failed"
}

// finishAttempt records a production Job's outcome on its deployment row,
// releasing the slot it held. A Job without a row has nothing to release.
func (h *JobStatusHandler) finishAttempt(ctx context.Context, u uow.UnitOfWork, dep *model.Deployment, outcome string) error {
	if dep == nil {
		return nil
	}
	if err := dep.Finish(outcome, time.Now()); err != nil {
		return fmt.Errorf("finish deployment %s: %w", dep.ID(), err)
	}
	return u.DeploymentsRepo().Save(ctx, dep)
}

// handleCancelled observes a Job whose schedule was cancelled without
// announcing anything for it: a Job still running is checked again so its slot
// is released once it finishes, and a finished Job's outcome is recorded on its
// row.
func (h *JobStatusHandler) handleCancelled(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.JobResult, dep *model.Deployment) error {
	if result.Status == model.JobStatusRunning {
		return h.handleRunning(ctx, u, cmd, dep, false)
	}
	h.logger.Info("Schedule cancelled — absorbing job result",
		"schedule_id", cmd.ScheduleID, "job_name", cmd.JobName, "status", result.Status)
	if dep != nil && dep.Mode() != model.ModeProduction {
		if !dep.Status().InFlight() {
			return nil
		}
		if err := dep.ReleaseSlot(); err != nil {
			return fmt.Errorf("release deployment %s: %w", dep.ID(), err)
		}
		return u.DeploymentsRepo().Save(ctx, dep)
	}
	return h.finishAttempt(ctx, u, dep, attemptOutcome(result))
}

// taskRetryBudget returns the task's retry budget carried on the check ticket,
// or pkgevents.DefaultTaskMaxRetries when the ticket carries none.
func taskRetryBudget(maxRetries int32) int32 {
	if maxRetries <= 0 {
		return pkgevents.DefaultTaskMaxRetries
	}
	return maxRetries
}

// handleSucceeded handles successful job completion.
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (SUCCEEDED)
//   - task_execution_recorded
//   - node_updated (→ node.updated:v1)
func (h *JobStatusHandler) handleSucceeded(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, dep *model.Deployment, result *model.JobResult) error {
	repo := u.OutboxRepo()

	// A successful run's pod output is uploaded exactly as a failed one's is:
	// the pod is garbage-collected at the Job's TTL, so leaving it unuploaded
	// reduces a finished run to timings with no evidence of what it did. Each
	// upload soft-fails to an empty key, so S3 being unavailable cannot turn a
	// success into a failure. The log tail is discarded — a successful
	// execution carries no error message.
	executionID, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))

	// Row 1: task_status_updated. Stamp the attempt that ran (cmd.RetryCount)
	// so the SUCCEEDED carries the same retry_count as that attempt's RUNNING;
	// state's attempt-monotonic guard relies on RUNNING and its terminal
	// sharing one attempt number.
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "SUCCEEDED", cmd.RetryCount); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded
	if err := h.writeTaskExecutionRecorded(ctx, repo, cmd, executionID, result, "", logS3Key, runResultsURI); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// Row 3: node_updated → node.updated:v1
	if err := h.writeNodeUpdated(ctx, repo, cmd, "SUCCEEDED"); err != nil {
		return fmt.Errorf("node_updated: %w", err)
	}

	h.logger.Info("Job succeeded — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"execution_time", result.ExecutionSeconds,
	)

	return h.finishAttempt(ctx, u, dep, "ok")
}

// handleValidationTerminal records the terminal result for a Job carrying the
// mode=validation label directly onto its deployments row and settles the
// release's validation leg — unblocking or skipping downstream nodes and
// emitting the per-node projection plus, once every node has settled, the
// terminal aggregate — all in the same transaction as this observation.
// release_id and node_id are read from the Job annotations (raw, unsanitized) so
// they match the dispatcher's deployments key; outcome is derived from the
// terminal status. An Unknown status is not terminal — the handler re-polls via the
// shared check.k8s:v1 ticket so a Job that is briefly Unknown (e.g. pods not yet
// scheduled) is re-checked rather than emitting a premature failure. Running is
// handled by the shared re-poll before this function is reached.
func (h *JobStatusHandler) handleValidationTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	dep *model.Deployment,
	result *model.JobResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd, dep, true) // not terminal yet; re-poll
	}

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))
	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}
	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]

	if err := h.outcomes.Record(ctx, u, outcomes.NodeOutcome{
		Mode:          model.ModeValidation,
		ReleaseID:     releaseID,
		NodeID:        nodeID,
		Outcome:       outcome,
		DBTLogURI:     logS3Key,
		RunResultsURI: runResultsURI,
	}); err != nil {
		return fmt.Errorf("record validation outcome: %w", err)
	}

	h.logger.Info("Validation Job terminal, outcome recorded",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
	)
	return nil
}

// handleSeedBuildTerminal records the terminal result for a Job carrying the
// mode=seed_build label directly onto its deployments row and settles the
// release's seed-build leg — emitting the terminal aggregate once every seed has
// settled — all in the same transaction as this observation. release_id and
// node_id are read from the Job annotations (raw, unsanitized) so they match the
// dispatcher's deployments key; outcome is derived from the terminal status.
// Unknown status is not terminal — re-poll via the shared check.k8s:v1 ticket.
// Running is handled before this function is reached.
func (h *JobStatusHandler) handleSeedBuildTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	dep *model.Deployment,
	result *model.JobResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd, dep, true) // not terminal yet; re-poll
	}

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))
	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}
	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]

	if err := h.outcomes.Record(ctx, u, outcomes.NodeOutcome{
		Mode:          model.ModeSeedBuild,
		ReleaseID:     releaseID,
		NodeID:        nodeID,
		Outcome:       outcome,
		DBTLogURI:     logS3Key,
		RunResultsURI: runResultsURI,
	}); err != nil {
		return fmt.Errorf("record seed-build outcome: %w", err)
	}

	h.logger.Info("Seed-build Job terminal, outcome recorded",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
	)
	return nil
}

// handleCompileTerminal records the terminal result for a Job carrying the
// mode=compile label directly onto its deployments row and settles the
// release's compile leg — emitting the terminal aggregate once the compile node
// has settled — all in the same transaction as this observation. release_id and
// node_id are read from the Job annotations (raw, unsanitized) so they match the
// dispatcher's deployments key; outcome is derived from the terminal status.
// Unknown status is not terminal — re-poll via the shared check.k8s:v1 ticket.
// Running is handled before this function is reached. Unlike validation, no
// stdout result block is parsed — the manifest went to S3 via the compile Job's
// upload container, so outcome is purely the Job's success/failure. dbt_log_uri
// and run_results_uri may be empty.
func (h *JobStatusHandler) handleCompileTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	dep *model.Deployment,
	result *model.JobResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd, dep, true) // not terminal yet; re-poll
	}

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, compileArtifactPath(cmd))
	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}
	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]

	if err := h.outcomes.Record(ctx, u, outcomes.NodeOutcome{
		Mode:            model.ModeCompile,
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		Outcome:         outcome,
		DBTLogURI:       logS3Key,
		RunResultsURI:   runResultsURI,
		FailedContainer: result.FailedContainer,
	}); err != nil {
		return fmt.Errorf("record compile outcome: %w", err)
	}

	h.logger.Info("Compile Job terminal, outcome recorded",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
	)
	return nil
}

// fetchAndUploadLogs fetches pod logs and uploads them to S3. The text log (with
// any structured-result sentinel block stripped) is uploaded under logs/...; when
// the pod emitted a structured block, that JSON is uploaded separately under
// run-results/... and its key returned as runResultsURI. Validation pods and
// python-family production containers (python-node, python-csv, python-api) emit one;
// dbt containers do not.
// Returns the log tail and the sentinel block's message (for error_message —
// empty when no block is present, the block fails to decode, or its status is
// "success"), both S3 keys, and a pre-generated execution ID. Each upload
// soft-fails independently to an empty key on error.
//
// All of this I/O runs under its own deadline, derived from — but shorter than —
// the caller's handler budget. It is best-effort observability that precedes the
// terminal outbox writes, and those writes share the caller's context with the
// transaction they run in: a slow pod-log read or an unreachable S3 that
// consumed the whole handler budget here would leave no budget to persist the
// task's outcome, so a finished task would be retried and eventually
// dead-lettered while still recorded as RUNNING. Capping the I/O keeps the outcome writable
// even when the log never arrives. Cancellation still propagates from the parent,
// so a shutting-down consumer is not held open by an upload.
// nodeArtifactPath files a Job's log and run-results under the node the Job ran.
func nodeArtifactPath(cmd command.CheckJobStatus) string {
	return fmt.Sprintf("%s/%s/%s", cmd.ServiceName, cmd.SchemaName, cmd.TableName)
}

// compileArtifactPath files a release's compile leg under the service and the leg
// that produced it. The compile Job runs no node, so it carries no schema and no
// table; addressing it as a node yields "<service>///<id>.log". MinIO rejects
// those empty path segments outright (XMinioInvalidObjectName) while AWS S3
// accepts them, so on any install using the bundled MinIO the compile log was
// silently dropped — including the log of a *failed* compile, which is the one a
// rejected release most needs.
func compileArtifactPath(cmd command.CheckJobStatus) string {
	return cmd.ServiceName + "/compile"
}

// artifactPath identifies which Job produced a log or run-results object; see
// nodeArtifactPath and compileArtifactPath for the two shapes it takes.
func (h *JobStatusHandler) fetchAndUploadLogs(
	ctx context.Context,
	cmd command.CheckJobStatus,
	artifactPath string,
) (executionID uuid.UUID, logS3Key, runResultsURI, tail, sentinelErrMsg string) {
	executionID = uuid.New()

	budget := h.config.LogIOTimeout
	if budget <= 0 {
		budget = DefaultLogIOTimeout
	}
	ioCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	fullLog, logTail, err := h.k8sClient.GetPodLogs(ioCtx, h.config.K8sNamespace, cmd.JobName, h.config.LogTailLines)
	if err != nil {
		h.logger.Warn("Failed to fetch pod logs",
			"job_name", cmd.JobName,
			"error", err,
		)
		return executionID, "", "", "", ""
	}

	tail = logTail

	// Separate the structured validation-result block (if any) from the text log.
	// Validation pods and the python production harness emit the block; dbt
	// production jobs do not, so cleanLog == fullLog for them.
	cleanLog, structured := validationresult.Split(fullLog)

	// GetPodLogs fetches the full log and the tail independently and each
	// soft-fails on its own, so the full log can come back empty while the
	// tail — the pod's last output, which is exactly where the sentinel block
	// sits — still carries the complete block. This fallback is scoped to the
	// error message only: cleanLog and the run-results upload below still use
	// the full-log block exclusively, never the tail's.
	sentinelJSON := structured
	if sentinelJSON == "" {
		_, sentinelJSON = validationresult.Split(tail)
	}

	// A non-success block's message is the real cause of the failure and is
	// preferred over the log tail below. A "success" block belongs to a pod
	// that completed its work and then crashed for an unrelated reason (e.g. a
	// container-level failure); its message (e.g. "rows=42") describes the
	// successful run, not the crash, so it must not become the error message.
	if sentinelJSON != "" {
		if r, err := validationresult.Parse([]byte(sentinelJSON)); err == nil && r.Status != "success" {
			sentinelErrMsg = strings.TrimSpace(r.Message)
		}
	}

	if cleanLog == "" {
		h.logger.Warn("Pod log is empty, skipping S3 upload", "job_name", cmd.JobName)
	} else {
		key := fmt.Sprintf("logs/task-executions/%s/%s.log", artifactPath, executionID.String())
		if err := h.logUploader.UploadLog(ioCtx, key, cleanLog); err != nil {
			h.logger.Warn("Failed to upload pod log to S3 — continuing without full log",
				"job_name", cmd.JobName,
				"key", key,
				"error", err,
			)
		} else {
			logS3Key = key
			h.logger.Info("Uploaded pod log to S3", "key", key, "job_name", cmd.JobName)
		}
	}

	if structured != "" {
		rrKey := fmt.Sprintf("run-results/task-executions/%s/%s.json", artifactPath, executionID.String())
		if err := h.logUploader.UploadLog(ioCtx, rrKey, structured); err != nil {
			h.logger.Warn("Failed to upload run-results to S3 — continuing without structured result",
				"job_name", cmd.JobName,
				"key", rrKey,
				"error", err,
			)
		} else {
			runResultsURI = rrKey
			h.logger.Info("Uploaded run-results to S3", "key", rrKey, "job_name", cmd.JobName)
		}
	}

	return executionID, logS3Key, runResultsURI, tail, sentinelErrMsg
}

// handleFailedPermanent handles permanently failed jobs (retry_count >= max_retries).
// Writes 3 canonical outbox rows in the transaction:
//   - task_status_updated (FAILED)
//   - task_execution_recorded
//   - node_updated (→ node.updated:v1)
func (h *JobStatusHandler) handleFailedPermanent(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, dep *model.Deployment, result *model.JobResult, retryCount int32) error {
	repo := u.OutboxRepo()
	newRetryCount := retryCount

	executionID, logS3Key, runResultsURI, logTail, sentinelErrMsg := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))
	errorMsg := h.resolveErrorMessage(sentinelErrMsg, logTail, result.TerminationMsg)

	// Row 1: task_status_updated (FAILED)
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", int32(newRetryCount)); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded
	if err := h.writeTaskExecutionRecorded(ctx, repo, cmd, executionID, result, errorMsg, logS3Key, runResultsURI); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// Row 3: node_updated → node.updated:v1
	if err := h.writeNodeUpdated(ctx, repo, cmd, "FAILED"); err != nil {
		return fmt.Errorf("node_updated: %w", err)
	}

	h.logger.Warn("Job failed permanently — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"retry_count", newRetryCount,
		"error", errorMsg,
		"log_s3_key", logS3Key,
	)
	return h.finishAttempt(ctx, u, dep, "failed")
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
// Writes 2 canonical outbox rows in the transaction:
//   - task_status_updated (FAILED)
//   - task_execution_recorded
//
// and re-queues the task as a new pending deployment — the -rN retry Job — via
// createDeployment, in the SAME unit of work as the two rows above, so the
// retry and the FAILED announcement cannot diverge.
func (h *JobStatusHandler) handleFailedWithRetry(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, dep *model.Deployment, result *model.JobResult, retryCount, maxRetries int32) error {
	repo := u.OutboxRepo()
	newRetryCount := retryCount + 1

	executionID, logS3Key, runResultsURI, logTail, sentinelErrMsg := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))
	errorMsg := h.resolveErrorMessage(sentinelErrMsg, logTail, result.TerminationMsg)
	newJobName := retryJobName(cmd.JobName, newRetryCount)

	// Row 1: task_status_updated (FAILED). Stamp the attempt that just ran
	// (retryCount), not the next attempt — this terminal must carry the same
	// retry_count as that attempt's RUNNING so state's attempt-monotonic guard
	// treats the upcoming retry's RUNNING (newRetryCount = retryCount+1) as a
	// strictly newer attempt and un-fills the slot. The retry itself is
	// dispatched at newRetryCount via the pending deployment queued below.
	if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "FAILED", retryCount); err != nil {
		return fmt.Errorf("task_status_updated: %w", err)
	}

	// Row 2: task_execution_recorded (for the failed attempt)
	if err := h.writeTaskExecutionRecorded(ctx, repo, cmd, executionID, result, errorMsg, logS3Key, runResultsURI); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}

	// The retry is a new pending deployment for the same task: the dispatcher
	// creates the -rN Job, and its own first check ticket follows. Same unit of
	// work as the FAILED announcement, so the two cannot diverge.
	if err := createDeployment(ctx, u, events.QueryModel{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
		JobName: newJobName, NodeType: pkgmodel.NodeType(cmd.NodeType), ImageTag: cmd.ImageTag,
		SecretRef: cmd.SecretRef,
		Operation: pkgmodel.Operation(cmd.Operation),
	}, uuid.Nil, int(newRetryCount), int(maxRetries)); err != nil {
		return fmt.Errorf("queue retry deployment: %w", err)
	}

	h.logger.Warn("Job failed, scheduling retry — outbox entries created",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"retry_count", newRetryCount,
		"log_s3_key", logS3Key,
	)

	return h.finishAttempt(ctx, u, dep, "failed")
}

// handleRunning handles a still-running Job. The first time an attempt is observed
// running (RunningAnnounced == false) it announces the task as RUNNING — making
// the job-status handler the sole producer of the task's running/terminal pod
// lifecycle — then re-enqueues a check_delayed ticket. The announcement is mode-aware:
// mode=validation Jobs use synthetic task IDs and carry no real task status, so
// their RUNNING is suppressed; the forward ticket still sets running_announced so
// metadata is not re-read on every poll. RUNNING is stamped with cmd.RetryCount so
// it shares the attempt number of that attempt's terminal, which state's
// attempt-monotonic guard relies on. The announcement and the forward ticket are
// written in the same transaction, so the flag and the announcement never diverge.
//
// announce is false for a Job whose schedule was cancelled: it is still polled so
// its slot is released once it finishes, but RUNNING is never announced for it.
// The first observation of a running Job also moves its row from starting to
// running.
func (h *JobStatusHandler) handleRunning(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, dep *model.Deployment, announce bool) error {
	repo := u.OutboxRepo()

	if announce && !cmd.RunningAnnounced {
		labels, _, err := h.k8sClient.GetJobMeta(ctx, h.config.K8sNamespace, cmd.JobName)
		if err != nil {
			return fmt.Errorf("fetch job meta for running announcement: %w", err)
		}
		if labels["mode"] != pkgevents.ModeValidation && labels["mode"] != pkgevents.ModeSeedBuild && labels["mode"] != pkgevents.ModeCompile && labels["mode"] != pkgevents.ModePromoteSeed {
			if err := h.writeTaskStatusUpdated(ctx, repo, cmd.TaskID, cmd.ScheduleID, "RUNNING", cmd.RetryCount); err != nil {
				return fmt.Errorf("task_status_updated RUNNING: %w", err)
			}
		}
	}

	// A row reconciliation already released (done, outcome unset) stays done;
	// the Job is still polled so its terminal records the outcome.
	if dep != nil && dep.Status().InFlight() {
		changed, err := dep.MarkRunning()
		if err != nil {
			return fmt.Errorf("mark deployment %s running: %w", dep.ID(), err)
		}
		if changed {
			if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
				return fmt.Errorf("save running deployment: %w", err)
			}
		}
	}

	checkAfter := time.Now().Add(time.Duration(h.config.CheckDelaySeconds) * time.Second)

	maxRetries := taskRetryBudget(cmd.MaxRetries)

	// Determine the outbox entry ID to carry forward for future dedup; use a new UUID
	// so each check-delayed row has its own identity in the check.k8s:v1 stream.
	outboxEntryID := uuid.New()

	checkPayload, err := json.Marshal(serialization.JobCheckRequestFromDomain(event.JobCheckRequest{
		TaskID:           cmd.TaskID.String(),
		ScheduleID:       cmd.ScheduleID.String(),
		ScheduleName:     cmd.ScheduleName,
		ServiceName:      cmd.ServiceName,
		SchemaName:       cmd.SchemaName,
		TableName:        cmd.TableName,
		JobName:          cmd.JobName,
		CheckAfter:       checkAfter.Unix(),
		NodeType:         cmd.NodeType,
		ImageTag:         cmd.ImageTag,
		SecretRef:        cmd.SecretRef,
		Operation:        cmd.Operation,
		RetryCount:       int(cmd.RetryCount),
		MaxRetries:       int(maxRetries),
		RunningAnnounced: true,
	}))
	if err != nil {
		return fmt.Errorf("marshal check_delayed: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{
		ID:            outboxEntryID,
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     event.EventTypeCheckDelayed,
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

// handleUnknown records a production Job whose status is Unknown as a
// permanent failure, writing the same rows as handleFailedPermanent:
// task_status_updated (FAILED), task_execution_recorded and node_updated
// (FAILED). An Unknown status gets no retry Job, so the FAILED status reports
// the task's retry budget as spent: state finalizes a run only once no failed
// task has attempts left (retry_count < max_retries), and a lower count would
// hold the run open until the watchdog cancels it.
func (h *JobStatusHandler) handleUnknown(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, dep *model.Deployment, result *model.JobResult, retryCount, maxRetries int32) error {
	unknown := *result
	if unknown.TerminationMsg == "" {
		unknown.TerminationMsg = "Job not found or unknown status"
	}

	h.logger.Error("Job status unknown — recording a permanent failure",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"error", unknown.TerminationMsg,
	)

	return h.handleFailedPermanent(ctx, u, cmd, dep, &unknown, max(retryCount, maxRetries))
}

// writeTaskStatusUpdated writes a task_status_updated canonical outbox row.
func (h *JobStatusHandler) writeTaskStatusUpdated(
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
		EventType:     event.EventTypeTaskStatusUpdated,
		Payload:       payload,
		StreamName:    streams.TaskStatusUpdatedV1,
	})
}

// parseCacheFromResult derives the parse_cache observability fields from the
// hydrate-parse-cache initContainer's termination message. Absent container
// (pre-feature Jobs, validation Jobs) -> ("",""): the fields are omitted.
func parseCacheFromResult(result *model.JobResult) (state, reason string) {
	msg, ok := result.InitTerminationMessages[parsecache.ContainerName]
	if !ok {
		return "", ""
	}
	switch {
	case msg == parsecache.Hydrated:
		return parsecache.Hydrated, ""
	case strings.HasPrefix(msg, parsecache.DegradedPrefix):
		return "degraded", strings.TrimPrefix(msg, parsecache.DegradedPrefix)
	default:
		return "unknown", ""
	}
}

// writeTaskExecutionRecorded writes a task_execution_recorded canonical outbox
// row. logS3Key and runResultsURI name the S3 objects the pod's output was
// uploaded to: the text log, and the structured result block when the pod
// printed one. Either is empty when its upload failed or did not apply.
func (h *JobStatusHandler) writeTaskExecutionRecorded(
	ctx context.Context,
	repo pkgoutbox.Repository,
	cmd command.CheckJobStatus,
	executionID uuid.UUID,
	result *model.JobResult,
	errorMsg string,
	logS3Key string,
	runResultsURI string,
) error {
	exec := pkgevents.TaskExecutionRecorded{
		ExecutionID:      executionID.String(),
		TaskID:           cmd.TaskID.String(),
		JobName:          cmd.JobName,
		ExecutionSeconds: result.ExecutionSeconds,
		ErrorMessage:     errorMsg,
		LogS3Key:         logS3Key,
		RunResultsURI:    runResultsURI,
	}
	if result.StartedAt != nil {
		exec.StartedAt = result.StartedAt.UTC().Format(time.RFC3339)
	}
	if result.CompletedAt != nil {
		exec.CompletedAt = result.CompletedAt.UTC().Format(time.RFC3339)
	}
	exec.ParseCache, exec.ParseCacheReason = parseCacheFromResult(result)

	payload, err := json.Marshal(exec)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     event.EventTypeTaskExecutionRecorded,
		Payload:       payload,
		StreamName:    streams.TaskExecutionRecordedV1,
	})
}

// writeNodeUpdated writes a node_updated canonical outbox row (→ node.updated:v1).
func (h *JobStatusHandler) writeNodeUpdated(
	ctx context.Context,
	repo pkgoutbox.Repository,
	cmd command.CheckJobStatus,
	status string,
) error {
	payload, err := json.Marshal(serialization.NodeUpdatedFromDomain(event.NodeUpdated{
		TaskID:       cmd.TaskID.String(),
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		Status:       status,
	}))
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     event.EventTypeNodeUpdated,
		Payload:       payload,
		StreamName:    streams.NodeUpdatedV1,
	})
}

// resolveErrorMessage applies the error_message precedence shared by
// handleFailedPermanent and handleFailedWithRetry: the sentinel block's
// message wins when present (it is the actual failure cause), else the raw
// log tail, else the pod's K8s termination message. Each candidate is run
// through truncateErrorMessage first, so the fallthrough decision is made on
// the truncated form.
func (h *JobStatusHandler) resolveErrorMessage(sentinelErrMsg, tail, terminationMsg string) string {
	errorMsg := h.truncateErrorMessage(sentinelErrMsg)
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(tail)
	}
	if errorMsg == "" {
		errorMsg = h.truncateErrorMessage(terminationMsg)
	}
	return errorMsg
}

// truncateErrorMessage truncates error messages to configured max length
func (h *JobStatusHandler) truncateErrorMessage(msg string) string {
	if len(msg) > h.config.ErrorMessageMaxLen {
		return msg[:h.config.ErrorMessageMaxLen] + "...[truncated]"
	}
	return msg
}
