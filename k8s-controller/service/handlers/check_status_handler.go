package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	"github.com/carolsimone/continuo/k8s-controller/service/ports"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/parsecache"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/validationresult"
	"github.com/google/uuid"
)

// validationLabelNamespace is the immutable UUIDv5 namespace used to derive the
// validation_node_completed outbox row's AggregateID from the release-id label.
// It must never change: deriving the aggregate ID deterministically lets a
// re-observed terminal Job map to the same aggregate for downstream dedup.
var validationLabelNamespace = uuid.MustParse("a4f1c2e6-8b3d-4f7a-9c1e-2d6b5a0f3e8c")

// seedBuildLabelNamespace is the immutable UUIDv5 namespace used to derive the
// seed_build_node_completed outbox row's AggregateID from the release-id annotation.
// Must never change — same dedup guarantee as validationLabelNamespace.
var seedBuildLabelNamespace = uuid.MustParse("c7a3e1d9-5f2b-4e6c-8d0a-1b4f7c2e9a3b")

// compileLabelNamespace is the immutable UUIDv5 namespace used to derive the
// compile_node_completed outbox row's AggregateID from the release-id annotation.
// Must never change — same dedup guarantee as validationLabelNamespace and
// seedBuildLabelNamespace. Distinct from both to avoid aggregate-ID collisions
// between compile, seed-build, and validation events for the same release.
var compileLabelNamespace = uuid.MustParse("e2b8d4f6-1a3c-5e7f-9b0d-2c4e6a8f0b2d")

// SplitValidationResult removes the structured-result sentinel block from a
// validation pod log and returns the cleaned log plus the inner single-line
// JSON. The sentinel markers are the shared cross-language contract in
// pkg/validationresult (the Python pod emits them; a guard test binds the two
// sides). When no well-formed block is present (production jobs, old images,
// truncated logs) it returns the log unchanged and an empty structured string —
// the caller then degrades to the text-log-only path.
func SplitValidationResult(log string) (cleanLog, structuredJSON string) {
	bi := strings.Index(log, validationresult.SentinelBegin)
	if bi < 0 {
		return log, ""
	}
	ei := strings.Index(log, validationresult.SentinelEnd)
	if ei < 0 || ei < bi {
		return log, ""
	}
	inner := strings.TrimSpace(log[bi+len(validationresult.SentinelBegin) : ei])
	clean := log[:bi] + log[ei+len(validationresult.SentinelEnd):]
	return clean, strings.TrimSpace(inner)
}

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

// DefaultLogIOTimeout bounds the best-effort pod-log fetch and its S3 uploads
// when HandlerConfig leaves LogIOTimeout unset. It must stay comfortably below
// the consumer's per-handler deadline: the log I/O runs before the terminal
// outbox writes and shares their context, so whatever it spends is taken from
// the budget those writes need to persist a task's outcome.
const DefaultLogIOTimeout = 20 * time.Second

// HandlerConfig contains handler configuration
type HandlerConfig struct {
	K8sNamespace          string
	CheckDelaySeconds     int
	ErrorMessageMaxLen    int
	LogTailLines          int64
	DefaultTaskMaxRetries int // used when max_retries is absent from the inbound message
	// LogIOTimeout bounds the pod-log fetch and its S3 uploads. Zero selects
	// DefaultLogIOTimeout.
	LogIOTimeout time.Duration
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
		converted, err := num.Int32(h.config.DefaultTaskMaxRetries, "default_task_max_retries")
		if err != nil {
			return fmt.Errorf("check job status: %w", err)
		}
		maxRetries = converted
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

	if labels["mode"] == pkgevents.ModeValidation {
		return h.handleValidationTerminal(ctx, u, cmd, result, annotations)
	}

	if labels["mode"] == pkgevents.ModeSeedBuild {
		return h.handleSeedBuildTerminal(ctx, u, cmd, result, annotations)
	}

	if labels["mode"] == pkgevents.ModeCompile {
		return h.handleCompileTerminal(ctx, u, cmd, result, annotations)
	}

	// Legacy promote-seed Jobs queued by a previous version have synthetic task
	// IDs with no run in state, so their lifecycle stays suppressed. Current
	// promoted-seed work carries no mode label and falls through to the
	// production path below. See events.ModePromoteSeed.
	if labels["mode"] == pkgevents.ModePromoteSeed {
		h.logger.Info("Legacy promote-seed Job terminal — no lifecycle events emitted",
			"job_name", cmd.JobName, "status", result.Status)
		return nil
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

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))

	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}

	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]
	payloadMap := map[string]any{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": logS3Key,
	}
	if runResultsURI != "" {
		payloadMap["run_results_uri"] = runResultsURI
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return fmt.Errorf("marshal validation_node_completed payload: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		AggregateType: "release",
		AggregateID:   uuid.NewSHA1(validationLabelNamespace, []byte("release:"+releaseID)),
		EventType:     event.EventTypeValidationNodeCompleted,
		Payload:       payload,
		StreamName:    streams.ValidationNodeCompletedV1,
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
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

// handleSeedBuildTerminal emits the per-seed build result for a Job carrying the
// mode=seed_build label. It writes a single seed_build_node_completed outbox row
// (→ seed.build.node.completed:v1) instead of the three production task-status rows.
// release_id and node_id are read from the Job annotations (raw, unsanitized) so
// they match the executor's executor_deployments key; outcome is derived from the
// terminal status. Unknown status is not terminal — re-poll via the shared
// check.k8s:v1 ticket. Running is handled before this function is reached.
func (h *CheckStatusHandler) handleSeedBuildTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	result *model.K8sPodResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd) // not terminal yet; re-poll
	}

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))

	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}

	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]
	payloadMap := map[string]any{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": logS3Key,
	}
	if runResultsURI != "" {
		payloadMap["run_results_uri"] = runResultsURI
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return fmt.Errorf("marshal seed_build_node_completed payload: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		AggregateType: "release",
		AggregateID:   uuid.NewSHA1(seedBuildLabelNamespace, []byte("release:"+releaseID)),
		EventType:     event.EventTypeSeedBuildNodeCompleted,
		Payload:       payload,
		StreamName:    streams.SeedBuildNodeCompletedV1,
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
	}); err != nil {
		return fmt.Errorf("create seed_build_node_completed row: %w", err)
	}

	h.logger.Info("Seed-build Job terminal — seed_build_node_completed outbox entry created",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
	)
	return nil
}

// handleCompileTerminal emits the per-compile result for a Job carrying the
// mode=compile label. It writes a single compile_node_completed outbox row
// (→ compile.node.completed:v1) instead of the three production task-status rows.
// release_id and node_id are read from the Job annotations (raw, unsanitized) so
// they match the executor's executor_deployments key; outcome is derived from the
// terminal status. Unknown status is not terminal — re-poll via the shared
// check.k8s:v1 ticket. Running is handled before this function is reached.
// Unlike validation, no stdout result block is parsed — the manifest went to S3
// via the compile Job's upload container, so outcome is purely the Job's success/failure.
// dbt_log_uri and run_results_uri may be empty.
func (h *CheckStatusHandler) handleCompileTerminal(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.CheckJobStatus,
	result *model.K8sPodResult,
	annotations map[string]string,
) error {
	if result.Status == model.JobStatusUnknown {
		return h.handleRunning(ctx, u, cmd) // not terminal yet; re-poll
	}

	_, logS3Key, runResultsURI, _, _ := h.fetchAndUploadLogs(ctx, cmd, compileArtifactPath(cmd))

	outcome := "failed"
	if result.Status == model.JobStatusSucceeded {
		outcome = "ok"
	}

	releaseID := annotations[pkgmodel.AnnotationReleaseID]
	nodeID := annotations[pkgmodel.AnnotationNodeID]
	payloadMap := map[string]any{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": logS3Key,
	}
	if runResultsURI != "" {
		payloadMap["run_results_uri"] = runResultsURI
	}
	if result.FailedContainer != "" {
		payloadMap["failed_container"] = result.FailedContainer
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return fmt.Errorf("marshal compile_node_completed payload: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		AggregateType: "release",
		AggregateID:   uuid.NewSHA1(compileLabelNamespace, []byte("release:"+releaseID)),
		EventType:     event.EventTypeCompileNodeCompleted,
		Payload:       payload,
		StreamName:    streams.CompileNodeCompletedV1,
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
	}); err != nil {
		return fmt.Errorf("create compile_node_completed row: %w", err)
	}

	h.logger.Info("Compile Job terminal — compile_node_completed outbox entry created",
		"job_name", cmd.JobName,
		"release_id", releaseID,
		"node_id", nodeID,
		"outcome", outcome,
	)
	return nil
}

// sentinelResult is the subset of the structured validation-result contract
// fetchAndUploadLogs reads. The block carries more fields (failures,
// unique_id); only schema_version, status, and message are consumed here —
// schema_version to validate the contract (see parseSentinelResult), status
// and message to build the error message — so this mirrors just those three
// rather than the full contract in
// remediation/domain/failure.StructuredResult.
type sentinelResult struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Message       string `json:"message"`
}

// supportedRunStatuses is the structured-result contract's status vocabulary —
// dbt's RunStatus, per continuo_validation_contract/result.py's docstring. A
// scanned candidate is accepted only when its status is one of these, so an
// unrelated status-bearing JSON object elsewhere in the log (e.g. a sidecar
// diagnostic) cannot pass as the contract's result block on status alone.
var supportedRunStatuses = map[string]bool{
	"success": true,
	"error":   true,
	"fail":    true,
	"skipped": true,
}

// parseSentinelResult decodes the JSON object inside a structured
// validation-result block (the string SplitValidationResult already isolated
// between the sentinel markers). Real pod logs can carry a stderr preamble
// before the JSON object, and/or trailing text after it, inside those markers
// — the same condition remediation/domain/failure.ParseStructuredResult was
// fixed to tolerate — so this scans for the object rather than unmarshalling
// the whole body: try each '{' in turn, decode a single JSON value from there
// (ignoring anything after it), and accept the first candidate whose
// schema_version matches the wire contract (validationresult.SchemaVersion)
// and whose status is one of supportedRunStatuses. Because the scan tolerates
// arbitrary preamble text, it can otherwise reach an unrelated status-bearing
// JSON object before the real result; requiring both checks is what tells the
// two apart — a candidate that decodes but fails either one is not the
// contract's block, so scanning continues to the next '{' rather than
// accepting it. ok is false when no such object is found.
//
// This guard is only as good as validationresult.SchemaVersion: a future
// contract schema bump must update that constant too, or every block from the
// new schema is silently rejected here and the error message quietly
// degrades to the log tail.
func parseSentinelResult(raw string) (sr sentinelResult, ok bool) {
	body := []byte(raw)
	for start := bytes.IndexByte(body, '{'); start >= 0; {
		var candidate sentinelResult
		if err := json.NewDecoder(bytes.NewReader(body[start:])).Decode(&candidate); err == nil &&
			candidate.SchemaVersion == validationresult.SchemaVersion &&
			supportedRunStatuses[candidate.Status] {
			return candidate, true
		}
		next := bytes.IndexByte(body[start+1:], '{')
		if next < 0 {
			break
		}
		start += next + 1
	}
	return sentinelResult{}, false
}

// fetchAndUploadLogs fetches pod logs and uploads them to S3. The text log (with
// any structured-result sentinel block stripped) is uploaded under logs/...; when
// the pod emitted a structured block, that JSON is uploaded separately under
// run-results/... and its key returned as runResultsURI. Validation pods and
// python-model containers emit one; dbt containers do not.
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
// task's outcome, so a finished task would be retried and eventually poison-ACKed
// while still recorded as RUNNING. Capping the I/O keeps the outcome writable
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
func (h *CheckStatusHandler) fetchAndUploadLogs(
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
	cleanLog, structured := SplitValidationResult(fullLog)

	// GetPodLogs fetches the full log and the tail independently and each
	// soft-fails on its own, so the full log can come back empty while the
	// tail — the pod's last output, which is exactly where the sentinel block
	// sits — still carries the complete block. This fallback is scoped to the
	// error message only: cleanLog and the run-results upload below still use
	// the full-log block exclusively, never the tail's.
	sentinelJSON := structured
	if sentinelJSON == "" {
		_, sentinelJSON = SplitValidationResult(tail)
	}

	// A non-success block's message is the real cause of the failure and is
	// preferred over the log tail below. A "success" block belongs to a pod
	// that completed its work and then crashed for an unrelated reason (e.g. a
	// container-level failure); its message (e.g. "rows=42") describes the
	// successful run, not the crash, so it must not become the error message.
	if sentinelJSON != "" {
		if sr, ok := parseSentinelResult(sentinelJSON); ok && sr.Status != "success" {
			sentinelErrMsg = strings.TrimSpace(sr.Message)
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
//   - node_status_updated (→ node.updated:v1)
func (h *CheckStatusHandler) handleFailedPermanent(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus, result *model.K8sPodResult, retryCount int32) error {
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

	executionID, logS3Key, runResultsURI, logTail, sentinelErrMsg := h.fetchAndUploadLogs(ctx, cmd, nodeArtifactPath(cmd))
	errorMsg := h.resolveErrorMessage(sentinelErrMsg, logTail, result.TerminationMsg)
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
	if err := h.writeTaskExecutionRecorded(ctx, repo, cmd, executionID, result, errorMsg, logS3Key, runResultsURI); err != nil {
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
		Operation:    cmd.Operation,
	})
	if err != nil {
		return fmt.Errorf("marshal task_retry: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{
		AggregateType: "task",
		AggregateID:   cmd.TaskID,
		EventType:     event.EventTypeTaskRetry,
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

// handleRunning handles a still-running Job. The first time an attempt is observed
// running (RunningAnnounced == false) it announces the task as RUNNING — making
// k8s-controller the sole producer of the task's running/terminal pod lifecycle —
// then re-enqueues a check_delayed ticket. The announcement is mode-aware:
// mode=validation Jobs use synthetic task IDs and carry no real task status, so
// their RUNNING is suppressed; the forward ticket still sets running_announced so
// metadata is not re-read on every poll. RUNNING is stamped with cmd.RetryCount so
// it shares the attempt number of that attempt's terminal, which state's
// attempt-monotonic guard relies on. The announcement and the forward ticket are
// written in the same transaction, so the flag and the announcement never diverge.
func (h *CheckStatusHandler) handleRunning(ctx context.Context, u uow.UnitOfWork, cmd command.CheckJobStatus) error {
	repo := u.OutboxRepo()

	if !cmd.RunningAnnounced {
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

	checkAfter := time.Now().Add(time.Duration(h.config.CheckDelaySeconds) * time.Second)

	maxRetries := cmd.MaxRetries
	if maxRetries == 0 {
		converted, err := num.Int32(h.config.DefaultTaskMaxRetries, "default_task_max_retries")
		if err != nil {
			return fmt.Errorf("check job status: %w", err)
		}
		maxRetries = converted
	}

	// Determine the outbox entry ID to carry forward for future dedup; use a new UUID
	// so each check-delayed row has its own identity in the check.k8s:v1 stream.
	outboxEntryID := uuid.New()

	checkPayload, err := json.Marshal(event.JobCheckRequest{
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
		Operation:        cmd.Operation,
		RetryCount:       int(cmd.RetryCount),
		MaxRetries:       int(maxRetries),
		RunningAnnounced: true,
	})
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
		EventType:     event.EventTypeTaskFailed,
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
		EventType:     event.EventTypeTaskStatusUpdated,
		Payload:       payload,
		StreamName:    streams.TaskStatusUpdatedV1,
	})
}

// parseCacheFromResult derives the parse_cache observability fields from the
// hydrate-parse-cache initContainer's termination message. Absent container
// (pre-feature Jobs, validation Jobs) -> ("",""): the fields are omitted.
func parseCacheFromResult(result *model.K8sPodResult) (state, reason string) {
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
func (h *CheckStatusHandler) writeTaskExecutionRecorded(
	ctx context.Context,
	repo pkgoutbox.Repository,
	cmd command.CheckJobStatus,
	executionID uuid.UUID,
	result *model.K8sPodResult,
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
		EventType:     event.EventTypeNodeStatusUpdated,
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
func (h *CheckStatusHandler) resolveErrorMessage(sentinelErrMsg, tail, terminationMsg string) string {
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
func (h *CheckStatusHandler) truncateErrorMessage(msg string) string {
	if len(msg) > h.config.ErrorMessageMaxLen {
		return msg[:h.config.ErrorMessageMaxLen] + "...[truncated]"
	}
	return msg
}
