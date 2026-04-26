package handlers_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/google/uuid"
)

// --- fakes ---

type fakeK8sClient struct {
	status *model.K8sPodResult
	err    error
}

func (f *fakeK8sClient) GetJobStatus(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
	return f.status, f.err
}

func (f *fakeK8sClient) GetPodLogs(_ context.Context, _, _ string, _ int64) (string, string, error) {
	return "", "", nil
}

type fakeLogUploader struct{}

func (f *fakeLogUploader) UploadLog(_ context.Context, _ string, _ string) error { return nil }

// fakeOutboxRepo records Create calls.
type fakeOutboxRepo struct {
	entries []*model.K8sStatusOutboxEntry
}

func (r *fakeOutboxRepo) Create(_ context.Context, e *model.K8sStatusOutboxEntry) error {
	r.entries = append(r.entries, e)
	return nil
}
func (r *fakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*model.K8sStatusOutboxEntry, error) {
	return nil, nil
}
func (r *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }
func (r *fakeOutboxRepo) GetStuckEntries(_ context.Context, _ int, _ int) ([]*model.K8sStatusOutboxEntry, error) {
	return nil, nil
}
func (r *fakeOutboxRepo) ForceMarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

// Verify at compile time.
var _ postgres.OutboxRepository = (*fakeOutboxRepo)(nil)

type fakeProcessedEventsRepo struct{}

func (r *fakeProcessedEventsRepo) TryMarkProcessed(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil // always "not yet processed" → proceed
}

var _ postgres.ProcessedEventsRepository = (*fakeProcessedEventsRepo)(nil)

// fakeUoW is an in-memory unit of work backed by the fakes above.
type fakeUoW struct {
	outboxRepo *fakeOutboxRepo
}

func newFakeUoW() *fakeUoW {
	return &fakeUoW{outboxRepo: &fakeOutboxRepo{}}
}

func (u *fakeUoW) Begin(_ context.Context) error                              { return nil }
func (u *fakeUoW) Commit() error                                              { return nil }
func (u *fakeUoW) Rollback() error                                            { return nil }
func (u *fakeUoW) OutboxRepo() postgres.OutboxRepository                      { return u.outboxRepo }
func (u *fakeUoW) ProcessedEventsRepo() postgres.ProcessedEventsRepository    { return &fakeProcessedEventsRepo{} }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

// --- helpers ---

func failedResult() *model.K8sPodResult {
	now := time.Now()
	return &model.K8sPodResult{
		Status:           model.JobStatusFailed,
		TerminationMsg:   "OOMKilled",
		StartedAt:        &now,
		CompletedAt:      &now,
		ExecutionSeconds: 1.0,
	}
}

func newHandler(k8s handlers.K8sStatusChecker, unitOfWork uow.UnitOfWork, defaultMaxRetries int) *handlers.CheckStatusHandler {
	cfg := &handlers.HandlerConfig{
		K8sNamespace:          "default",
		CheckDelaySeconds:     30,
		ErrorMessageMaxLen:    4096,
		LogTailLines:          50,
		DefaultTaskMaxRetries: defaultMaxRetries,
	}
	return handlers.NewCheckStatusHandler(k8s, unitOfWork, &fakeLogUploader{}, cfg, slog.Default())
}

// --- tests ---

// TestHandleFailedWithRetry verifies that a failed job whose retryCount < maxRetries
// produces a "task_retry" outbox entry — using cmd fields, no gRPC.
func TestHandleFailedWithRetry(t *testing.T) {
	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: failedResult()}, fw, 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 0, // first failure
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	if got := fw.outboxRepo.entries[0].EventType; got != "task_retry" {
		t.Errorf("expected event_type=task_retry, got %q", got)
	}
}

// TestHandleFailedPermanent verifies that a failed job whose retryCount >= maxRetries
// produces a "task_failed" outbox entry — using cmd fields, no gRPC.
func TestHandleFailedPermanent(t *testing.T) {
	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: failedResult()}, fw, 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 3, // exhausted
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	if got := fw.outboxRepo.entries[0].EventType; got != "task_failed" {
		t.Errorf("expected event_type=task_failed, got %q", got)
	}
}

// TestHandleRunningCarriesRetryInfo verifies that a running job's check_delayed entry
// carries TaskRetryCount and TaskMaxRetries forward so the next check cycle
// doesn't need to call state gRPC.
func TestHandleRunningCarriesRetryInfo(t *testing.T) {
	fw := newFakeUoW()
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}},
		fw, 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-running",
		RetryCount: 1,
		MaxRetries: 5,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entry, got none")
	}
	entry := fw.outboxRepo.entries[0]
	if entry.EventType != "check_delayed" {
		t.Errorf("expected event_type=check_delayed, got %q", entry.EventType)
	}
	if entry.TaskRetryCount != 1 {
		t.Errorf("expected TaskRetryCount=1, got %d", entry.TaskRetryCount)
	}
	if entry.TaskMaxRetries != 5 {
		t.Errorf("expected TaskMaxRetries=5, got %d", entry.TaskMaxRetries)
	}
}

// TestDefaultMaxRetriesAppliedWhenZero verifies backward-compat: when cmd.MaxRetries==0
// (old executor-controller message with no max_retries field), the handler falls back to
// config.DefaultTaskMaxRetries.  With RetryCount=0 and default=3 the job should be retried.
func TestDefaultMaxRetriesAppliedWhenZero(t *testing.T) {
	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: failedResult()}, fw, 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-legacy",
		RetryCount: 0,
		MaxRetries: 0, // absent from message
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entry, got none")
	}
	// RetryCount(0) < defaultMaxRetries(3) → should retry, not permanent fail
	if got := fw.outboxRepo.entries[0].EventType; got != "task_retry" {
		t.Errorf("expected event_type=task_retry (default max_retries applied), got %q", got)
	}
}

// TestCheckStatusHandler_FailsPermanentlyAfter3TotalAttempts documents the invariant:
// retryCount=2 (3rd attempt) with maxRetries=2 must produce a permanent failure.
func TestCheckStatusHandler_FailsPermanentlyAfter3TotalAttempts(t *testing.T) {
	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: failedResult()}, fw, 2)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 2, // 0-indexed; 2 = third attempt
		MaxRetries: 2,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	if got := fw.outboxRepo.entries[0].EventType; got != "task_failed" {
		t.Errorf("expected event_type=task_failed, got %q", got)
	}
}

// TestNotFoundRetry verifies that when GetJobStatus returns "Job not found in Kubernetes"
// (now JobStatusFailed), with retryCount < maxRetries, the handler creates a task_retry
// outbox entry — not a permanent failure — so the job gets re-created and re-checked.
func TestNotFoundRetry(t *testing.T) {
	notFoundResult := &model.K8sPodResult{
		Status:         model.JobStatusFailed,
		TerminationMsg: "Job not found in Kubernetes",
	}

	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: notFoundResult}, fw, 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "service-1-e2e-schema-table-b-a1b2c3d4",
		RetryCount: 0, // first occurrence
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) == 0 {
		t.Fatal("expected outbox entry, got none")
	}
	if got := fw.outboxRepo.entries[0].EventType; got != "task_retry" {
		t.Errorf("expected event_type=task_retry (not_found retried), got %q", got)
	}
}

// TestNotFoundPermanentFailureNotifiesOrchestrator verifies that when "Job not found" retries
// are exhausted, handleFailedPermanent is called — which writes both task_failed AND
// node_status_updated (node.updated:v1) entries, so the orchestrator cascades failure.
func TestNotFoundPermanentFailureNotifiesOrchestrator(t *testing.T) {
	notFoundResult := &model.K8sPodResult{
		Status:         model.JobStatusFailed,
		TerminationMsg: "Job not found in Kubernetes",
	}

	fw := newFakeUoW()
	handler := newHandler(&fakeK8sClient{status: notFoundResult}, fw, 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "service-1-e2e-schema-table-b-a1b2c3d4",
		RetryCount: 3, // exhausted
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fw.outboxRepo.entries) < 2 {
		t.Fatalf("expected 2 outbox entries (task_failed + node_status_updated), got %d", len(fw.outboxRepo.entries))
	}

	eventTypes := make([]string, len(fw.outboxRepo.entries))
	for i, e := range fw.outboxRepo.entries {
		eventTypes[i] = e.EventType
	}

	hasTaskFailed := false
	hasNodeUpdated := false
	for _, e := range fw.outboxRepo.entries {
		if e.EventType == "task_failed" {
			hasTaskFailed = true
		}
		if e.EventType == "node_status_updated" && e.StreamName == "node.updated:v1" {
			hasNodeUpdated = true
		}
	}

	if !hasTaskFailed {
		t.Errorf("expected task_failed entry, got event_types: %v", eventTypes)
	}
	if !hasNodeUpdated {
		t.Errorf("expected node_status_updated entry on node.updated:v1, got event_types: %v", eventTypes)
	}
}

// Compile-time interface checks.
var _ s3adapter.LogUploader = (*fakeLogUploader)(nil)
var _ handlers.K8sStatusChecker = (*fakeK8sClient)(nil)
