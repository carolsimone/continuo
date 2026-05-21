package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	s3adapter "github.com/carolsimone/continuo/k8s-controller/adapters/s3"
	postgresadapter "github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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

// fakeOutboxRepo records Create calls using canonical pkgoutbox.Entry.
type fakeOutboxRepo struct {
	entries []*pkgoutbox.Entry
}

func (r *fakeOutboxRepo) Create(_ context.Context, e *pkgoutbox.Entry) error {
	r.entries = append(r.entries, e)
	return nil
}
func (r *fakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}
func (r *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

// Verify at compile time.
var _ pkgoutbox.Repository = (*fakeOutboxRepo)(nil)

type threadSafeFakeOutboxRepo struct {
	mu      sync.Mutex
	entries []*pkgoutbox.Entry
}

func (r *threadSafeFakeOutboxRepo) Create(_ context.Context, e *pkgoutbox.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
	return nil
}
func (r *threadSafeFakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}
func (r *threadSafeFakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error {
	return nil
}
func (r *threadSafeFakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *threadSafeFakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (r *threadSafeFakeOutboxRepo) entriesSnapshot() []*pkgoutbox.Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*pkgoutbox.Entry(nil), r.entries...)
}

var _ pkgoutbox.Repository = (*threadSafeFakeOutboxRepo)(nil)

type fakeCancelledSchedulesRepo struct {
	ids map[uuid.UUID]bool
}

func (f *fakeCancelledSchedulesRepo) Insert(_ context.Context, id uuid.UUID) error { return nil }
func (f *fakeCancelledSchedulesRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	return f.ids[id], nil
}
func (f *fakeCancelledSchedulesRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ postgresadapter.CancelledSchedulesRepository = (*fakeCancelledSchedulesRepo)(nil)

func noopCancelledRepo() *fakeCancelledSchedulesRepo {
	return &fakeCancelledSchedulesRepo{ids: map[uuid.UUID]bool{}}
}

// fakeMessageProcessingRepo is a fake for testing that allows first call to proceed and second to be a duplicate.
type fakeMessageProcessingRepo struct {
	seen map[string]uuid.UUID
}

func (r *fakeMessageProcessingRepo) InsertIfNotExists(_ context.Context, msgProc *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	if r.seen == nil {
		r.seen = make(map[string]uuid.UUID)
	}
	key := msgProc.MessageID + "\x00" + msgProc.StreamName
	if id, exists := r.seen[key]; exists {
		return id, false, nil // already seen → duplicate
	}
	id := uuid.New()
	r.seen[key] = id
	return id, true, nil // newly inserted
}

func (r *fakeMessageProcessingRepo) GetByMessageIDAndStream(_ context.Context, messageID, streamName string) (*messageprocessing.MessageProcessing, error) {
	key := messageID + "\x00" + streamName
	if id, exists := r.seen[key]; exists {
		return &messageprocessing.MessageProcessing{ID: id, MessageID: messageID, StreamName: streamName, State: "completed"}, nil
	}
	return &messageprocessing.MessageProcessing{ID: uuid.New(), MessageID: messageID, StreamName: streamName, State: "completed"}, nil
}

func (r *fakeMessageProcessingRepo) GetByID(_ context.Context, id uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{ID: id, State: "completed"}, nil
}

func (r *fakeMessageProcessingRepo) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

var _ messageprocessing.Repository = (*fakeMessageProcessingRepo)(nil)

type fakeUnitOfWork struct {
	outboxRepo pkgoutbox.Repository
	mpRepo     messageprocessing.Repository
}

func (u *fakeUnitOfWork) OutboxRepo() pkgoutbox.Repository                    { return u.outboxRepo }
func (u *fakeUnitOfWork) MessageProcessingRepo() messageprocessing.Repository { return u.mpRepo }
func (u *fakeUnitOfWork) Begin(_ context.Context) error                       { return nil }
func (u *fakeUnitOfWork) Commit() error                                       { return nil }
func (u *fakeUnitOfWork) Rollback() error                                     { return nil }

var _ uow.UnitOfWork = (*fakeUnitOfWork)(nil)

func newFakeUoW(outbox pkgoutbox.Repository) *fakeUnitOfWork {
	return &fakeUnitOfWork{outboxRepo: outbox, mpRepo: &fakeMessageProcessingRepo{}}
}

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

func newHandler(k8s handlers.K8sStatusChecker, cancelledSchedules postgresadapter.CancelledSchedulesRepository, defaultMaxRetries int) *handlers.CheckStatusHandler {
	cfg := &handlers.HandlerConfig{
		K8sNamespace:          "default",
		CheckDelaySeconds:     30,
		ErrorMessageMaxLen:    4096,
		LogTailLines:          50,
		DefaultTaskMaxRetries: defaultMaxRetries,
	}
	return handlers.NewCheckStatusHandler(k8s, &fakeLogUploader{}, cfg, cancelledSchedules, slog.Default())
}

// eventTypeOf returns the event_type of the i-th outbox entry.
func eventTypeOf(entries []*pkgoutbox.Entry, i int) string {
	if i >= len(entries) {
		return ""
	}
	return entries[i].EventType
}

// findEntryByEventType returns the first entry with the given event_type.
func findEntryByEventType(entries []*pkgoutbox.Entry, eventType string) *pkgoutbox.Entry {
	for _, e := range entries {
		if e.EventType == eventType {
			return e
		}
	}
	return nil
}

// --- tests ---

// TestHandleFailedWithRetry verifies that a failed job whose retryCount < maxRetries
// produces 3 canonical outbox rows: task_status_updated, task_execution_recorded, task_retry.
func TestHandleFailedWithRetry(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 0, // first failure
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 3 {
		t.Fatalf("expected 3 outbox entries (task_status_updated + task_execution_recorded + task_retry), got %d", len(entries))
	}

	// Check event types
	if got := eventTypeOf(entries, 0); got != "task_status_updated" {
		t.Errorf("entries[0]: expected task_status_updated, got %q", got)
	}
	if got := eventTypeOf(entries, 1); got != "task_execution_recorded" {
		t.Errorf("entries[1]: expected task_execution_recorded, got %q", got)
	}
	if got := eventTypeOf(entries, 2); got != "task_retry" {
		t.Errorf("entries[2]: expected task_retry, got %q", got)
	}

	// Verify task_status_updated payload
	statusEntry := findEntryByEventType(entries, "task_status_updated")
	if statusEntry == nil {
		t.Fatal("missing task_status_updated entry")
	}
	var statusPayload pkgevents.TaskStatusUpdated
	if err := json.Unmarshal(statusEntry.Payload, &statusPayload); err != nil {
		t.Fatalf("unmarshal task_status_updated: %v", err)
	}
	if statusPayload.Status != "FAILED" {
		t.Errorf("task_status_updated status: expected FAILED, got %q", statusPayload.Status)
	}
	// Attempt-consistency invariant (state's attempt-monotonic guard depends on
	// it): the FAILED terminal carries the attempt that just ran (cmd.RetryCount),
	// while the retry is dispatched at the next attempt (cmd.RetryCount+1). The
	// two must differ so the retry's RUNNING is strictly newer than this terminal.
	if statusPayload.RetryCount != cmd.RetryCount {
		t.Errorf("FAILED retry_count: expected %d (attempt that ran), got %d", cmd.RetryCount, statusPayload.RetryCount)
	}
	retryEntry := findEntryByEventType(entries, "task_retry")
	if retryEntry == nil {
		t.Fatal("missing task_retry entry")
	}
	var retryPayload map[string]interface{}
	if err := json.Unmarshal(retryEntry.Payload, &retryPayload); err != nil {
		t.Fatalf("unmarshal task_retry: %v", err)
	}
	if got := int32(retryPayload["retry_count"].(float64)); got != cmd.RetryCount+1 {
		t.Errorf("task_retry retry_count: expected %d (next attempt), got %d", cmd.RetryCount+1, got)
	}
}

// TestHandleSucceededStampsAttemptRetryCount guards against regressing the
// SUCCEEDED row to a hardcoded retry_count: it must carry the attempt that ran
// (cmd.RetryCount) so a late stale RUNNING for the same attempt is recognized as
// not-newer by state's attempt-monotonic guard and ignored.
func TestHandleSucceededStampsAttemptRetryCount(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusSucceeded}},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-ok",
		RetryCount: 2, // succeeded on the 3rd attempt
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	statusEntry := findEntryByEventType(outbox.entries, "task_status_updated")
	if statusEntry == nil {
		t.Fatal("missing task_status_updated entry")
	}
	var statusPayload pkgevents.TaskStatusUpdated
	if err := json.Unmarshal(statusEntry.Payload, &statusPayload); err != nil {
		t.Fatalf("unmarshal task_status_updated: %v", err)
	}
	if statusPayload.Status != "SUCCEEDED" {
		t.Errorf("status: expected SUCCEEDED, got %q", statusPayload.Status)
	}
	if statusPayload.RetryCount != cmd.RetryCount {
		t.Errorf("SUCCEEDED retry_count: expected %d (attempt that ran), got %d", cmd.RetryCount, statusPayload.RetryCount)
	}
}

func TestCheckStatusHandler_Handle_AllowsConcurrentCalls(t *testing.T) {
	repo := &threadSafeFakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, noopCancelledRepo(), 3)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u := &fakeUnitOfWork{outboxRepo: repo, mpRepo: &fakeMessageProcessingRepo{}}
			errs <- handler.Handle(context.Background(), u, command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "job-abc",
				RetryCount: 0,
				MaxRetries: 3,
			}, uuid.Nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if got := len(repo.entriesSnapshot()); got != 6 {
		t.Fatalf("expected 6 outbox entries (2 calls × 3 rows), got %d", got)
	}
}

// TestHandleFailedPermanent verifies that a permanently failed job produces
// 3 canonical outbox rows: task_status_updated, task_execution_recorded, node_status_updated.
func TestHandleFailedPermanent(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 3, // exhausted
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 3 {
		t.Fatalf("expected 3 outbox entries (task_status_updated + task_execution_recorded + node_status_updated), got %d", len(entries))
	}

	if got := eventTypeOf(entries, 0); got != "task_status_updated" {
		t.Errorf("entries[0]: expected task_status_updated, got %q", got)
	}
	if got := eventTypeOf(entries, 1); got != "task_execution_recorded" {
		t.Errorf("entries[1]: expected task_execution_recorded, got %q", got)
	}
	if got := eventTypeOf(entries, 2); got != "node_status_updated" {
		t.Errorf("entries[2]: expected node_status_updated, got %q", got)
	}
}

// TestHandleRunningCarriesRetryInfo verifies that a running job's check_delayed entry
// carries RetryCount and MaxRetries forward in its payload.
func TestHandleRunningCarriesRetryInfo(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-running",
		RetryCount: 1,
		MaxRetries: 5,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 {
		t.Fatalf("expected 1 outbox entry (check_delayed), got %d", len(entries))
	}
	entry := entries[0]
	if entry.EventType != "check_delayed" {
		t.Errorf("expected event_type=check_delayed, got %q", entry.EventType)
	}

	// Verify payload carries retry info
	var payload map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		t.Fatalf("unmarshal check_delayed: %v", err)
	}
	if got := int(payload["retry_count"].(float64)); got != 1 {
		t.Errorf("expected retry_count=1, got %d", got)
	}
	if got := int(payload["max_retries"].(float64)); got != 5 {
		t.Errorf("expected max_retries=5, got %d", got)
	}
}

// TestDefaultMaxRetriesAppliedWhenZero verifies backward-compat: when cmd.MaxRetries==0
// (old executor-controller message with no max_retries field), the handler falls back to
// config.DefaultTaskMaxRetries.  With RetryCount=0 and default=3 the job should be retried.
func TestDefaultMaxRetriesAppliedWhenZero(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-legacy",
		RetryCount: 0,
		MaxRetries: 0, // absent from message
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	// RetryCount(0) < defaultMaxRetries(3) → retry path → last row is task_retry
	lastEntry := entries[len(entries)-1]
	if lastEntry.EventType != "task_retry" {
		t.Errorf("expected last event_type=task_retry (default max_retries applied), got %q", lastEntry.EventType)
	}
}

// TestCheckStatusHandler_FailsPermanentlyAfter3TotalAttempts documents the invariant:
// retryCount=2 (3rd attempt) with maxRetries=2 must produce a permanent failure.
func TestCheckStatusHandler_FailsPermanentlyAfter3TotalAttempts(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, noopCancelledRepo(), 2)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 2, // 0-indexed; 2 = third attempt
		MaxRetries: 2,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	// Permanent fail path produces: task_status_updated + task_execution_recorded + node_status_updated
	hasNodeUpdated := false
	for _, e := range entries {
		if e.EventType == "node_status_updated" {
			hasNodeUpdated = true
		}
	}
	if !hasNodeUpdated {
		t.Error("expected node_status_updated entry for permanent failure")
	}
}

func TestCheckStatusHandler_DropsOutboxWhenScheduleCancelled(t *testing.T) {
	scheduleID := uuid.New()
	cancelledRepo := &fakeCancelledSchedulesRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	outbox := &fakeOutboxRepo{}

	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusSucceeded}},
		cancelledRepo,
		3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		JobName:    "job-1",
		MaxRetries: 3,
	}

	err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(outbox.entries) != 0 {
		t.Errorf("expected no outbox entries for cancelled schedule, got %d", len(outbox.entries))
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

	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: notFoundResult}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "service-1-e2e-schema-table-b-a1b2c3d4",
		RetryCount: 0, // first occurrence
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) == 0 {
		t.Fatal("expected outbox entries, got none")
	}
	// Retry path → last entry is task_retry
	lastEntry := entries[len(entries)-1]
	if lastEntry.EventType != "task_retry" {
		t.Errorf("expected last event_type=task_retry (not_found retried), got %q", lastEntry.EventType)
	}
}

// TestNotFoundPermanentFailureNotifiesOrchestrator verifies that when "Job not found" retries
// are exhausted, handleFailedPermanent is called — which writes task_status_updated,
// task_execution_recorded, and node_status_updated entries, so the orchestrator cascades failure.
func TestNotFoundPermanentFailureNotifiesOrchestrator(t *testing.T) {
	notFoundResult := &model.K8sPodResult{
		Status:         model.JobStatusFailed,
		TerminationMsg: "Job not found in Kubernetes",
	}

	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: notFoundResult}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "service-1-e2e-schema-table-b-a1b2c3d4",
		RetryCount: 3, // exhausted
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 outbox entries, got %d", len(entries))
	}

	eventTypes := make([]string, len(entries))
	for i, e := range entries {
		eventTypes[i] = e.EventType
	}

	hasTaskStatusUpdated := findEntryByEventType(entries, "task_status_updated") != nil
	hasNodeUpdated := findEntryByEventType(entries, "node_status_updated") != nil

	if !hasTaskStatusUpdated {
		t.Errorf("expected task_status_updated entry, got event_types: %v", eventTypes)
	}
	if !hasNodeUpdated {
		t.Errorf("expected node_status_updated entry, got event_types: %v", eventTypes)
	}

	// Verify the node_status_updated stream is correct
	nodeEntry := findEntryByEventType(entries, "node_status_updated")
	if nodeEntry != nil && nodeEntry.StreamName != "node.updated:v1" {
		t.Errorf("node_status_updated stream: expected node.updated:v1, got %q", nodeEntry.StreamName)
	}
}

// TestHandleSucceeded verifies that a succeeded job produces 3 canonical rows:
// task_status_updated (SUCCEEDED), task_execution_recorded, node_status_updated.
func TestHandleSucceeded(t *testing.T) {
	now := time.Now()
	succeededResult := &model.K8sPodResult{
		Status:           model.JobStatusSucceeded,
		StartedAt:        &now,
		CompletedAt:      &now,
		ExecutionSeconds: 5.0,
	}

	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{status: succeededResult}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-succeeded",
		RetryCount: 0,
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 3 {
		t.Fatalf("expected 3 outbox entries for succeeded job, got %d", len(entries))
	}

	// Row order: task_status_updated, task_execution_recorded, node_status_updated
	if got := eventTypeOf(entries, 0); got != "task_status_updated" {
		t.Errorf("entries[0]: expected task_status_updated, got %q", got)
	}
	if got := eventTypeOf(entries, 1); got != "task_execution_recorded" {
		t.Errorf("entries[1]: expected task_execution_recorded, got %q", got)
	}
	if got := eventTypeOf(entries, 2); got != "node_status_updated" {
		t.Errorf("entries[2]: expected node_status_updated, got %q", got)
	}

	// Verify task_status_updated payload has SUCCEEDED
	var statusPayload pkgevents.TaskStatusUpdated
	if err := json.Unmarshal(entries[0].Payload, &statusPayload); err != nil {
		t.Fatalf("unmarshal task_status_updated: %v", err)
	}
	if statusPayload.Status != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %q", statusPayload.Status)
	}
	if statusPayload.RetryCount != 0 {
		t.Errorf("expected retry_count=0, got %d", statusPayload.RetryCount)
	}

	// Verify task_execution_recorded payload
	var execPayload pkgevents.TaskExecutionRecorded
	if err := json.Unmarshal(entries[1].Payload, &execPayload); err != nil {
		t.Fatalf("unmarshal task_execution_recorded: %v", err)
	}
	if execPayload.JobName != "job-succeeded" {
		t.Errorf("expected job_name=job-succeeded, got %q", execPayload.JobName)
	}
	if execPayload.ExecutionSeconds != 5.0 {
		t.Errorf("expected execution_seconds=5.0, got %f", execPayload.ExecutionSeconds)
	}
}

// Compile-time interface checks.
var _ s3adapter.LogUploader = (*fakeLogUploader)(nil)
var _ handlers.K8sStatusChecker = (*fakeK8sClient)(nil)
