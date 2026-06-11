package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/ports"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// --- fakes ---

type fakeK8sClient struct {
	status      *model.K8sPodResult
	err         error
	labels      map[string]string
	annotations map[string]string
}

func (f *fakeK8sClient) GetJobStatus(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
	return f.status, f.err
}

func (f *fakeK8sClient) GetPodLogs(_ context.Context, _, _ string, _ int64) (string, string, error) {
	return "", "", nil
}

func (f *fakeK8sClient) GetJobMeta(_ context.Context, _, _ string) (labels, annotations map[string]string, err error) {
	return f.labels, f.annotations, nil
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
func (r *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error        { return nil }
func (r *fakeOutboxRepo) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
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
func (r *threadSafeFakeOutboxRepo) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error {
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

var _ repository.CancelledSchedulesRepository = (*fakeCancelledSchedulesRepo)(nil)

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

func (r *fakeMessageProcessingRepo) DeleteTerminalOlderThan(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
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

func newHandler(k8s handlers.K8sStatusChecker, cancelledSchedules repository.CancelledSchedulesRepository, defaultMaxRetries int) *handlers.CheckStatusHandler {
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
	if nodeEntry != nil && nodeEntry.StreamName != streams.NodeUpdatedV1 {
		t.Errorf("node_status_updated stream: expected %s, got %q", streams.NodeUpdatedV1, nodeEntry.StreamName)
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

// TestHandle_ValidationModeLabel_WritesValidationNodeCompletedOutboxRowOnly verifies
// that a succeeded Job carrying mode=validation emits exactly one
// validation_node_completed row (outcome=ok, release_id/node_id from labels) and
// none of the three production task-status rows.
func TestHandle_ValidationModeLabel_WritesValidationNodeCompletedOutboxRowOnly(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusSucceeded},
			labels: map[string]string{"mode": "validation"},
			annotations: map[string]string{
				pkgmodel.AnnotationReleaseID: "rel-123",
				pkgmodel.AnnotationNodeID:    "node-abc",
			},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "validate-node-abc",
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 outbox entry (validation_node_completed), got %d", len(entries))
	}
	entry := entries[0]
	if entry.EventType != "validation_node_completed" {
		t.Errorf("expected event_type=validation_node_completed, got %q", entry.EventType)
	}
	if entry.StreamName != streams.ValidationNodeCompletedV1 {
		t.Errorf("expected stream=%s, got %q", streams.ValidationNodeCompletedV1, entry.StreamName)
	}
	if entry.AggregateType != "release" {
		t.Errorf("expected aggregate_type=release, got %q", entry.AggregateType)
	}

	// Ensure no production rows leaked in.
	for _, e := range entries {
		switch e.EventType {
		case "task_status_updated", "task_execution_recorded", "node_status_updated", "task_retry", "task_failed":
			t.Errorf("unexpected production outbox row %q for validation Job", e.EventType)
		}
	}

	var payload map[string]any
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		t.Fatalf("unmarshal validation_node_completed: %v", err)
	}
	if payload["release_id"] != "rel-123" {
		t.Errorf("expected release_id=rel-123, got %v", payload["release_id"])
	}
	if payload["node_id"] != "node-abc" {
		t.Errorf("expected node_id=node-abc, got %v", payload["node_id"])
	}
	if payload["outcome"] != "ok" {
		t.Errorf("expected outcome=ok, got %v", payload["outcome"])
	}
}

// TestHandle_ValidationModeLabel_FailedStatus_OutcomeFailed verifies a failed
// validation Job emits a single row with outcome=failed.
func TestHandle_ValidationModeLabel_FailedStatus_OutcomeFailed(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: failedResult(),
			labels: map[string]string{"mode": "validation"},
			annotations: map[string]string{
				pkgmodel.AnnotationReleaseID: "rel-9",
				pkgmodel.AnnotationNodeID:    "node-z",
			},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "validate-node-z",
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(outbox.entries) != 1 {
		t.Fatalf("expected exactly 1 outbox entry, got %d", len(outbox.entries))
	}
	var payload map[string]any
	if err := json.Unmarshal(outbox.entries[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["outcome"] != "failed" {
		t.Errorf("expected outcome=failed, got %v", payload["outcome"])
	}
}

// TestHandle_ValidationModeLabel_RunningStatus_WritesCheckK8sRepoll verifies a
// still-running (or briefly Unknown) validation Job is re-polled: the handler
// writes exactly one check.k8s:v1 re-poll ticket and no validation.node.completed
// or production rows. Without this the Job would be checked once and dropped,
// hanging the release.
func TestHandle_ValidationModeLabel_RunningStatus_WritesCheckK8sRepoll(t *testing.T) {
	for _, status := range []model.JobStatus{model.JobStatusRunning, model.JobStatusUnknown} {
		outbox := &fakeOutboxRepo{}
		handler := newHandler(
			&fakeK8sClient{
				status: &model.K8sPodResult{Status: status},
				labels: map[string]string{"mode": "validation"},
				annotations: map[string]string{
					pkgmodel.AnnotationReleaseID: "rel-1",
					pkgmodel.AnnotationNodeID:    "node-1",
				},
			},
			noopCancelledRepo(), 3,
		)

		cmd := command.CheckJobStatus{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			JobName:    "validate-node-1",
			MaxRetries: 3,
		}

		if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
			t.Fatalf("Handle (%s): %v", status, err)
		}
		entries := outbox.entries
		if len(entries) != 1 {
			t.Fatalf("status %s: expected exactly 1 check.k8s:v1 re-poll row, got %d", status, len(entries))
		}
		if entries[0].EventType != "check_delayed" {
			t.Errorf("status %s: expected event_type=check_delayed, got %q", status, entries[0].EventType)
		}
		if entries[0].StreamName != streams.CheckK8sV1 {
			t.Errorf("status %s: expected stream=%s, got %q", status, streams.CheckK8sV1, entries[0].StreamName)
		}
		for _, e := range entries {
			switch e.EventType {
			case "validation_node_completed", "task_status_updated", "task_execution_recorded", "node_status_updated", "task_retry", "task_failed":
				t.Errorf("status %s: unexpected non-repoll row %q for running validation Job", status, e.EventType)
			}
		}
	}
}

// TestHandle_ValidationModeLabel_RunningThenSucceeded_EmitsNodeCompletedOnlyOnTerminal
// drives the re-poll lifecycle at the unit level: the first check (Running) writes
// a single check.k8s:v1 re-poll and emits nothing terminal; the second check
// (Succeeded) emits exactly one validation_node_completed.
func TestHandle_ValidationModeLabel_RunningThenSucceeded_EmitsNodeCompletedOnlyOnTerminal(t *testing.T) {
	labels := map[string]string{"mode": "validation"}
	annotations := map[string]string{
		pkgmodel.AnnotationReleaseID: "rel-7",
		pkgmodel.AnnotationNodeID:    "node-7",
	}
	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "validate-node-7",
		MaxRetries: 3,
	}

	// First check: Running → one re-poll, nothing terminal.
	runningOutbox := &fakeOutboxRepo{}
	runningHandler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}, labels: labels, annotations: annotations},
		noopCancelledRepo(), 3,
	)
	if err := runningHandler.Handle(context.Background(), newFakeUoW(runningOutbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle (running): %v", err)
	}
	if len(runningOutbox.entries) != 1 || runningOutbox.entries[0].EventType != "check_delayed" {
		t.Fatalf("running check: expected 1 check_delayed re-poll, got %v", eventTypesOf(runningOutbox.entries))
	}

	// Re-check: Succeeded → one validation_node_completed.
	doneOutbox := &fakeOutboxRepo{}
	doneHandler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusSucceeded}, labels: labels, annotations: annotations},
		noopCancelledRepo(), 3,
	)
	if err := doneHandler.Handle(context.Background(), newFakeUoW(doneOutbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle (succeeded): %v", err)
	}
	if len(doneOutbox.entries) != 1 || doneOutbox.entries[0].EventType != "validation_node_completed" {
		t.Fatalf("terminal check: expected 1 validation_node_completed, got %v", eventTypesOf(doneOutbox.entries))
	}
}

// TestHandle_ValidationModeLabel_RawIDsRoundTripViaAnnotations verifies the I2 fix:
// a node_id that sanitizeK8sLabel WOULD alter (out-of-charset chars and >63 chars)
// is carried losslessly via Job annotations into the validation.node.completed
// payload, so the executor's raw-keyed outcome lookup matches.
func TestHandle_ValidationModeLabel_RawIDsRoundTripViaAnnotations(t *testing.T) {
	// Out-of-charset chars (: / +) AND >63 chars: sanitizeK8sLabel would both
	// replace and truncate this, so a label round-trip would desync the lookup.
	rawNodeID := "service-1.analytics.my_model:with/colon+" + repeatStr("x", 60)
	rawReleaseID := "release/2026-05-29T12:00:00+00:00"

	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusSucceeded},
			labels: map[string]string{"mode": "validation"},
			annotations: map[string]string{
				pkgmodel.AnnotationReleaseID: rawReleaseID,
				pkgmodel.AnnotationNodeID:    rawNodeID,
			},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "validate-node-raw",
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(outbox.entries) != 1 {
		t.Fatalf("expected 1 validation_node_completed row, got %d", len(outbox.entries))
	}
	var payload map[string]any
	if err := json.Unmarshal(outbox.entries[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["node_id"] != rawNodeID {
		t.Errorf("node_id not round-tripped: want %q, got %v", rawNodeID, payload["node_id"])
	}
	if payload["release_id"] != rawReleaseID {
		t.Errorf("release_id not round-tripped: want %q, got %v", rawReleaseID, payload["release_id"])
	}
}

// repeatStr returns s repeated n times.
func repeatStr(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

// eventTypesOf returns the event_type of every entry, for failure messages.
func eventTypesOf(entries []*pkgoutbox.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.EventType
	}
	return out
}

// TestHandle_ProductionModeLabel_WritesThreeProdOutboxRows_NoChange locks the
// production path: a Job without mode=validation (here mode=production, but a
// missing label behaves identically) still writes the three production rows.
func TestHandle_ProductionModeLabel_WritesThreeProdOutboxRows_NoChange(t *testing.T) {
	now := time.Now()
	for _, labels := range []map[string]string{
		{"mode": "production"},
		nil,
	} {
		outbox := &fakeOutboxRepo{}
		handler := newHandler(
			&fakeK8sClient{
				status: &model.K8sPodResult{
					Status:           model.JobStatusSucceeded,
					StartedAt:        &now,
					CompletedAt:      &now,
					ExecutionSeconds: 5.0,
				},
				labels: labels,
			},
			noopCancelledRepo(), 3,
		)

		cmd := command.CheckJobStatus{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			JobName:    "job-prod",
			MaxRetries: 3,
		}

		if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
			t.Fatalf("Handle (labels=%v): %v", labels, err)
		}

		entries := outbox.entries
		if len(entries) != 3 {
			t.Fatalf("labels=%v: expected 3 production outbox rows, got %d", labels, len(entries))
		}
		if got := eventTypeOf(entries, 0); got != "task_status_updated" {
			t.Errorf("labels=%v: entries[0]: expected task_status_updated, got %q", labels, got)
		}
		if got := eventTypeOf(entries, 1); got != "task_execution_recorded" {
			t.Errorf("labels=%v: entries[1]: expected task_execution_recorded, got %q", labels, got)
		}
		if got := eventTypeOf(entries, 2); got != "node_status_updated" {
			t.Errorf("labels=%v: entries[2]: expected node_status_updated, got %q", labels, got)
		}
		if findEntryByEventType(entries, "validation_node_completed") != nil {
			t.Errorf("labels=%v: unexpected validation_node_completed row on production path", labels)
		}
	}
}

// Compile-time interface checks.
var _ ports.LogUploader = (*fakeLogUploader)(nil)
var _ handlers.K8sStatusChecker = (*fakeK8sClient)(nil)
