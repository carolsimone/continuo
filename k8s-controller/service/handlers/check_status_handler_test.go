package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/validationresult"
	"github.com/google/uuid"
)

// --- fakes ---

type fakeK8sClient struct {
	status      *model.K8sPodResult
	err         error
	labels      map[string]string
	annotations map[string]string
	podLog      string // full pod log returned by GetPodLogs (tail mirrors it)
	podLogsErr  error  // when set, GetPodLogs fails
	// blockPodLogs makes GetPodLogs hang until its context expires, standing in
	// for an unreachable kubelet or a stalled API read.
	blockPodLogs bool
}

func (f *fakeK8sClient) GetJobStatus(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
	return f.status, f.err
}

func (f *fakeK8sClient) GetPodLogs(ctx context.Context, _, _ string, _ int64) (string, string, error) {
	if f.blockPodLogs {
		<-ctx.Done()
		return "", "", ctx.Err()
	}
	if f.podLogsErr != nil {
		return "", "", f.podLogsErr
	}
	return f.podLog, f.podLog, nil
}

func (f *fakeK8sClient) GetJobMeta(_ context.Context, _, _ string) (labels, annotations map[string]string, err error) {
	return f.labels, f.annotations, nil
}

type fakeLogUploader struct{ uploaded map[string]string }

func (f *fakeLogUploader) UploadLog(_ context.Context, key, content string) error {
	if f.uploaded == nil {
		f.uploaded = map[string]string{}
	}
	f.uploaded[key] = content
	return nil
}

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

func (r *fakeMessageProcessingRepo) AlreadyProcessed(_ context.Context, _, _ string, _ *uuid.UUID) (bool, error) {
	return false, nil
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
	h, _ := newHandlerWithUploader(k8s, cancelledSchedules, defaultMaxRetries)
	return h
}

func newHandlerWithUploader(k8s handlers.K8sStatusChecker, cancelledSchedules repository.CancelledSchedulesRepository, defaultMaxRetries int) (*handlers.CheckStatusHandler, *fakeLogUploader) {
	cfg := &handlers.HandlerConfig{
		K8sNamespace:          "default",
		CheckDelaySeconds:     30,
		ErrorMessageMaxLen:    4096,
		LogTailLines:          50,
		DefaultTaskMaxRetries: defaultMaxRetries,
	}
	up := &fakeLogUploader{}
	return handlers.NewCheckStatusHandler(k8s, up, cfg, cancelledSchedules, slog.Default()), up
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

// TestHandleFailedWithRetry_OperationFromDurableCommand_VanishedJob guards against
// the false-green bug where a retried `dbt test` Job comes back as `dbt run`. The
// operation must be read from the durable CheckJobStatus.Operation, NOT from the
// failed Job's labels: a TTL-reaped ("vanished") Job returns EMPTY labels from
// GetJobMeta, so a label-sourced read would silently emit `dbt run`. Here the fake
// client returns empty labels (vanished Job) yet cmd.Operation=="test", and the
// task_retry payload must stay "test".
func TestHandleFailedWithRetry_OperationFromDurableCommand_VanishedJob(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{
		status: failedResult(),
		labels: map[string]string{}, // vanished Job: GetJobMeta returns empty labels
	}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		Operation:  "test", // durable, carried on node.deployed:v1 / check.k8s:v1
		RetryCount: 0,
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	retryEntry := findEntryByEventType(outbox.entries, "task_retry")
	if retryEntry == nil {
		t.Fatal("missing task_retry entry")
	}
	var retryPayload map[string]interface{}
	if err := json.Unmarshal(retryEntry.Payload, &retryPayload); err != nil {
		t.Fatalf("unmarshal task_retry: %v", err)
	}
	if got, _ := retryPayload["operation"].(string); got != "test" {
		t.Errorf("task_retry operation: expected %q, got %q (payload=%v)", "test", got, retryPayload)
	}
}

// TestHandleFailedWithRetry_NoOperationStaysEmpty guards the normal `dbt run`
// path: a command with no Operation (the common case) must produce a task_retry
// payload with an empty/absent operation so the wire format is unchanged for
// production runs.
func TestHandleFailedWithRetry_NoOperationStaysEmpty(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(&fakeK8sClient{
		status: failedResult(),
		labels: map[string]string{},
	}, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "job-abc",
		RetryCount: 0,
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	retryEntry := findEntryByEventType(outbox.entries, "task_retry")
	if retryEntry == nil {
		t.Fatal("missing task_retry entry")
	}
	var retryPayload map[string]interface{}
	if err := json.Unmarshal(retryEntry.Payload, &retryPayload); err != nil {
		t.Fatalf("unmarshal task_retry: %v", err)
	}
	if got, _ := retryPayload["operation"].(string); got != "" {
		t.Errorf("task_retry operation: expected empty, got %q", got)
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

// TestHandleRunningCarriesRetryInfo verifies that a subsequent poll of a running
// job (RunningAnnounced=true) writes only the check_delayed re-poll and carries
// RetryCount and MaxRetries forward in its payload.
func TestHandleRunningCarriesRetryInfo(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "job-running",
		RetryCount:       1,
		MaxRetries:       5,
		RunningAnnounced: true,
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

// TestHandleRunning_RecirculatesOperation verifies the dbt verb rides the
// check.k8s:v1 self-poll loop: a running Job with cmd.Operation=="test" must emit
// a check_delayed ticket whose payload carries operation="test". Without this, a
// re-check that lands after the Job is TTL-reaped would lose the verb and a later
// retry would rebuild `dbt run` — the vanished-Job regression, one hop upstream.
func TestHandleRunning_RecirculatesOperation(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "job-running",
		Operation:        "test",
		RetryCount:       1,
		MaxRetries:       5,
		RunningAnnounced: true,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entry := findEntryByEventType(outbox.entries, "check_delayed")
	if entry == nil {
		t.Fatal("missing check_delayed entry")
	}
	// Decode the recirculated ticket and assert the verb survives the
	// running -> re-check hop (the check.k8s:v1 payload carries operation).
	var payload map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		t.Fatalf("unmarshal check_delayed: %v", err)
	}
	if got, _ := payload["operation"].(string); got != "test" {
		t.Fatalf("check.k8s ticket dropped operation: got %q, want %q (payload=%v)", got, "test", payload)
	}
}

// TestHandleRunning_FirstObservation_AnnouncesRunningOncePerAttempt verifies that
// the first time k8s observes a production Job running (RunningAnnounced=false) it
// emits a task_status_updated RUNNING row stamped with the running attempt
// (cmd.RetryCount), plus the check_delayed re-poll ticket carrying
// running_announced=true so the next poll does not re-announce.
func TestHandleRunning_FirstObservation_AnnouncesRunningOncePerAttempt(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusRunning},
			labels: map[string]string{"mode": "production"},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "job-running",
		RetryCount:       1,
		MaxRetries:       5,
		RunningAnnounced: false,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (task_status_updated RUNNING + check_delayed), got %d: %v", len(entries), eventTypesOf(entries))
	}

	statusEntry := findEntryByEventType(entries, "task_status_updated")
	if statusEntry == nil {
		t.Fatal("missing task_status_updated entry")
	}
	var statusPayload pkgevents.TaskStatusUpdated
	if err := json.Unmarshal(statusEntry.Payload, &statusPayload); err != nil {
		t.Fatalf("unmarshal task_status_updated: %v", err)
	}
	if statusPayload.Status != "RUNNING" {
		t.Errorf("expected status=RUNNING, got %q", statusPayload.Status)
	}
	if statusPayload.RetryCount != cmd.RetryCount {
		t.Errorf("RUNNING retry_count: expected %d (attempt running), got %d", cmd.RetryCount, statusPayload.RetryCount)
	}

	checkEntry := findEntryByEventType(entries, "check_delayed")
	if checkEntry == nil {
		t.Fatal("missing check_delayed entry")
	}
	var checkPayload map[string]interface{}
	if err := json.Unmarshal(checkEntry.Payload, &checkPayload); err != nil {
		t.Fatalf("unmarshal check_delayed: %v", err)
	}
	if ra, _ := checkPayload["running_announced"].(bool); !ra {
		t.Errorf("expected check_delayed running_announced=true, got %v", checkPayload["running_announced"])
	}
}

// TestHandleRunning_AlreadyAnnounced_DoesNotReannounce verifies a subsequent poll
// (RunningAnnounced=true) writes only the check_delayed re-poll — RUNNING is
// emitted exactly once per attempt.
func TestHandleRunning_AlreadyAnnounced_DoesNotReannounce(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{status: &model.K8sPodResult{Status: model.JobStatusRunning}},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "job-running",
		RetryCount:       1,
		MaxRetries:       5,
		RunningAnnounced: true,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (check_delayed only), got %d: %v", len(entries), eventTypesOf(entries))
	}
	if entries[0].EventType != "check_delayed" {
		t.Errorf("expected check_delayed, got %q", entries[0].EventType)
	}
	if findEntryByEventType(entries, "task_status_updated") != nil {
		t.Error("unexpected task_status_updated on already-announced poll")
	}
}

// TestHandleRunning_ValidationJob_SuppressesRunningAnnouncement verifies a
// mode=validation Job observed running for the first time writes only the
// check_delayed re-poll and never a task_status_updated row (validation Jobs use
// synthetic task IDs and carry no real task status).
func TestHandleRunning_ValidationJob_SuppressesRunningAnnouncement(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusRunning},
			labels: map[string]string{"mode": "validation"},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "validate-node-1",
		MaxRetries:       3,
		RunningAnnounced: false,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 || entries[0].EventType != "check_delayed" {
		t.Fatalf("expected 1 check_delayed entry, got %v", eventTypesOf(entries))
	}
	if findEntryByEventType(entries, "task_status_updated") != nil {
		t.Error("validation Job must not emit task_status_updated RUNNING")
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

// TestHandleSucceeded_ParseCache verifies that writeTaskExecutionRecorded
// derives ParseCache/ParseCacheReason from the hydrate-parse-cache initContainer's
// termination message: "hydrated" -> state hydrated/no reason; "degraded:<reason>"
// -> state degraded/reason; and an absent hydrate-parse-cache entry (pre-feature
// Jobs, validation Jobs) omits both fields from the wire payload entirely.
func TestHandleSucceeded_ParseCache(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initMessages   map[string]string
		wantParseCache string
		wantReason     string
		wantOmitted    bool
	}{
		{
			name:           "hydrated",
			initMessages:   map[string]string{"hydrate-parse-cache": "hydrated"},
			wantParseCache: "hydrated",
			wantReason:     "",
		},
		{
			name:           "degraded",
			initMessages:   map[string]string{"hydrate-parse-cache": "degraded:artifact missing"},
			wantParseCache: "degraded",
			wantReason:     "artifact missing",
		},
		{
			name:           "unknown",
			initMessages:   map[string]string{"hydrate-parse-cache": "corrupted-gibberish"},
			wantParseCache: "unknown",
			wantReason:     "",
		},
		{
			name:         "container absent",
			initMessages: nil,
			wantOmitted:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &fakeOutboxRepo{}
			result := &model.K8sPodResult{
				Status:                  model.JobStatusSucceeded,
				InitTerminationMessages: tc.initMessages,
				ExecutionSeconds:        1.0,
			}
			handler := newHandler(&fakeK8sClient{status: result}, noopCancelledRepo(), 3)

			cmd := command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "job-parse-cache-" + tc.name,
				MaxRetries: 3,
			}

			if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			execEntry := findEntryByEventType(outbox.entries, "task_execution_recorded")
			if execEntry == nil {
				t.Fatalf("expected a task_execution_recorded entry, got %v", eventTypesOf(outbox.entries))
			}

			var payload map[string]any
			if err := json.Unmarshal(execEntry.Payload, &payload); err != nil {
				t.Fatalf("unmarshal task_execution_recorded: %v", err)
			}

			if tc.wantOmitted {
				if _, present := payload["parse_cache"]; present {
					t.Errorf("expected parse_cache to be omitted, got %v", payload["parse_cache"])
				}
				if _, present := payload["parse_cache_reason"]; present {
					t.Errorf("expected parse_cache_reason to be omitted, got %v", payload["parse_cache_reason"])
				}
				return
			}

			if payload["parse_cache"] != tc.wantParseCache {
				t.Errorf("expected parse_cache=%q, got %v", tc.wantParseCache, payload["parse_cache"])
			}
			if tc.wantReason == "" {
				if _, present := payload["parse_cache_reason"]; present {
					t.Errorf("expected parse_cache_reason to be omitted, got %v", payload["parse_cache_reason"])
				}
			} else if payload["parse_cache_reason"] != tc.wantReason {
				t.Errorf("expected parse_cache_reason=%q, got %v", tc.wantReason, payload["parse_cache_reason"])
			}
		})
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

// TestValidationTerminal_UploadsRunResultsAndSetsURI verifies that a validation
// Job whose pod log carries the structured-result sentinel block uploads that JSON
// to a run-results/ key, strips it from the text log, and surfaces run_results_uri
// on the validation_node_completed payload.
func TestValidationTerminal_UploadsRunResultsAndSetsURI(t *testing.T) {
	podLog := "build failed\n" +
		"===CONTINUO_VALIDATION_RESULT_BEGIN===\n" +
		`{"schema_version":1,"status":"error","message":"relation x does not exist","failures":0,"unique_id":"model.svc.x"}` + "\n" +
		"===CONTINUO_VALIDATION_RESULT_END===\n"

	k8s := &fakeK8sClient{
		status: &model.K8sPodResult{Status: model.JobStatusFailed},
		labels: map[string]string{"mode": "validation"},
		annotations: map[string]string{
			pkgmodel.AnnotationReleaseID: "rel-1",
			pkgmodel.AnnotationNodeID:    "svc.schema.x",
		},
		podLog: podLog,
	}
	handler, up := newHandlerWithUploader(k8s, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "validate-node-rr",
		MaxRetries: 3,
	}

	outbox := &fakeOutboxRepo{}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 1. A run-results artifact was uploaded; its body is the structured JSON.
	var rrKey, rrBody string
	for k, v := range up.uploaded {
		if strings.HasPrefix(k, "run-results/task-executions/") {
			rrKey, rrBody = k, v
		}
	}
	if rrKey == "" {
		t.Fatalf("expected a run-results/ artifact upload, got keys %v", keysOf(up.uploaded))
	}
	if !strings.Contains(rrBody, `"status":"error"`) {
		t.Fatalf("run-results body not the structured JSON: %q", rrBody)
	}
	if !strings.HasSuffix(rrKey, ".json") {
		t.Fatalf("run-results key should end .json: %q", rrKey)
	}

	// 2. The text log uploaded under logs/ must NOT contain the sentinel block.
	for k, v := range up.uploaded {
		if strings.HasPrefix(k, "logs/task-executions/") && strings.Contains(v, "CONTINUO_VALIDATION_RESULT") {
			t.Fatalf("text log still carries the sentinel block: %q", v)
		}
	}

	// 3. The validation_node_completed payload carries run_results_uri == rrKey.
	if len(outbox.entries) != 1 || outbox.entries[0].EventType != "validation_node_completed" {
		t.Fatalf("expected 1 validation_node_completed, got %v", eventTypesOf(outbox.entries))
	}
	var payload map[string]any
	if err := json.Unmarshal(outbox.entries[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["run_results_uri"] != rrKey {
		t.Errorf("run_results_uri = %v, want %q", payload["run_results_uri"], rrKey)
	}
}

// keysOf returns the keys of an upload map, for failure messages.
func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSplitValidationResult(t *testing.T) {
	log := "line a\nline b\n" +
		"===CONTINUO_VALIDATION_RESULT_BEGIN===\n" +
		`{"schema_version":1,"status":"error","message":"boom","failures":0,"unique_id":"model.svc.x"}` + "\n" +
		"===CONTINUO_VALIDATION_RESULT_END===\n"
	clean, structured := handlers.SplitValidationResult(log)
	if strings.Contains(clean, "CONTINUO_VALIDATION_RESULT") {
		t.Fatalf("clean log still contains sentinel: %q", clean)
	}
	if !strings.Contains(structured, `"status":"error"`) {
		t.Fatalf("structured JSON not extracted: %q", structured)
	}

	clean2, structured2 := handlers.SplitValidationResult("just a normal log\n")
	if structured2 != "" {
		t.Fatalf("expected no structured block, got %q", structured2)
	}
	if clean2 != "just a normal log\n" {
		t.Fatalf("clean log altered when no block present: %q", clean2)
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

// TestHandle_SeedBuildModeLabel_WritesSeedBuildNodeCompletedOutboxRowOnly verifies
// that a terminal Job carrying mode=seed_build emits exactly one
// seed_build_node_completed row (stream=seed.build.node.completed:v1, outcome=ok
// on Succeeded / outcome=failed on Failed, release_id/node_id from annotations)
// and none of the three production task-status rows.
func TestHandle_SeedBuildModeLabel_WritesSeedBuildNodeCompletedOutboxRowOnly(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          model.JobStatus
		expectedOutcome string
	}{
		{"succeeded", model.JobStatusSucceeded, "ok"},
		{"failed", model.JobStatusFailed, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &fakeOutboxRepo{}
			handler := newHandler(
				&fakeK8sClient{
					status: &model.K8sPodResult{Status: tc.status},
					labels: map[string]string{"mode": "seed_build"},
					annotations: map[string]string{
						pkgmodel.AnnotationReleaseID: "rel-sb-1",
						pkgmodel.AnnotationNodeID:    "seed-node-abc",
					},
				},
				noopCancelledRepo(), 3,
			)

			cmd := command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "seed-build-node-abc",
				MaxRetries: 3,
			}

			if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			entries := outbox.entries
			if len(entries) != 1 {
				t.Fatalf("expected exactly 1 outbox entry (seed_build_node_completed), got %d: %v", len(entries), eventTypesOf(entries))
			}
			entry := entries[0]
			if entry.EventType != "seed_build_node_completed" {
				t.Errorf("expected event_type=seed_build_node_completed, got %q", entry.EventType)
			}
			if entry.StreamName != streams.SeedBuildNodeCompletedV1 {
				t.Errorf("expected stream=%s, got %q", streams.SeedBuildNodeCompletedV1, entry.StreamName)
			}
			if entry.AggregateType != "release" {
				t.Errorf("expected aggregate_type=release, got %q", entry.AggregateType)
			}

			// Ensure no production rows leaked in.
			for _, e := range entries {
				switch e.EventType {
				case "task_status_updated", "task_execution_recorded", "node_status_updated", "task_retry", "task_failed":
					t.Errorf("unexpected production outbox row %q for seed_build Job", e.EventType)
				}
			}

			var payload map[string]any
			if err := json.Unmarshal(entry.Payload, &payload); err != nil {
				t.Fatalf("unmarshal seed_build_node_completed: %v", err)
			}
			if payload["release_id"] != "rel-sb-1" {
				t.Errorf("expected release_id=rel-sb-1, got %v", payload["release_id"])
			}
			if payload["node_id"] != "seed-node-abc" {
				t.Errorf("expected node_id=seed-node-abc, got %v", payload["node_id"])
			}
			if payload["outcome"] != tc.expectedOutcome {
				t.Errorf("expected outcome=%s, got %v", tc.expectedOutcome, payload["outcome"])
			}
		})
	}
}

// TestHandle_SeedBuildModeLabel_SuppressesRunningAnnouncement verifies a
// mode=seed_build Job observed running for the first time writes only the
// check_delayed re-poll and never a task_status_updated row (seed_build Jobs
// use synthetic task IDs and carry no real task status, same as validation).
func TestHandle_SeedBuildModeLabel_SuppressesRunningAnnouncement(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusRunning},
			labels: map[string]string{"mode": "seed_build"},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "seed-build-node-1",
		MaxRetries:       3,
		RunningAnnounced: false,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 || entries[0].EventType != "check_delayed" {
		t.Fatalf("expected 1 check_delayed entry, got %v", eventTypesOf(entries))
	}
	if findEntryByEventType(entries, "task_status_updated") != nil {
		t.Error("seed_build Job must not emit task_status_updated RUNNING")
	}
}

// TestHandle_CompileModeLabel_WritesCompileNodeCompletedOutboxRowOnly verifies
// that a terminal Job carrying mode=compile emits exactly one
// compile_node_completed row (stream=compile.node.completed:v1, outcome=ok on
// Succeeded / outcome=failed on Failed, release_id/node_id from annotations)
// and none of the three production task-status rows.
func TestHandle_CompileModeLabel_WritesCompileNodeCompletedOutboxRowOnly(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          model.JobStatus
		expectedOutcome string
	}{
		{"succeeded", model.JobStatusSucceeded, "ok"},
		{"failed", model.JobStatusFailed, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &fakeOutboxRepo{}
			handler := newHandler(
				&fakeK8sClient{
					status: &model.K8sPodResult{Status: tc.status},
					labels: map[string]string{"mode": "compile"},
					annotations: map[string]string{
						pkgmodel.AnnotationReleaseID: "rel-compile-1",
						pkgmodel.AnnotationNodeID:    "compile-node-abc",
					},
				},
				noopCancelledRepo(), 3,
			)

			cmd := command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "compile-node-abc",
				MaxRetries: 3,
			}

			if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			entries := outbox.entries
			if len(entries) != 1 {
				t.Fatalf("expected exactly 1 outbox entry (compile_node_completed), got %d: %v", len(entries), eventTypesOf(entries))
			}
			entry := entries[0]
			if entry.EventType != "compile_node_completed" {
				t.Errorf("expected event_type=compile_node_completed, got %q", entry.EventType)
			}
			if entry.StreamName != streams.CompileNodeCompletedV1 {
				t.Errorf("expected stream=%s, got %q", streams.CompileNodeCompletedV1, entry.StreamName)
			}
			if entry.AggregateType != "release" {
				t.Errorf("expected aggregate_type=release, got %q", entry.AggregateType)
			}

			// Ensure no production rows leaked in.
			for _, e := range entries {
				switch e.EventType {
				case "task_status_updated", "task_execution_recorded", "node_status_updated", "task_retry", "task_failed":
					t.Errorf("unexpected production outbox row %q for compile Job", e.EventType)
				}
			}

			var payload map[string]any
			if err := json.Unmarshal(entry.Payload, &payload); err != nil {
				t.Fatalf("unmarshal compile_node_completed: %v", err)
			}
			if payload["release_id"] != "rel-compile-1" {
				t.Errorf("expected release_id=rel-compile-1, got %v", payload["release_id"])
			}
			if payload["node_id"] != "compile-node-abc" {
				t.Errorf("expected node_id=compile-node-abc, got %v", payload["node_id"])
			}
			if payload["outcome"] != tc.expectedOutcome {
				t.Errorf("expected outcome=%s, got %v", tc.expectedOutcome, payload["outcome"])
			}
		})
	}
}

// TestHandle_CompileModeLabel_PayloadIncludesFailedContainerWhenSet verifies that
// handleCompileTerminal adds a failed_container key to the compile_node_completed
// payload when K8sPodResult.FailedContainer is set — downstream consumers use it to
// map compile-leg containers (compile/parse-prod/parse-candidate/upload) to distinct
// release-reject reasons.
func TestHandle_CompileModeLabel_PayloadIncludesFailedContainerWhenSet(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusFailed, FailedContainer: "parse-prod"},
			labels: map[string]string{"mode": "compile"},
			annotations: map[string]string{
				pkgmodel.AnnotationReleaseID: "rel-compile-2",
				pkgmodel.AnnotationNodeID:    "compile-node-def",
			},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "compile-node-def",
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 outbox entry, got %d: %v", len(entries), eventTypesOf(entries))
	}

	var payload map[string]any
	if err := json.Unmarshal(entries[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal compile_node_completed: %v", err)
	}
	if payload["failed_container"] != "parse-prod" {
		t.Errorf("expected failed_container=parse-prod, got %v", payload["failed_container"])
	}
}

// TestHandle_CompileModeLabel_PayloadOmitsFailedContainerWhenEmpty verifies that
// handleCompileTerminal omits the failed_container key entirely when
// K8sPodResult.FailedContainer is empty (e.g. a succeeded compile Job).
func TestHandle_CompileModeLabel_PayloadOmitsFailedContainerWhenEmpty(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusSucceeded},
			labels: map[string]string{"mode": "compile"},
			annotations: map[string]string{
				pkgmodel.AnnotationReleaseID: "rel-compile-3",
				pkgmodel.AnnotationNodeID:    "compile-node-ghi",
			},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		JobName:    "compile-node-ghi",
		MaxRetries: 3,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 outbox entry, got %d: %v", len(entries), eventTypesOf(entries))
	}

	var payload map[string]any
	if err := json.Unmarshal(entries[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal compile_node_completed: %v", err)
	}
	if _, present := payload["failed_container"]; present {
		t.Errorf("expected failed_container key to be omitted, got %v", payload["failed_container"])
	}
}

// TestHandle_CompileModeLabel_SuppressesRunningAnnouncement verifies a
// mode=compile Job observed running for the first time writes only the
// check_delayed re-poll and never a task_status_updated row (compile Jobs
// use synthetic task IDs and carry no real task status, same as validation/seed_build).
func TestHandle_CompileModeLabel_SuppressesRunningAnnouncement(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusRunning},
			labels: map[string]string{"mode": "compile"},
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		JobName:          "compile-node-1",
		MaxRetries:       3,
		RunningAnnounced: false,
	}

	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries := outbox.entries
	if len(entries) != 1 || entries[0].EventType != "check_delayed" {
		t.Fatalf("expected 1 check_delayed entry, got %v", eventTypesOf(entries))
	}
	if findEntryByEventType(entries, "task_status_updated") != nil {
		t.Error("compile Job must not emit task_status_updated RUNNING")
	}
}

// TestHandle_CompileModeLabel_RunningStatus_WritesCheckK8sRepoll verifies a
// still-running (or briefly Unknown) compile Job is re-polled: the handler
// writes exactly one check.k8s:v1 re-poll ticket and no compile_node_completed
// or production rows.
func TestHandle_CompileModeLabel_RunningStatus_WritesCheckK8sRepoll(t *testing.T) {
	for _, status := range []model.JobStatus{model.JobStatusRunning, model.JobStatusUnknown} {
		outbox := &fakeOutboxRepo{}
		handler := newHandler(
			&fakeK8sClient{
				status: &model.K8sPodResult{Status: status},
				labels: map[string]string{"mode": "compile"},
				annotations: map[string]string{
					pkgmodel.AnnotationReleaseID: "rel-c-1",
					pkgmodel.AnnotationNodeID:    "compile-1",
				},
			},
			noopCancelledRepo(), 3,
		)

		cmd := command.CheckJobStatus{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			JobName:    "compile-node-1",
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
			case "compile_node_completed", "task_status_updated", "task_execution_recorded", "node_status_updated", "task_retry", "task_failed":
				t.Errorf("status %s: unexpected non-repoll row %q for running compile Job", status, e.EventType)
			}
		}
	}
}

// TestHandle_PromoteSeedMode_TerminalJob_NoOutboxRows verifies that a terminal Job
// carrying mode=promote_seed produces NO outbox rows (neither production
// task.status.updated / task.execution.recorded, nor any candidate-mode event).
// pythonResultBlockLog returns a pod log shaped like a python-model container's
// output: diagnostics first, terminated by exactly one sentinel-framed result
// block as the last line.
func pythonResultBlockLog(status, message string) string {
	return "running node analytics.orders -> analytics.orders\n" +
		validationresult.SentinelBegin + "\n" +
		`{"schema_version":1,"status":"` + status + `","message":"` + message +
		`","failures":1,"unique_id":"analytics.orders"}` + "\n" +
		validationresult.SentinelEnd + "\n"
}

// decodeExecutionPayload returns the task_execution_recorded payload written to
// the outbox, failing the test when no such row exists.
func decodeExecutionPayload(t *testing.T, entries []*pkgoutbox.Entry) pkgevents.TaskExecutionRecorded {
	t.Helper()
	entry := findEntryByEventType(entries, "task_execution_recorded")
	if entry == nil {
		t.Fatal("no task_execution_recorded entry written")
	}
	var payload pkgevents.TaskExecutionRecorded
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		t.Fatalf("unmarshal task_execution_recorded payload: %v", err)
	}
	return payload
}

// TestHandleFailedPermanent_RunResultsURI verifies a permanently-failed
// production Job whose pod printed a result block records the uploaded JSON's
// S3 key, instead of writing the object and dropping its address. It also pins
// the division of labour between the two uploads: the text log has the block
// stripped, and the block's own JSON carries the error class.
func TestHandleFailedPermanent_RunResultsURI(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{
		status: failedResult(),
		podLog: pythonResultBlockLog("error", "ConformError: column 'id' cannot be safely cast to INTEGER"),
	}
	handler, uploader := newHandlerWithUploader(k8s, noopCancelledRepo(), 0)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-py-failed",
		ServiceName: "svc-py", SchemaName: "analytics", TableName: "orders",
		RetryCount: 0, MaxRetries: 0,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	payload := decodeExecutionPayload(t, outbox.entries)
	if payload.RunResultsURI == "" {
		t.Fatal("run_results_uri must be recorded when the pod printed a result block")
	}
	if !strings.HasPrefix(payload.RunResultsURI, "run-results/task-executions/svc-py/analytics/orders/") {
		t.Errorf("unexpected run-results key %q", payload.RunResultsURI)
	}
	if got := uploader.uploaded[payload.RunResultsURI]; !strings.Contains(got, "ConformError") {
		t.Errorf("uploaded run-results JSON must carry the error class, got %q", got)
	}
	if logged := uploader.uploaded[payload.LogS3Key]; strings.Contains(logged, validationresult.SentinelBegin) {
		t.Error("the uploaded text log must have the sentinel block stripped")
	}
}

// TestHandleFailedWithRetry_RunResultsURI verifies the retry path records the
// key too — a node that will be retried is exactly the one whose first failure
// an operator wants to read.
func TestHandleFailedWithRetry_RunResultsURI(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{
		status: failedResult(),
		podLog: pythonResultBlockLog("error", "ReadError: unknown read 'orders'"),
	}
	handler := newHandler(k8s, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-py-retry",
		ServiceName: "svc-py", SchemaName: "analytics", TableName: "orders",
		RetryCount: 0, MaxRetries: 3,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if payload := decodeExecutionPayload(t, outbox.entries); payload.RunResultsURI == "" {
		t.Fatal("run_results_uri must be recorded on the retry path too")
	}
}

// TestHandleFailedPermanent_NoResultBlockOmitsRunResultsURI pins the dbt case:
// a pod log with no sentinel block produces a payload with no run_results_uri
// at all, so dbt wire payloads are unchanged.
func TestHandleFailedPermanent_NoResultBlockOmitsRunResultsURI(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{status: failedResult(), podLog: "Database Error in model orders\n"}
	handler := newHandler(k8s, noopCancelledRepo(), 0)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-dbt-failed",
		ServiceName: "service-1", SchemaName: "analytics", TableName: "orders",
		RetryCount: 0, MaxRetries: 0,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entry := findEntryByEventType(outbox.entries, "task_execution_recorded")
	if entry == nil {
		t.Fatal("no task_execution_recorded entry written")
	}
	if strings.Contains(string(entry.Payload), "run_results_uri") {
		t.Errorf("dbt payload must not carry run_results_uri, got %s", entry.Payload)
	}
}

// succeededResult returns a terminal Succeeded pod result.
func succeededResult() *model.K8sPodResult {
	now := time.Now()
	return &model.K8sPodResult{
		Status:           model.JobStatusSucceeded,
		StartedAt:        &now,
		CompletedAt:      &now,
		ExecutionSeconds: 2.0,
	}
}

// TestHandleSucceeded_UploadsLog verifies a successful production Job's pod log
// reaches S3 and its key reaches the wire. Without it the log is unreachable
// once the Job's TTL reaps the pod, leaving a finished run with timings and no
// evidence of what it did.
func TestHandleSucceeded_UploadsLog(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{
		status: succeededResult(),
		podLog: "1 of 1 OK created sql table model analytics.orders\n",
	}
	handler, uploader := newHandlerWithUploader(k8s, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-ok",
		ServiceName: "service-1", SchemaName: "analytics", TableName: "orders",
		MaxRetries: 3,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	payload := decodeExecutionPayload(t, outbox.entries)
	if payload.LogS3Key == "" {
		t.Fatal("a successful job must record its log key")
	}
	if !strings.HasPrefix(payload.LogS3Key, "logs/task-executions/service-1/analytics/orders/") {
		t.Errorf("unexpected log key %q", payload.LogS3Key)
	}
	if !strings.Contains(uploader.uploaded[payload.LogS3Key], "1 of 1 OK") {
		t.Error("the pod log content must be uploaded")
	}
	if payload.ErrorMessage != "" {
		t.Errorf("a successful job must not carry an error message, got %q", payload.ErrorMessage)
	}
}

// TestHandleSucceeded_UploadsRunResults verifies a successful python-model Job's
// result block is captured too, and stripped from the text log.
func TestHandleSucceeded_UploadsRunResults(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{
		status: succeededResult(),
		podLog: pythonResultBlockLog("success", "rows=42"),
	}
	handler, uploader := newHandlerWithUploader(k8s, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-py-ok",
		ServiceName: "svc-py", SchemaName: "analytics", TableName: "orders",
		MaxRetries: 3,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	payload := decodeExecutionPayload(t, outbox.entries)
	if payload.RunResultsURI == "" {
		t.Fatal("a successful job that printed a result block must record its key")
	}
	if !strings.Contains(uploader.uploaded[payload.RunResultsURI], "rows=42") {
		t.Error("the uploaded run-results JSON must carry the block's message")
	}
	if strings.Contains(uploader.uploaded[payload.LogS3Key], validationresult.SentinelBegin) {
		t.Error("the uploaded text log must have the sentinel block stripped")
	}
}

// TestHandleSucceeded_LogFetchFailureStillSucceeds pins the soft-failure
// guarantee: adding an S3 dependency to the success path must never be able to
// turn a successful run into a failed one.
func TestHandleSucceeded_LogFetchFailureStillSucceeds(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{status: succeededResult(), podLogsErr: errors.New("pod gone")}
	handler := newHandler(k8s, noopCancelledRepo(), 3)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-ok-nolog",
		ServiceName: "service-1", SchemaName: "analytics", TableName: "orders",
		MaxRetries: 3,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("a failed log fetch must not fail the handler: %v", err)
	}

	statusEntry := findEntryByEventType(outbox.entries, "task_status_updated")
	if statusEntry == nil || !strings.Contains(string(statusEntry.Payload), "SUCCEEDED") {
		t.Fatal("the task must still be recorded as SUCCEEDED")
	}
	entry := findEntryByEventType(outbox.entries, "task_execution_recorded")
	if entry == nil {
		t.Fatal("the execution row must still be written")
	}
	if strings.Contains(string(entry.Payload), "log_s3_key") {
		t.Errorf("no log key should be recorded when the fetch failed, got %s", entry.Payload)
	}
}

// TestHandleSucceeded_LogIOTimeoutStillPersistsOutcome is the regression guard
// for the ordering hazard the log upload introduces on the success path: the
// I/O runs before the terminal outbox writes and shares their context, so an
// unbounded slow pod-log read or S3 stall would consume the whole handler
// budget and leave nothing to persist the task's outcome with. The task would
// then be retried and eventually poison-ACKed while still recorded as RUNNING.
//
// The fake blocks until its own context expires, which the handler's own
// LogIOTimeout must cut short well inside the parent budget.
func TestHandleSucceeded_LogIOTimeoutStillPersistsOutcome(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	k8s := &fakeK8sClient{status: succeededResult(), blockPodLogs: true}

	cfg := &handlers.HandlerConfig{
		K8sNamespace:          "default",
		CheckDelaySeconds:     30,
		ErrorMessageMaxLen:    4096,
		LogTailLines:          50,
		DefaultTaskMaxRetries: 3,
		LogIOTimeout:          50 * time.Millisecond,
	}
	handler := handlers.NewCheckStatusHandler(k8s, &fakeLogUploader{}, cfg, noopCancelledRepo(), slog.Default())

	// A parent budget far larger than the I/O cap: the terminal writes must
	// still have almost all of it left once the log fetch is abandoned.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(), JobName: "job-slow-logs",
		ServiceName: "service-1", SchemaName: "analytics", TableName: "orders",
		MaxRetries: 3,
	}
	if err := handler.Handle(ctx, newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("a stalled log fetch must not fail the handler: %v", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("the parent context must still be live for the terminal writes, got %v", err)
	}

	statusEntry := findEntryByEventType(outbox.entries, "task_status_updated")
	if statusEntry == nil || !strings.Contains(string(statusEntry.Payload), "SUCCEEDED") {
		t.Fatal("the task must still be recorded as SUCCEEDED")
	}
	if findEntryByEventType(outbox.entries, "task_execution_recorded") == nil {
		t.Fatal("the execution row must still be written")
	}
	if findEntryByEventType(outbox.entries, "node_status_updated") == nil {
		t.Fatal("the node status row must still be written")
	}
}

// TestUploadedArtifactKeysHaveNoEmptySegments pins the object keys that logs and
// run-results are stored under.
//
// A compile Job runs no node, so its command carries no schema and no table.
// Addressing it like a node Job produced "logs/task-executions/<service>///<id>.log":
// MinIO rejects empty path segments outright (XMinioInvalidObjectName) while AWS
// S3 accepts them, so the compile log vanished on exactly the installs that use
// the bundled MinIO — and the log lost that way is the one belonging to a failed
// compile, which is what a rejected release most needs to explain itself.
func TestUploadedArtifactKeysHaveNoEmptySegments(t *testing.T) {
	for _, tc := range []struct {
		name    string
		labels  map[string]string
		cmd     command.CheckJobStatus
		wantDir string
	}{
		{
			name:   "compile Job is addressed by its service and leg",
			labels: map[string]string{"mode": "compile"},
			// No SchemaName or TableName: a compile Job builds a manifest, not a node.
			cmd:     command.CheckJobStatus{ServiceName: "core", JobName: "compile-core"},
			wantDir: "logs/task-executions/core/compile/",
		},
		{
			name:   "node Job stays addressed by the node it ran",
			labels: map[string]string{},
			cmd: command.CheckJobStatus{
				ServiceName: "core", SchemaName: "analytics", TableName: "daily_transactions",
				JobName: "core-analytics-daily-transactions",
			},
			wantDir: "logs/task-executions/core/analytics/daily_transactions/",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, uploader := newHandlerWithUploader(
				&fakeK8sClient{
					status: &model.K8sPodResult{Status: model.JobStatusSucceeded},
					labels: tc.labels,
					annotations: map[string]string{
						pkgmodel.AnnotationReleaseID: "rel-1",
						pkgmodel.AnnotationNodeID:    "node-1",
					},
					podLog: "some dbt output",
				},
				noopCancelledRepo(), 3,
			)

			cmd := tc.cmd
			cmd.TaskID = uuid.New()
			cmd.ScheduleID = uuid.New()
			cmd.MaxRetries = 3

			if err := handler.Handle(context.Background(), newFakeUoW(&fakeOutboxRepo{}), cmd, uuid.Nil); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			if len(uploader.uploaded) == 0 {
				t.Fatal("expected the pod log to be uploaded")
			}
			for key := range uploader.uploaded {
				// The bug this guards: any empty segment makes the key invalid on MinIO.
				if strings.Contains(key, "//") {
					t.Errorf("key %q contains an empty path segment", key)
				}
				if !strings.HasPrefix(key, tc.wantDir) {
					t.Errorf("key %q is not filed under %q", key, tc.wantDir)
				}
			}
		})
	}
}

// TestHandle_LegacyPromoteSeedMode_EmitsNoLifecycleRows covers work queued by a
// previous version during a rolling upgrade: a Job still labelled
// mode=promote_seed carries a synthetic task ID with no run in state, so
// announcing its lifecycle would address a run state cannot load and wedge that
// consumer on endless redelivery. Current promoted-seed work is an ordinary run
// and carries no mode label, so it falls through to the production path.
func TestHandle_LegacyPromoteSeedMode_EmitsNoLifecycleRows(t *testing.T) {
	for _, status := range []model.JobStatus{model.JobStatusSucceeded, model.JobStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			outbox := &fakeOutboxRepo{}
			handler := newHandler(
				&fakeK8sClient{
					status: &model.K8sPodResult{Status: status},
					labels: map[string]string{"mode": pkgevents.ModePromoteSeed},
				},
				noopCancelledRepo(), 3,
			)

			cmd := command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "legacy-promote-seed-core-analytics-fx",
				MaxRetries: 3,
			}

			if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if len(outbox.entries) != 0 {
				t.Errorf("expected no outbox rows for a legacy promote-seed Job, got %v", eventTypesOf(outbox.entries))
			}
		})
	}
}

// A Job with no mode label is current promoted-seed (or ordinary) work and MUST
// go through the production lifecycle — that is the whole point of the change.
func TestHandle_NoModeLabel_UsesTheProductionLifecycle(t *testing.T) {
	outbox := &fakeOutboxRepo{}
	handler := newHandler(
		&fakeK8sClient{
			status: &model.K8sPodResult{Status: model.JobStatusSucceeded},
			labels: map[string]string{},
			podLog: "dbt seed output",
		},
		noopCancelledRepo(), 3,
	)

	cmd := command.CheckJobStatus{
		TaskID: uuid.New(), ScheduleID: uuid.New(),
		ServiceName: "core", SchemaName: "analytics", TableName: "seed_users",
		JobName: "core-analytics-seed-users-abc", MaxRetries: 3,
	}
	if err := handler.Handle(context.Background(), newFakeUoW(outbox), cmd, uuid.Nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if findEntryByEventType(outbox.entries, "task_status_updated") == nil {
		t.Error("a mode-less Job must announce its task status")
	}
	if findEntryByEventType(outbox.entries, "task_execution_recorded") == nil {
		t.Error("a mode-less Job must record its execution")
	}
}
