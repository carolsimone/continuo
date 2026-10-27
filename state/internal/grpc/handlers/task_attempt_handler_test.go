package handlers

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/state/domain/projection"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// fakeAttemptReader records the arguments of the last ListRunAttempts call
// and answers with its rows and total.
type fakeAttemptReader struct {
	calls      int
	gotRunID   uuid.UUID
	gotTaskID  *uuid.UUID
	gotSize    int
	gotOffset  int
	rows       []projection.TaskAttempt
	totalCount int
}

func (f *fakeAttemptReader) ListRunAttempts(_ context.Context, _ string, runID uuid.UUID, taskID *uuid.UUID, pageSize, pageOffset int) ([]projection.TaskAttempt, int, error) {
	f.calls++
	f.gotRunID, f.gotTaskID, f.gotSize, f.gotOffset = runID, taskID, pageSize, pageOffset
	return f.rows, f.totalCount, nil
}

func newAttemptHandler(r *fakeAttemptReader) *TaskAttemptHandler {
	return NewTaskAttemptHandler(r, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestListTaskAttempts_RejectsAMissingOrMalformedRunID(t *testing.T) {
	for _, runID := range []string{"", "not-a-uuid"} {
		reader := &fakeAttemptReader{}
		_, err := newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{RunId: runID})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "run_id %q: %v", runID, err)
		assert.Zero(t, reader.calls, "run_id %q reaches no read", runID)
	}
}

func TestListTaskAttempts_RejectsAMalformedTaskID(t *testing.T) {
	reader := &fakeAttemptReader{}
	_, err := newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{
		RunId: uuid.NewString(), TaskId: "task-1",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	assert.Zero(t, reader.calls)
}

func TestListTaskAttempts_ClampsPageSizeAndOffset(t *testing.T) {
	cases := []struct {
		name                 string
		size, offset         int32
		wantSize, wantOffset int
	}{
		{"unset size is 50", 0, 0, 50, 0},
		{"oversized size is 200", 500, 0, 200, 0},
		{"negative offset is 0", 20, -3, 20, 0},
		{"in-range values pass through", 25, 10, 25, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeAttemptReader{}
			_, err := newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{
				RunId: uuid.NewString(), PageSize: tc.size, PageOffset: tc.offset,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantSize, reader.gotSize)
			assert.Equal(t, tc.wantOffset, reader.gotOffset)
		})
	}
}

func TestListTaskAttempts_PassesTheTaskFilter(t *testing.T) {
	runID, taskID := uuid.New(), uuid.New()

	reader := &fakeAttemptReader{}
	_, err := newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{
		RunId: runID.String(), TaskId: taskID.String(),
	})
	require.NoError(t, err)
	assert.Equal(t, runID, reader.gotRunID)
	require.NotNil(t, reader.gotTaskID)
	assert.Equal(t, taskID, *reader.gotTaskID)

	reader = &fakeAttemptReader{}
	_, err = newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{RunId: runID.String()})
	require.NoError(t, err)
	assert.Nil(t, reader.gotTaskID, "an empty task_id lists every task of the run")
}

func TestListTaskAttempts_MapsEveryField(t *testing.T) {
	at := func(s int) time.Time { return time.Date(2026, 10, 21, 9, 0, s, 0, time.UTC) }
	ptr := func(t time.Time) *time.Time { return &t }
	str := func(s string) *string { return &s }
	exit, secs := 137, 12.5
	full := projection.TaskAttempt{
		TaskID: uuid.New(), Attempt: 1, JobName: "job-orders-r1", Status: "failed",
		FailureClass: str("job"), FailureReason: str("oom_killed"), ExitCode: &exit,
		DispatchedAt: at(1), QueuedAt: ptr(at(2)), StartedAt: ptr(at(3)), FinishedAt: ptr(at(4)),
		ExecutionSeconds: &secs, ErrorMessage: str("killed"), LogS3Key: str("logs/a.log"), RunResultsURI: str("rr/a.json"),
	}
	bare := projection.TaskAttempt{TaskID: uuid.New(), Attempt: 0, JobName: "job-orders", Status: "dispatched", DispatchedAt: at(5)}
	reader := &fakeAttemptReader{rows: []projection.TaskAttempt{full, bare}, totalCount: 7}

	resp, err := newAttemptHandler(reader).ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{RunId: uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, int32(7), resp.GetTotalCount())
	require.Len(t, resp.GetAttempts(), 2)

	got := resp.GetAttempts()[0]
	assert.Equal(t, full.TaskID.String(), got.GetTaskId())
	assert.Equal(t, int32(1), got.GetAttempt())
	assert.Equal(t, "job-orders-r1", got.GetJobName())
	assert.Equal(t, "failed", got.GetStatus())
	assert.Equal(t, "job", got.GetFailureClass())
	assert.Equal(t, "oom_killed", got.GetFailureReason())
	assert.Equal(t, int32(137), got.GetExitCode())
	assert.Equal(t, at(1), got.GetDispatchedAt().AsTime())
	assert.Equal(t, at(2), got.GetQueuedAt().AsTime())
	assert.Equal(t, at(3), got.GetStartedAt().AsTime())
	assert.Equal(t, at(4), got.GetFinishedAt().AsTime())
	assert.InDelta(t, 12.5, got.GetExecutionSeconds(), 1e-9)
	assert.Equal(t, "killed", got.GetErrorMessage())
	assert.Equal(t, "logs/a.log", got.GetLogS3Key())
	assert.Equal(t, "rr/a.json", got.GetRunResultsUri())

	empty := resp.GetAttempts()[1]
	assert.Equal(t, bare.TaskID.String(), empty.GetTaskId())
	assert.Equal(t, at(5), empty.GetDispatchedAt().AsTime())
	assert.Nil(t, empty.GetQueuedAt())
	assert.Nil(t, empty.GetStartedAt())
	assert.Nil(t, empty.GetFinishedAt())
	assert.Empty(t, empty.GetFailureClass())
	assert.Empty(t, empty.GetFailureReason())
	assert.Zero(t, empty.GetExitCode())
	assert.Zero(t, empty.GetExecutionSeconds())
	assert.Empty(t, empty.GetErrorMessage())
	assert.Empty(t, empty.GetLogS3Key())
	assert.Empty(t, empty.GetRunResultsUri())
}

// A stopped attempt is reported with the status word "stopped".
func TestListTaskAttempts_ReportsAStoppedAttempt(t *testing.T) {
	stopped := projection.TaskAttempt{TaskID: uuid.New(), Attempt: 0, JobName: "job-orders", Status: "stopped",
		DispatchedAt: time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)}
	resp, err := newAttemptHandler(&fakeAttemptReader{rows: []projection.TaskAttempt{stopped}, totalCount: 1}).
		ListTaskAttempts(context.Background(), &statev1.ListTaskAttemptsRequest{RunId: uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, "stopped", resp.GetAttempts()[0].GetStatus())
}
