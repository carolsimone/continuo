package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// ListTaskAttempts paging bounds: a request without a page size gets
// defaultTaskAttemptsPageSize attempts, and no page holds more than
// maxTaskAttemptsPageSize.
const (
	defaultTaskAttemptsPageSize = 50
	maxTaskAttemptsPageSize     = 200
)

// TaskAttemptHandler serves ListTaskAttempts.
type TaskAttemptHandler struct {
	attempts repository.TaskAttemptReader
	logger   *slog.Logger
}

// NewTaskAttemptHandler builds a TaskAttemptHandler over attempts.
func NewTaskAttemptHandler(attempts repository.TaskAttemptReader, logger *slog.Logger) *TaskAttemptHandler {
	return &TaskAttemptHandler{attempts: attempts, logger: logger}
}

// ListTaskAttempts implements the StateService ListTaskAttempts RPC: one page
// of a run's attempts, or of one of its tasks when task_id is set.
func (h *TaskAttemptHandler) ListTaskAttempts(ctx context.Context, req *statev1.ListTaskAttemptsRequest) (*statev1.ListTaskAttemptsResponse, error) {
	runID, err := uuid.Parse(req.GetRunId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "run_id must be a UUID: %q", req.GetRunId())
	}
	var taskID *uuid.UUID
	if req.GetTaskId() != "" {
		id, err := uuid.Parse(req.GetTaskId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "task_id must be a UUID: %q", req.GetTaskId())
		}
		taskID = &id
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultTaskAttemptsPageSize
	}
	if pageSize > maxTaskAttemptsPageSize {
		pageSize = maxTaskAttemptsPageSize
	}
	offset := max(int(req.GetPageOffset()), 0)

	rows, total, err := h.attempts.ListRunAttempts(ctx, runID, taskID, pageSize, offset)
	if err != nil {
		h.logger.Error("ListTaskAttempts failed", "run_id", runID, "task_id", req.GetTaskId(), "error", err)
		return nil, status.Errorf(codes.Internal, "ListTaskAttempts: %v", err)
	}

	resp := &statev1.ListTaskAttemptsResponse{
		Attempts:   make([]*statev1.TaskAttempt, 0, len(rows)),
		TotalCount: num.ClampInt32(total),
	}
	for _, a := range rows {
		resp.Attempts = append(resp.Attempts, toProtoTaskAttempt(a))
	}
	return resp, nil
}

func toProtoTaskAttempt(a projection.TaskAttempt) *statev1.TaskAttempt {
	out := &statev1.TaskAttempt{
		TaskId:        a.TaskID.String(),
		Attempt:       num.ClampInt32(a.Attempt),
		JobName:       a.JobName,
		Status:        a.Status,
		FailureClass:  stringPtrOrEmpty(a.FailureClass),
		FailureReason: stringPtrOrEmpty(a.FailureReason),
		DispatchedAt:  timestamppb.New(a.DispatchedAt),
		QueuedAt:      timestampOrNil(a.QueuedAt),
		StartedAt:     timestampOrNil(a.StartedAt),
		FinishedAt:    timestampOrNil(a.FinishedAt),
		ErrorMessage:  stringPtrOrEmpty(a.ErrorMessage),
		LogS3Key:      stringPtrOrEmpty(a.LogS3Key),
		RunResultsUri: stringPtrOrEmpty(a.RunResultsURI),
	}
	if a.ExitCode != nil {
		out.ExitCode = num.ClampInt32(*a.ExitCode)
	}
	if a.ExecutionSeconds != nil {
		out.ExecutionSeconds = *a.ExecutionSeconds
	}
	return out
}

// timestampOrNil converts t to a timestamp; a nil t stays unset.
func timestampOrNil(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
