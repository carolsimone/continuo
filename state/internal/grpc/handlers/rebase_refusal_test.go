package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRebaseStarter records the input it was given and answers with err.
type fakeRebaseStarter struct {
	in  svchandlers.TriggerRebaseInput
	err error
}

func (f *fakeRebaseStarter) Handle(_ context.Context, _ uow.UnitOfWork, in svchandlers.TriggerRebaseInput) (uuid.UUID, string, error) {
	f.in = in
	if f.err != nil {
		return uuid.Nil, "", f.err
	}
	return uuid.MustParse("6f1d7f0e-3c55-4c4a-9d55-0f2d8f5b7c11"), "daily", nil
}

// fakeRebasePreviewReader records what it was asked and answers with preview or
// err.
type fakeRebasePreviewReader struct {
	sourceID uuid.UUID
	include  bool
	preview  runreads.RebasePreview
	err      error
}

func (f *fakeRebasePreviewReader) Preview(_ context.Context, sourceID uuid.UUID, include bool) (runreads.RebasePreview, error) {
	f.sourceID, f.include = sourceID, include
	return f.preview, f.err
}

// The refusals both rebase RPCs share map to one status each.
func TestRebaseRefusal_MapsEachRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
		msg  string
	}{
		{"source not found", fmt.Errorf("get source: %w", postgres.ErrNotFound), codes.NotFound, "source run not found"},
		{"source not terminal", run.ErrSourceMustBeTerminallyFailedOrCancelled, codes.FailedPrecondition, run.ErrSourceMustBeTerminallyFailedOrCancelled.Error()},
		{"nothing to rebase", run.ErrNothingToRebase, codes.FailedPrecondition, run.ErrNothingToRebase.Error()},
		{"active run", run.ErrScheduleHasActiveRun, codes.FailedPrecondition, run.ErrScheduleHasActiveRun.Error()},
		{"test source", runplan.ErrRerunOfTestUnsupported, codes.FailedPrecondition, runplan.ErrRerunOfTestUnsupported.Error()},
		{"nothing to execute", runplan.ErrNothingToRerun, codes.FailedPrecondition, runplan.ErrNothingToRerun.Error()},
		{"no live release", fmt.Errorf("%w: no release is live", runplan.ErrEmptySelection), codes.FailedPrecondition, "runplan: the selection is empty: no release is live"},
		{"no source release", fmt.Errorf("%w: run x", runstart.ErrSourceHasNoRelease), codes.FailedPrecondition, "runstart: the source run names no release: run x"},
		{"undeclared node type", fmt.Errorf("%w: raw.a", runplan.ErrInvalidNodeType), codes.FailedPrecondition, runplan.ErrInvalidNodeType.Error() + ": raw.a"},
		{"live release moved", runstart.ErrLiveReleaseMoved, codes.FailedPrecondition, "the live release changed; preview again"},
		{"live release moved, wrapped", fmt.Errorf("rebase: %w", runstart.ErrLiveReleaseMoved), codes.FailedPrecondition, "the live release changed; preview again"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, ok := rebaseRefusal(tc.err)
			require.True(t, ok)
			assert.Equal(t, tc.code, st.Code())
			assert.Equal(t, tc.msg, st.Message())
		})
	}
	_, ok := rebaseRefusal(errors.New("connection reset"))
	assert.False(t, ok, "an infrastructure error is no refusal")
}

// TriggerRebase passes the option and the previewed release to the use case
// and answers with the new run.
func TestRebaseHandler_PassesTheRequestToTheUseCase(t *testing.T) {
	starter := &fakeRebaseStarter{}
	h := NewRebaseHandler(starter, func() uow.UnitOfWork { return &uow.FakeUnitOfWork{} }, newTestLogger())
	src := uuid.New()

	resp, err := h.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{
		SourceRunId: src.String(), IncludeChangedUpstream: true, ExpectedReleaseId: "rel-7",
	})

	require.NoError(t, err)
	assert.Equal(t, "6f1d7f0e-3c55-4c4a-9d55-0f2d8f5b7c11", resp.RunId)
	assert.Equal(t, "daily", resp.ScheduleName)
	assert.Equal(t, src, starter.in.SourceRunID)
	assert.True(t, starter.in.IncludeChangedUpstream)
	assert.Equal(t, "rel-7", starter.in.ExpectedReleaseID)
}

// The live release having moved since the preview is FailedPrecondition with
// exactly the message the ui recognises.
func TestRebaseHandler_AMovedLiveReleaseIsFailedPreconditionWithTheExactMessage(t *testing.T) {
	h := NewRebaseHandler(&fakeRebaseStarter{err: runstart.ErrLiveReleaseMoved}, func() uow.UnitOfWork { return &uow.FakeUnitOfWork{} }, newTestLogger())

	_, err := h.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: uuid.NewString(), ExpectedReleaseId: "rel-6"})

	assert.Equal(t, status.Error(codes.FailedPrecondition, "the live release changed; preview again"), err)
}

func TestRebaseHandler_AnUnknownFailureIsInternal(t *testing.T) {
	h := NewRebaseHandler(&fakeRebaseStarter{err: errors.New("connection reset")}, func() uow.UnitOfWork { return &uow.FakeUnitOfWork{} }, newTestLogger())

	_, err := h.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: uuid.NewString()})

	requireGRPCCode(t, err, codes.Internal)
}

func TestPreviewRebaseHandler_RejectsAMissingOrMalformedSourceRunID(t *testing.T) {
	h := NewPreviewRebaseHandler(&fakeRebasePreviewReader{}, newTestLogger())
	for _, id := range []string{"", "not-a-uuid"} {
		_, err := h.PreviewRebase(context.Background(), &statev1.PreviewRebaseRequest{SourceRunId: id})
		requireGRPCCode(t, err, codes.InvalidArgument)
	}
}

// The preview answers with the live release, the counts, and each changed
// upstream node in the order the selection lists them, with its last
// successful duration in seconds (0 for a node without one).
func TestPreviewRebaseHandler_MapsThePreview(t *testing.T) {
	reader := &fakeRebasePreviewReader{preview: runreads.RebasePreview{
		Release: run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12},
		Preview: runplan.Preview{
			Plan:            runplan.Plan{Tasks: make([]runplan.PlannedTask, 9)},
			FailedOrSkipped: 2, New: 1, ChangedUpstream: 2, Downstream: 1, Inherited: 3,
		},
		Changed: []runreads.ChangedUpstream{
			{
				ChangedNode: runplan.ChangedNode{Key: "raw.invoices", ServiceName: "billing", SchemaName: "raw", TableName: "Invoices", Downstream: 4},
				LastSuccess: 252500 * time.Millisecond,
			},
			{ChangedNode: runplan.ChangedNode{Key: "mart.orders", ServiceName: "sales", SchemaName: "mart", TableName: "orders", Downstream: 1}},
		},
	}}
	h := NewPreviewRebaseHandler(reader, newTestLogger())
	src := uuid.New()

	resp, err := h.PreviewRebase(context.Background(), &statev1.PreviewRebaseRequest{SourceRunId: src.String(), IncludeChangedUpstream: true})

	require.NoError(t, err)
	assert.Equal(t, src, reader.sourceID)
	assert.True(t, reader.include)
	assert.Equal(t, "rel-7", resp.ReleaseId)
	assert.Equal(t, int64(12), resp.PromotionSeq)
	assert.Equal(t, int32(9), resp.TotalNodes)
	assert.Equal(t, int32(6), resp.WillRun)
	assert.Equal(t, int32(2), resp.FailedOrSkipped)
	assert.Equal(t, int32(1), resp.NewNodes)
	assert.Equal(t, int32(2), resp.ChangedUpstream)
	assert.Equal(t, int32(1), resp.Downstream)
	assert.Equal(t, int32(3), resp.Inherited)
	require.Len(t, resp.ChangedNodes, 2)
	assert.Equal(t, "billing", resp.ChangedNodes[0].ServiceName)
	assert.Equal(t, "raw", resp.ChangedNodes[0].SchemaName)
	assert.Equal(t, "Invoices", resp.ChangedNodes[0].TableName)
	assert.Equal(t, int32(4), resp.ChangedNodes[0].Downstream)
	assert.InDelta(t, 252.5, resp.ChangedNodes[0].LastSuccessSeconds, 1e-9)
	assert.Equal(t, "sales", resp.ChangedNodes[1].ServiceName)
	assert.Zero(t, resp.ChangedNodes[1].LastSuccessSeconds, "a node that never succeeded has no duration")
}

// A preview is refused like a rebase, through the same mapping.
func TestPreviewRebaseHandler_MapsRefusals(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("get source: %w", postgres.ErrNotFound), codes.NotFound},
		{run.ErrSourceMustBeTerminallyFailedOrCancelled, codes.FailedPrecondition},
		{runplan.ErrNothingToRerun, codes.FailedPrecondition},
		{errors.New("connection reset"), codes.Internal},
	} {
		h := NewPreviewRebaseHandler(&fakeRebasePreviewReader{err: tc.err}, newTestLogger())
		_, err := h.PreviewRebase(context.Background(), &statev1.PreviewRebaseRequest{SourceRunId: uuid.NewString()})
		requireGRPCCode(t, err, tc.code)
	}
}
