package runservice_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/runplan"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/carolsimone/continuo/state/service/commands"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/runstart"
)

// fakeCommands answers each command with the run its tenant holds: a command
// for a tenant with no run is the run-not-found refusal, so a handler that
// acts for another tenant than the request's answers with the wrong run or
// none. It records the last command of each kind.
type fakeCommands struct {
	runOf map[string]uuid.UUID
	err   error

	start   svchandlers.StartRunCommand
	rerun   svchandlers.TriggerRerunCommand
	rebase  svchandlers.TriggerRebaseCommand
	single  svchandlers.TriggerSingleNodeRunCommand
	active  svchandlers.CancelActiveRunCommand
	cancel  svchandlers.CancelRunCommand
	calls   int
	preview runreads.RebasePreview
}

func (f *fakeCommands) result(tenantID string) (svchandlers.CommandResult, error) {
	f.calls++
	if f.err != nil {
		return svchandlers.CommandResult{}, f.err
	}
	id, ok := f.runOf[tenantID]
	if !ok {
		return svchandlers.CommandResult{}, fmt.Errorf("get source: %w", repository.ErrRunNotFound)
	}
	return svchandlers.CommandResult{RunID: id, ScheduleName: "schedule-of-" + tenantID}, nil
}

func (f *fakeCommands) StartRun(_ context.Context, c svchandlers.StartRunCommand) (svchandlers.CommandResult, error) {
	f.start = c
	return f.result(c.TenantID)
}

func (f *fakeCommands) TriggerRerun(_ context.Context, c svchandlers.TriggerRerunCommand) (svchandlers.CommandResult, error) {
	f.rerun = c
	return f.result(c.TenantID)
}

func (f *fakeCommands) TriggerRebase(_ context.Context, c svchandlers.TriggerRebaseCommand) (svchandlers.CommandResult, error) {
	f.rebase = c
	return f.result(c.TenantID)
}

func (f *fakeCommands) TriggerSingleNodeRun(_ context.Context, c svchandlers.TriggerSingleNodeRunCommand) (svchandlers.CommandResult, error) {
	f.single = c
	return f.result(c.TenantID)
}

func (f *fakeCommands) CancelActiveRun(_ context.Context, c svchandlers.CancelActiveRunCommand) (svchandlers.CommandResult, error) {
	f.active = c
	return f.result(c.TenantID)
}

func (f *fakeCommands) CancelRun(_ context.Context, c svchandlers.CancelRunCommand) (uuid.UUID, error) {
	f.cancel = c
	res, err := f.result(c.TenantID)
	return res.RunID, err
}

func (f *fakeCommands) Preview(_ context.Context, tenantID string, _ uuid.UUID, _ bool) (runreads.RebasePreview, error) {
	if _, err := f.result(tenantID); err != nil {
		return runreads.RebasePreview{}, err
	}
	return f.preview, nil
}

// commandCalls are one valid call of each command that takes a request id,
// sent with requestID.
func commandCalls(srv runv1.RunServiceServer, ctx context.Context, requestID string) map[string]func() error {
	src := uuid.NewString()
	return map[string]func() error{
		"StartRun": func() error {
			_, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: "daily"})
			return err
		},
		"TriggerRerun": func() error {
			_, err := srv.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: requestID, SourceRunId: src})
			return err
		},
		"TriggerRebase": func() error {
			_, err := srv.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: requestID, SourceRunId: src})
			return err
		},
		"TriggerSingleNodeRun": func() error {
			_, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
				RequestId: requestID, ServiceName: "service-1", SchemaName: "mart", TableName: "orders", MetadataSource: "latest",
			})
			return err
		},
		"CancelActiveRun": func() error {
			_, err := srv.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{RequestId: requestID, ScheduleName: "daily"})
			return err
		},
	}
}

func TestStartRun_RequiresARequestID(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	for _, id := range []string{"", "not-a-uuid"} {
		_, err := srv.StartRun(ctxOf(tenantA), &runv1.StartRunRequest{RequestId: id, ScheduleName: "daily"})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "request_id %q", id)
	}
	assert.Zero(t, cmds.calls, "a refused request reaches no command")
}

// Every command that is idempotent per request id refuses an empty or
// malformed one before it acts.
func TestCommands_RequireARequestID(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	for _, id := range []string{"", "not-a-uuid"} {
		for name, call := range commandCalls(srv, ctxOf(tenantA), id) {
			assert.Equal(t, codes.InvalidArgument, status.Code(call()), "%s with request_id %q", name, id)
		}
	}
	assert.Zero(t, cmds.calls)
}

// Each command acts for the tenant of its request: the fake answers only the
// tenant's own run, so a handler that acted for any other tenant would
// answer NotFound or another run.
func TestCommands_ActForTheRequestsTenant(t *testing.T) {
	acmeRun := uuid.New()
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New(), tenantB: acmeRun}}
	store := newStore()
	store.putRun(tenantB, projection.RunDetail{RunID: acmeRun, Status: run.RunStatusCancelling, CreatedAt: base})
	srv := newCommandServer(store, cmds)
	ctx := ctxOf(tenantB)
	requestID := uuid.NewString()

	start, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: "daily"})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), start.GetRunId())
	assert.Equal(t, tenantB, cmds.start.TenantID)

	rerun, err := srv.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: requestID, SourceRunId: uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), rerun.GetRunId())
	assert.Equal(t, "schedule-of-"+tenantB, rerun.GetScheduleName())

	rebase, err := srv.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: requestID, SourceRunId: uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), rebase.GetRunId())
	assert.Equal(t, tenantB, cmds.rebase.TenantID)

	single, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: requestID, ServiceName: "service-1", SchemaName: "mart", TableName: "orders", MetadataSource: "latest",
	})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), single.GetRunId())
	assert.Equal(t, tenantB, cmds.single.TenantID)

	active, err := srv.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{RequestId: requestID, ScheduleName: "daily"})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), active.GetRunId())

	cancelled, err := srv.CancelRun(ctx, &runv1.CancelRunRequest{RunId: acmeRun.String()})
	require.NoError(t, err)
	assert.Equal(t, acmeRun.String(), cancelled.GetRun().GetRunId())
	assert.Equal(t, "cancelling", cancelled.GetRun().GetStatus(), "the run as the cancel left it, read for the request's tenant")
	assert.Equal(t, tenantB, cmds.cancel.TenantID)

	_, err = srv.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: uuid.NewString()})
	require.NoError(t, err)
}

// A request whose tenant holds no run is answered NotFound, whatever run the
// default tenant holds.
func TestCommands_ARunOfAnotherTenantIsNotFound(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	ctx := ctxOf(tenantB)
	_, err := srv.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: uuid.NewString(), SourceRunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = srv.CancelRun(ctx, &runv1.CancelRunRequest{RunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = srv.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// A command that reaches state without a tenant is Internal before it acts.
func TestCommands_ARequestWithoutATenantIsInternal(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	for name, call := range commandCalls(srv, context.Background(), uuid.NewString()) {
		assert.Equal(t, codes.Internal, status.Code(call()), name)
	}
	_, err := srv.CancelRun(context.Background(), &runv1.CancelRunRequest{RunId: uuid.NewString()})
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Zero(t, cmds.calls)
}

// The parsed parameters reach the command with their defaults applied.
func TestCommands_PassTheParsedParameters(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	ctx := ctxOf(tenantA)
	requestID := uuid.New()
	src := uuid.New()

	_, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID.String(), ScheduleName: "daily", Operation: "build"})
	require.NoError(t, err)
	assert.Equal(t, svchandlers.StartRunCommand{
		TenantID: tenantA, RequestID: requestID, ScheduleName: "daily", Operation: model.OperationBuild, Initiator: cmds.start.Initiator,
	}, cmds.start)

	_, err = srv.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{
		RequestId: requestID.String(), SourceRunId: src.String(), IncludeChangedUpstream: true, ExpectedReleaseId: "rel-7",
	})
	require.NoError(t, err)
	assert.Equal(t, requestID, cmds.rebase.RequestID)
	assert.Equal(t, src, cmds.rebase.SourceRunID)
	assert.True(t, cmds.rebase.IncludeChangedUpstream)
	assert.Equal(t, "rel-7", cmds.rebase.ExpectedReleaseID)

	_, err = srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: requestID.String(), ServiceName: "service-1", SchemaName: "mart", TableName: "orders",
		MetadataSource: "snapshot_of_run", SourceRunId: src.String(), Operation: "full_refresh",
	})
	require.NoError(t, err)
	assert.Equal(t, run.NodeID{ServiceName: "service-1", SchemaName: "mart", TableName: "orders"}, cmds.single.Target)
	assert.Equal(t, run.MetadataSourceSnapshotOfRun, cmds.single.MetadataSource)
	require.NotNil(t, cmds.single.SourceRunID)
	assert.Equal(t, src, *cmds.single.SourceRunID)
	assert.Equal(t, model.OperationFullRefresh, cmds.single.Operation)

	_, err = srv.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
		RequestId: requestID.String(), ScheduleName: "daily", CancelledBy: "bob", CancellationReason: "wrong data",
	})
	require.NoError(t, err)
	assert.Equal(t, "daily", cmds.active.ScheduleName)
	assert.Equal(t, "bob", cmds.active.CancelledBy, "an unauthenticated caller's own label is kept")
	assert.Equal(t, "wrong data", cmds.active.CancellationReason)
}

func TestCommands_RefuseMalformedParameters(t *testing.T) {
	cmds := &fakeCommands{runOf: map[string]uuid.UUID{tenantA: uuid.New()}}
	srv := newCommandServer(newStore(), cmds)
	ctx := ctxOf(tenantA)
	id := uuid.NewString()
	calls := map[string]func() error{
		"StartRun without a schedule": func() error {
			_, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: id})
			return err
		},
		"StartRun of an unknown operation": func() error {
			_, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: id, ScheduleName: "daily", Operation: "explode"})
			return err
		},
		"StartRun of a full refresh": func() error {
			_, err := srv.StartRun(ctx, &runv1.StartRunRequest{RequestId: id, ScheduleName: "daily", Operation: "full_refresh"})
			return err
		},
		"TriggerRerun of a malformed source": func() error {
			_, err := srv.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: id, SourceRunId: "x"})
			return err
		},
		"TriggerRebase without a source": func() error {
			_, err := srv.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: id})
			return err
		},
		"TriggerSingleNodeRun without a table": func() error {
			_, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
				RequestId: id, ServiceName: "service-1", SchemaName: "mart", MetadataSource: "latest",
			})
			return err
		},
		"TriggerSingleNodeRun of an unknown metadata source": func() error {
			_, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
				RequestId: id, ServiceName: "service-1", SchemaName: "mart", TableName: "orders", MetadataSource: "yesterday",
			})
			return err
		},
		"TriggerSingleNodeRun of a snapshot without a source": func() error {
			_, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
				RequestId: id, ServiceName: "service-1", SchemaName: "mart", TableName: "orders", MetadataSource: "snapshot_of_run",
			})
			return err
		},
		"TriggerSingleNodeRun of the latest with a source": func() error {
			_, err := srv.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
				RequestId: id, ServiceName: "service-1", SchemaName: "mart", TableName: "orders", MetadataSource: "latest",
				SourceRunId: uuid.NewString(),
			})
			return err
		},
		"CancelActiveRun without a schedule": func() error {
			_, err := srv.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{RequestId: id})
			return err
		},
		"CancelRun of a malformed run": func() error {
			_, err := srv.CancelRun(ctx, &runv1.CancelRunRequest{RunId: "x"})
			return err
		},
		"PreviewRebase of a malformed source": func() error {
			_, err := srv.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: "x"})
			return err
		},
	}
	for name, call := range calls {
		assert.Equal(t, codes.InvalidArgument, status.Code(call()), name)
	}
	assert.Zero(t, cmds.calls)
}

// A request id reused for another command is AlreadyExists with the contract's
// message; a claim that acted on another run is Internal; a claim that keeps
// losing to calls that roll back is Aborted, which the caller may retry with
// the same id.
func TestCommands_MapTheRequestErrors(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
		msg  string
	}{
		{commandrequest.ErrReused, codes.AlreadyExists, "request_id reused with different parameters"},
		{fmt.Errorf("%w: command start_run", commands.ErrWrongRun), codes.Internal, ""},
		{fmt.Errorf("%w: command start_run", commands.ErrClaimContended), codes.Aborted, ""},
	}
	for _, c := range cases {
		cmds := &fakeCommands{err: c.err}
		srv := newCommandServer(newStore(), cmds)
		for name, call := range commandCalls(srv, ctxOf(tenantA), uuid.NewString()) {
			err := call()
			assert.Equal(t, c.code, status.Code(err), "%s answering %v", name, c.err)
			if c.msg != "" {
				assert.Equal(t, c.msg, status.Convert(err).Message(), name)
			}
		}
	}
}

// The refusals of a command keep the codes the contract names.
func TestCommands_MapTheRefusals(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
	}{
		{run.ErrScheduleNotFound, codes.NotFound},
		{runplan.ErrTargetNotFound, codes.NotFound},
		{run.ErrTaskNotFound, codes.NotFound},
		{runplan.ErrFullRefreshUnsupported, codes.InvalidArgument},
		{run.ErrScheduleHasActiveRun, codes.FailedPrecondition},
		{runplan.ErrEmptySelection, codes.FailedPrecondition},
		{runplan.ErrNoTests, codes.FailedPrecondition},
		{runplan.ErrInvalidNodeType, codes.FailedPrecondition},
		{runplan.ErrNothingToRerun, codes.FailedPrecondition},
		{runplan.ErrRerunOfTestUnsupported, codes.FailedPrecondition},
		{runstart.ErrSourceHasNoRelease, codes.FailedPrecondition},
		{run.ErrSourceMustBeTerminallyFailedOrCancelled, codes.FailedPrecondition},
		{run.ErrNothingToRerun, codes.FailedPrecondition},
		{run.ErrNothingToRebase, codes.FailedPrecondition},
		{run.ErrAlreadyTerminal, codes.FailedPrecondition},
		{svchandlers.ErrSourceRunNotTerminal, codes.FailedPrecondition},
		{svchandlers.ErrNoActiveRun, codes.FailedPrecondition},
		{errBoom, codes.Internal},
	}
	for _, c := range cases {
		cmds := &fakeCommands{err: fmt.Errorf("wrapped: %w", c.err)}
		srv := newCommandServer(newStore(), cmds)
		for name, call := range commandCalls(srv, ctxOf(tenantA), uuid.NewString()) {
			assert.Equal(t, c.code, status.Code(call()), "%s answering %v", name, c.err)
		}
	}

	cmds := &fakeCommands{err: fmt.Errorf("select: %w", runstart.ErrLiveReleaseMoved)}
	srv := newCommandServer(newStore(), cmds)
	_, err := srv.TriggerRebase(ctxOf(tenantA), &runv1.TriggerRebaseRequest{RequestId: uuid.NewString(), SourceRunId: uuid.NewString()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Equal(t, "the live release changed; preview again", status.Convert(err).Message())
}

// A cancel of a run that ended succeeded, failed or skipped is
// FailedPrecondition.
func TestCancelRun_AnEndedRunIsFailedPrecondition(t *testing.T) {
	cmds := &fakeCommands{err: fmt.Errorf("cancel: %w", run.ErrAlreadyTerminal)}
	srv := newCommandServer(newStore(), cmds)
	_, err := srv.CancelRun(ctxOf(tenantA), &runv1.CancelRunRequest{RunId: uuid.NewString()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// A schedule with no active run is FailedPrecondition.
func TestCancelActiveRun_NoActiveRunIsFailedPrecondition(t *testing.T) {
	cmds := &fakeCommands{err: svchandlers.ErrNoActiveRun}
	srv := newCommandServer(newStore(), cmds)
	_, err := srv.CancelActiveRun(ctxOf(tenantA), &runv1.CancelActiveRunRequest{RequestId: uuid.NewString(), ScheduleName: "daily"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
