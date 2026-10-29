package runservice

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/commandrequest"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/carolsimone/continuo/run-controller/service/commands"
	svchandlers "github.com/carolsimone/continuo/run-controller/service/handlers"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
)

// StartRun starts a run of the request's tenant's schedule, once per
// request id.
func (s *Server) StartRun(ctx context.Context, req *runv1.StartRunRequest) (*runv1.StartRunResponse, error) {
	tenantID, requestID, err := commandScope(ctx, req.GetRequestId())
	if err != nil {
		return nil, err
	}
	if req.GetScheduleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	if operation.IsSingleNodeOnly() {
		return nil, status.Errorf(codes.InvalidArgument, "operation %q targets a single node; use TriggerSingleNodeRun", operation)
	}
	res, err := s.commands.StartRun(ctx, svchandlers.StartRunCommand{
		TenantID: tenantID, RequestID: requestID, ScheduleName: req.GetScheduleName(),
		Operation: operation, Initiator: identity.FromContext(ctx),
	})
	if err != nil {
		return nil, s.commandError("StartRun", err, "schedule_name", req.GetScheduleName())
	}
	return &runv1.StartRunResponse{RunId: res.RunID.String()}, nil
}

// TriggerRerun reruns a failed or cancelled run of the request's tenant, once
// per request id.
func (s *Server) TriggerRerun(ctx context.Context, req *runv1.TriggerRerunRequest) (*runv1.TriggerRerunResponse, error) {
	tenantID, requestID, err := commandScope(ctx, req.GetRequestId())
	if err != nil {
		return nil, err
	}
	sourceID, err := parseUUID("source_run_id", req.GetSourceRunId())
	if err != nil {
		return nil, err
	}
	res, err := s.commands.TriggerRerun(ctx, svchandlers.TriggerRerunCommand{
		TenantID: tenantID, RequestID: requestID, SourceRunID: sourceID, Initiator: identity.FromContext(ctx),
	})
	if err != nil {
		return nil, s.commandError("TriggerRerun", err, "source_run_id", sourceID)
	}
	return &runv1.TriggerRerunResponse{RunId: res.RunID.String(), ScheduleName: res.ScheduleName}, nil
}

// TriggerRebase rebases a failed or cancelled run of the request's tenant onto
// the live release, once per request id.
func (s *Server) TriggerRebase(ctx context.Context, req *runv1.TriggerRebaseRequest) (*runv1.TriggerRebaseResponse, error) {
	tenantID, requestID, err := commandScope(ctx, req.GetRequestId())
	if err != nil {
		return nil, err
	}
	sourceID, err := parseUUID("source_run_id", req.GetSourceRunId())
	if err != nil {
		return nil, err
	}
	res, err := s.commands.TriggerRebase(ctx, svchandlers.TriggerRebaseCommand{
		RequestID: requestID,
		TriggerRebaseInput: svchandlers.TriggerRebaseInput{
			TenantID: tenantID, SourceRunID: sourceID, IncludeChangedUpstream: req.GetIncludeChangedUpstream(),
			ExpectedReleaseID: req.GetExpectedReleaseId(), Initiator: identity.FromContext(ctx),
		},
	})
	if err != nil {
		return nil, s.commandError("TriggerRebase", err, "source_run_id", sourceID)
	}
	return &runv1.TriggerRebaseResponse{RunId: res.RunID.String(), ScheduleName: res.ScheduleName}, nil
}

// TriggerSingleNodeRun runs one node of the request's tenant, once per
// request id.
func (s *Server) TriggerSingleNodeRun(ctx context.Context, req *runv1.TriggerSingleNodeRunRequest) (*runv1.TriggerSingleNodeRunResponse, error) {
	tenantID, requestID, err := commandScope(ctx, req.GetRequestId())
	if err != nil {
		return nil, err
	}
	if req.GetServiceName() == "" || req.GetSchemaName() == "" || req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name, schema_name and table_name are required")
	}
	var sourceID *uuid.UUID
	switch run.MetadataSource(req.GetMetadataSource()) {
	case run.MetadataSourceLatest:
		if req.GetSourceRunId() != "" {
			return nil, status.Error(codes.InvalidArgument, "source_run_id must be empty when metadata_source is 'latest'")
		}
	case run.MetadataSourceSnapshotOfRun:
		id, perr := parseUUID("source_run_id", req.GetSourceRunId())
		if perr != nil {
			return nil, perr
		}
		sourceID = &id
	default:
		return nil, status.Errorf(codes.InvalidArgument, "metadata_source must be 'latest' or 'snapshot_of_run', got %q", req.GetMetadataSource())
	}
	operation, err := model.ParseOperation(req.GetOperation())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	target := run.NodeID{ServiceName: req.GetServiceName(), SchemaName: req.GetSchemaName(), TableName: req.GetTableName()}
	res, err := s.commands.TriggerSingleNodeRun(ctx, svchandlers.TriggerSingleNodeRunCommand{
		RequestID: requestID,
		TriggerSingleNodeRunInput: svchandlers.TriggerSingleNodeRunInput{
			TenantID: tenantID, Target: target, MetadataSource: run.MetadataSource(req.GetMetadataSource()),
			Operation: operation, SourceRunID: sourceID, Initiator: identity.FromContext(ctx),
		},
	})
	if err != nil {
		return nil, s.commandError("TriggerSingleNodeRun", err, "target", target)
	}
	return &runv1.TriggerSingleNodeRunResponse{RunId: res.RunID.String(), ScheduleName: res.ScheduleName}, nil
}

// PreviewRebase reports what TriggerRebase would run for the same source and
// option on the live release, without creating a run. It is a read: it takes
// no request id and stores nothing.
func (s *Server) PreviewRebase(ctx context.Context, req *runv1.PreviewRebaseRequest) (*runv1.PreviewRebaseResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	sourceID, err := parseUUID("source_run_id", req.GetSourceRunId())
	if err != nil {
		return nil, err
	}
	got, err := s.reads.Rebases.Preview(ctx, tenantID, sourceID, req.GetIncludeChangedUpstream())
	if err != nil {
		return nil, s.commandError("PreviewRebase", err, "source_run_id", sourceID)
	}
	pv := got.Preview
	resp := &runv1.PreviewRebaseResponse{
		ReleaseId:       got.Release.ReleaseID,
		PromotionSeq:    got.Release.PromotionSeq,
		TotalNodes:      num.ClampInt32(pv.Total()),
		WillRun:         num.ClampInt32(pv.WillRun()),
		FailedOrSkipped: num.ClampInt32(pv.FailedOrSkipped),
		NewNodes:        num.ClampInt32(pv.New),
		ChangedUpstream: num.ClampInt32(pv.ChangedUpstream),
		Downstream:      num.ClampInt32(pv.Downstream),
		Inherited:       num.ClampInt32(pv.Inherited),
		ChangedNodes:    make([]*runv1.ChangedUpstreamNode, 0, len(got.Changed)),
	}
	for _, c := range got.Changed {
		resp.ChangedNodes = append(resp.ChangedNodes, &runv1.ChangedUpstreamNode{
			ServiceName:        c.ServiceName,
			SchemaName:         c.SchemaName,
			TableName:          c.TableName,
			Downstream:         num.ClampInt32(c.Downstream),
			LastSuccessSeconds: c.LastSuccess.Seconds(),
		})
	}
	return resp, nil
}

// CancelRun cancels a run of the request's tenant and answers with the run as
// the cancel left it. A run that is already cancelling or cancelled is
// answered as it is.
func (s *Server) CancelRun(ctx context.Context, req *runv1.CancelRunRequest) (*runv1.RunResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID("run_id", req.GetRunId())
	if err != nil {
		return nil, err
	}
	id, err := s.commands.CancelRun(ctx, svchandlers.CancelRunCommand{
		TenantID: tenantID, RunID: runID,
		CancelledBy: cancelledBy(ctx, req.GetCancelledBy()), CancellationReason: req.GetCancellationReason(),
	})
	if errors.Is(err, repository.ErrRunNotFound) {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	if err != nil {
		return nil, s.commandError("CancelRun", err, "run_id", runID)
	}
	detail, err := s.reads.Runs.GetRun(ctx, tenantID, id)
	if err != nil {
		return nil, s.internalError("CancelRun", err, "run_id", runID)
	}
	return &runv1.RunResponse{Run: toProtoRun(detail)}, nil
}

// CancelActiveRun cancels the active run of the request's tenant's schedule,
// once per request id, and answers with its id.
func (s *Server) CancelActiveRun(ctx context.Context, req *runv1.CancelActiveRunRequest) (*runv1.CancelActiveRunResponse, error) {
	tenantID, requestID, err := commandScope(ctx, req.GetRequestId())
	if err != nil {
		return nil, err
	}
	if req.GetScheduleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	res, err := s.commands.CancelActiveRun(ctx, svchandlers.CancelActiveRunCommand{
		TenantID: tenantID, RequestID: requestID, ScheduleName: req.GetScheduleName(),
		CancelledBy: cancelledBy(ctx, req.GetCancelledBy()), CancellationReason: req.GetCancellationReason(),
	})
	if err != nil {
		return nil, s.commandError("CancelActiveRun", err, "schedule_name", req.GetScheduleName())
	}
	return &runv1.CancelActiveRunResponse{RunId: res.RunID.String()}, nil
}

// commandScope returns the tenant a command acts for and its request id; a
// request with no tenant is Internal and an empty or malformed request id is
// InvalidArgument.
func commandScope(ctx context.Context, rawRequestID string) (string, uuid.UUID, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return "", uuid.Nil, err
	}
	requestID, err := parseUUID("request_id", rawRequestID)
	if err != nil {
		return "", uuid.Nil, err
	}
	return tenantID, requestID, nil
}

// cancelledBy is who a cancel records as cancelling: the authenticated user
// the request's identity names, else the request's own cancelled_by, else the
// system sentinel.
func cancelledBy(ctx context.Context, requested string) string {
	id := identity.FromContext(ctx)
	if !id.IsSystem() {
		return id.UserID
	}
	if requested != "" {
		return requested
	}
	return identity.SystemUserID
}

// commandError maps the error of a command to the status the RPC returns.
//
//   - A request id reused with other parameters is AlreadyExists.
//   - A request id whose claim keeps being lost to calls that roll back is
//     Aborted: the conflict is with concurrent calls of the same request, and
//     the caller may send it again with the same id.
//   - A command that acted on another run than it claimed is Internal: it is
//     a defect in run-controller, and nothing it wrote was kept.
//   - A run, schedule, node or task that does not exist is NotFound; a full
//     refresh of a node type that cannot be rebuilt, and a schedule name
//     longer than a run carries, are InvalidArgument; a live release other
//     than the one the caller previewed, and every other refusal of the run's
//     selection or its source, is FailedPrecondition.
//
// Anything else is logged and Internal.
func (s *Server) commandError(rpc string, err error, attrs ...any) error {
	switch {
	case errors.Is(err, commandrequest.ErrReused):
		return status.Error(codes.AlreadyExists, commandrequest.ErrReused.Error())
	case errors.Is(err, commands.ErrClaimContended):
		return status.Errorf(codes.Aborted, "%s: %v", rpc, err)
	case errors.Is(err, commands.ErrWrongRun):
		return s.internalError(rpc, err, attrs...)
	case errors.Is(err, runstart.ErrLiveReleaseMoved):
		return status.Error(codes.FailedPrecondition, runstart.ErrLiveReleaseMoved.Error())
	case errors.Is(err, repository.ErrRunNotFound):
		return status.Error(codes.NotFound, "source run not found")
	case errors.Is(err, run.ErrTaskNotFound):
		return status.Error(codes.NotFound, "source run does not have a task matching the identity")
	case errors.Is(err, run.ErrScheduleNotFound),
		errors.Is(err, runplan.ErrTargetNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, runplan.ErrFullRefreshUnsupported),
		errors.Is(err, run.ErrScheduleNameTooLong):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, run.ErrScheduleHasActiveRun),
		errors.Is(err, run.ErrAlreadyTerminal),
		errors.Is(err, run.ErrSourceMustBeTerminallyFailedOrCancelled),
		errors.Is(err, run.ErrNothingToRerun),
		errors.Is(err, run.ErrNothingToRebase),
		errors.Is(err, runplan.ErrEmptySelection),
		errors.Is(err, runplan.ErrNoTests),
		errors.Is(err, runplan.ErrRerunOfTestUnsupported),
		errors.Is(err, runplan.ErrInvalidNodeType),
		errors.Is(err, runplan.ErrNothingToRerun),
		errors.Is(err, runstart.ErrSourceHasNoRelease),
		errors.Is(err, svchandlers.ErrSourceRunNotTerminal),
		errors.Is(err, svchandlers.ErrNoActiveRun):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return s.internalError(rpc, err, attrs...)
}
