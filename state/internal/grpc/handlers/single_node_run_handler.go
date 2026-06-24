package handlers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SingleNodeRunHandler implements the TriggerSingleNodeRun gRPC method.
type SingleNodeRunHandler struct {
	useCase    *svchandlers.TriggerSingleNodeRunHandler
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewSingleNodeRunHandler creates a new SingleNodeRunHandler.
func NewSingleNodeRunHandler(
	useCase *svchandlers.TriggerSingleNodeRunHandler,
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
) *SingleNodeRunHandler {
	return &SingleNodeRunHandler{useCase: useCase, uowFactory: uowFactory, logger: logger}
}

// TriggerSingleNodeRun validates the request and delegates to TriggerSingleNodeRunHandler.
func (h *SingleNodeRunHandler) TriggerSingleNodeRun(ctx context.Context, req *statev1.TriggerSingleNodeRunRequest) (*statev1.TriggerSingleNodeRunResponse, error) {
	h.logger.Info("TriggerSingleNodeRun called",
		"service_name", req.ServiceName,
		"schema_name", req.SchemaName,
		"table_name", req.TableName,
		"metadata_source", req.MetadataSource,
	)

	// Validate identity fields.
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, schema_name, and table_name are required")
	}

	// Validate metadata_source and source_run_id consistency.
	switch req.MetadataSource {
	case "latest":
		if req.SourceRunId != "" {
			return nil, status.Errorf(codes.InvalidArgument, "source_run_id must be empty when metadata_source is 'latest'")
		}
	case "snapshot_of_run":
		if req.SourceRunId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required when metadata_source is 'snapshot_of_run'")
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "metadata_source must be 'latest' or 'snapshot_of_run', got %q", req.MetadataSource)
	}

	var srcPtr *uuid.UUID
	if req.SourceRunId != "" {
		parsed, err := uuid.Parse(req.SourceRunId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
		}
		srcPtr = &parsed
	}

	in := svchandlers.TriggerSingleNodeRunInput{
		Target:         run.NodeID{ServiceName: req.ServiceName, SchemaName: req.SchemaName, TableName: req.TableName},
		MetadataSource: run.MetadataSource(req.MetadataSource),
		SourceRunID:    srcPtr,
		Initiator:      identity.FromContext(ctx),
	}

	id, name, err := h.useCase.Handle(ctx, h.uowFactory(), in)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "source run not found")
		case errors.Is(err, run.ErrTaskNotFound):
			return nil, status.Errorf(codes.NotFound, "source run does not have a task matching the identity")
		case errors.Is(err, svchandlers.ErrSourceRunNotTerminal):
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		default:
			h.logger.Error("TriggerSingleNodeRun failed", "error", err)
			return nil, status.Errorf(codes.Internal, "single_node_run: %v", err)
		}
	}
	return &statev1.TriggerSingleNodeRunResponse{RunId: id.String(), ScheduleName: name}, nil
}
