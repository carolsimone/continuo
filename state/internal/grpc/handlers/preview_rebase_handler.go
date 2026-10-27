package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/num"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RebasePreviewReader previews a rebase of a tenant's run: the read behind
// PreviewRebase.
type RebasePreviewReader interface {
	Preview(ctx context.Context, tenantID string, sourceID uuid.UUID, includeChangedUpstream bool) (runreads.RebasePreview, error)
}

// PreviewRebaseHandler implements the PreviewRebase gRPC method: what
// TriggerRebase would run for the same source and option on the live release,
// without creating a run.
type PreviewRebaseHandler struct {
	reader RebasePreviewReader
	logger *slog.Logger
}

// NewPreviewRebaseHandler creates a PreviewRebaseHandler over reader.
func NewPreviewRebaseHandler(reader RebasePreviewReader, logger *slog.Logger) *PreviewRebaseHandler {
	return &PreviewRebaseHandler{reader: reader, logger: logger}
}

// PreviewRebase validates the request, previews the rebase and maps the
// preview to the response: the live release, the counts by reason, and the
// changed upstream nodes in the order the selection lists them (service,
// schema, table), each with its last successful duration in seconds, 0 when
// it has none. The source is a run of the request's tenant: another tenant's
// run is NotFound.
func (h *PreviewRebaseHandler) PreviewRebase(ctx context.Context, req *statev1.PreviewRebaseRequest) (*statev1.PreviewRebaseResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	srcID, err := parseSourceRunID(req.GetSourceRunId())
	if err != nil {
		return nil, err
	}
	got, err := h.reader.Preview(ctx, tenantID, srcID, req.GetIncludeChangedUpstream())
	if err != nil {
		if st, ok := rebaseRefusal(err); ok {
			return nil, st.Err()
		}
		h.logger.Error("PreviewRebase failed", "source_run_id", srcID, "error", err)
		return nil, status.Errorf(codes.Internal, "preview rebase: %v", err)
	}
	pv := got.Preview
	resp := &statev1.PreviewRebaseResponse{
		ReleaseId:       got.Release.ReleaseID,
		PromotionSeq:    got.Release.PromotionSeq,
		TotalNodes:      num.ClampInt32(pv.Total()),
		WillRun:         num.ClampInt32(pv.WillRun()),
		FailedOrSkipped: num.ClampInt32(pv.FailedOrSkipped),
		NewNodes:        num.ClampInt32(pv.New),
		ChangedUpstream: num.ClampInt32(pv.ChangedUpstream),
		Downstream:      num.ClampInt32(pv.Downstream),
		Inherited:       num.ClampInt32(pv.Inherited),
		ChangedNodes:    make([]*statev1.ChangedUpstreamNode, 0, len(got.Changed)),
	}
	for _, c := range got.Changed {
		resp.ChangedNodes = append(resp.ChangedNodes, &statev1.ChangedUpstreamNode{
			ServiceName:        c.ServiceName,
			SchemaName:         c.SchemaName,
			TableName:          c.TableName,
			Downstream:         num.ClampInt32(c.Downstream),
			LastSuccessSeconds: c.LastSuccess.Seconds(),
		})
	}
	return resp, nil
}
