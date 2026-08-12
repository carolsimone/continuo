package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type stubNodeReader struct {
	ScheduleAndRunListReader // embed so unused methods are nil; only GetNode is called
	meta                     *domain.NodeMeta
	err                      error
}

func (s stubNodeReader) GetNode(_ context.Context, _, _, _ string) (*domain.NodeMeta, error) {
	return s.meta, s.err
}

func TestGetNode_MapsMeta(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{meta: &domain.NodeMeta{NodeType: "dbt-model", TestCount: 0, TestCountKnown: true}}, nil, nil, testLogger())
	resp, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "fct"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.NodeType != "dbt-model" || resp.TestCount != 0 || !resp.TestCountKnown {
		t.Fatalf("unexpected resp: %+v", resp)
	}
}

func TestGetNode_NotFound(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{err: domain.ErrNodeNotFound}, nil, nil, testLogger())
	_, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetNode_MissingArgs(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{}, nil, nil, testLogger())
	_, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "", SchemaName: "an", TableName: "fct"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}
