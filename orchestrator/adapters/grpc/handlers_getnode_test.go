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
	gotScope                 *domain.NodeScope
}

func (s stubNodeReader) GetNode(_ context.Context, _, _, _ string, scope domain.NodeScope) (*domain.NodeMeta, error) {
	if s.gotScope != nil {
		*s.gotScope = scope
	}
	return s.meta, s.err
}

func TestGetNode_MapsMeta(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{meta: &domain.NodeMeta{NodeType: "dbt-model", TestCount: 0, TestCountKnown: true}}, nil, nil, nil, testLogger())
	resp, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "fct"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.NodeType != "dbt-model" || resp.TestCount != 0 || !resp.TestCountKnown || resp.Inactive {
		t.Fatalf("unexpected resp: %+v", resp)
	}
}

func TestGetNode_IncludeInactiveMapsToScope(t *testing.T) {
	for includeInactive, want := range map[bool]domain.NodeScope{
		false: domain.ActiveNodesOnly,
		true:  domain.IncludeInactiveNodes,
	} {
		got := domain.NodeScope(-1)
		h := NewQueryHandler(stubNodeReader{meta: &domain.NodeMeta{NodeType: "dbt-model"}, gotScope: &got}, nil, nil, nil, testLogger())
		if _, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "fct", IncludeInactive: includeInactive}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != want {
			t.Fatalf("include_inactive=%v reached the reader as scope %v, want %v", includeInactive, got, want)
		}
	}
}

func TestGetNode_MapsInactive(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{meta: &domain.NodeMeta{NodeType: "dbt-seed", Inactive: true}}, nil, nil, nil, testLogger())
	resp, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "seed", IncludeInactive: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.NodeType != "dbt-seed" || !resp.Inactive {
		t.Fatalf("an inactive node must report inactive=true with its node_type: %+v", resp)
	}
}

func TestGetNode_NotFound(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{err: domain.ErrNodeNotFound}, nil, nil, nil, testLogger())
	_, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetNode_MissingArgs(t *testing.T) {
	h := NewQueryHandler(stubNodeReader{}, nil, nil, nil, testLogger())
	_, err := h.GetNode(context.Background(), &orchestratorv1.GetNodeRequest{ServiceName: "", SchemaName: "an", TableName: "fct"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}
