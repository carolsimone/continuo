package grpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"

	grpcadapter "github.com/carolsimone/continuo/agent-remediation/adapters/grpc"
)

// contractCatalog answers GetNodeDataContract from a map, NOT_FOUND for a node it
// does not hold, and records the requests.
type contractCatalog struct {
	catalogv1.UnimplementedCatalogQueryServer
	docs     map[string]string
	err      error
	requests []*catalogv1.GetNodeDataContractRequest
}

func (c *contractCatalog) GetNodeDataContract(_ context.Context, req *catalogv1.GetNodeDataContractRequest) (*catalogv1.GetNodeDataContractResponse, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	doc, ok := c.docs[req.GetUniqueId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no contract")
	}
	return &catalogv1.GetNodeDataContractResponse{Contract: &catalogv1.ContractVersionView{
		UniqueId: req.GetUniqueId(), Version: 3, DocumentYaml: doc, IsCurrent: true,
	}}, nil
}

// serveCatalog serves srv on a local port and returns a GraphClient dialled
// to it.
func serveCatalog(t *testing.T, srv catalogv1.CatalogQueryServer) *grpcadapter.GraphClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	catalogv1.RegisterCatalogQueryServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	c, err := grpcadapter.NewGraphClient(lis.Addr().String())
	require.NoError(t, err)
	return c
}

func TestCurrentContract_ReturnsThePublishedDocument(t *testing.T) {
	cat := &contractCatalog{docs: map[string]string{"analytics.kpis": "id: analytics.kpis\nversion: \"3\"\n"}}
	c := serveCatalog(t, cat)

	got, err := c.CurrentContract(context.Background(), "analytics.kpis")

	require.NoError(t, err)
	assert.Equal(t, "id: analytics.kpis\nversion: \"3\"\n", got)
	require.Len(t, cat.requests, 1)
	assert.Equal(t, "analytics.kpis", cat.requests[0].GetUniqueId())
	assert.True(t, cat.requests[0].GetIncludeDocument(), "the document is asked for")
}

func TestCurrentContract_ANodeWithNoContractIsEmpty(t *testing.T) {
	got, err := serveCatalog(t, &contractCatalog{}).CurrentContract(context.Background(), "analytics.kpis")

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCurrentContract_AnyOtherErrorIsReturned(t *testing.T) {
	_, err := serveCatalog(t, &contractCatalog{err: status.Error(codes.Unavailable, "catalog down")}).
		CurrentContract(context.Background(), "analytics.kpis")

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}
