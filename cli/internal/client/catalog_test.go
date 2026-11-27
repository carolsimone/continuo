package client

import (
	"context"
	"net"
	"testing"

	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// recordingCatalogServer answers the data contract reads and keeps the last request
// of each, so a test sees what the client put on the wire.
type recordingCatalogServer struct {
	catalogv1.UnimplementedCatalogQueryServer

	contractReq *catalogv1.GetNodeDataContractRequest
	versionsReq *catalogv1.ListNodeDataContractVersionsRequest
	diffReq     *catalogv1.GetNodeDataContractDiffRequest
}

func (s *recordingCatalogServer) GetNodeDataContract(_ context.Context, req *catalogv1.GetNodeDataContractRequest) (*catalogv1.GetNodeDataContractResponse, error) {
	s.contractReq = req
	return &catalogv1.GetNodeDataContractResponse{Contract: &catalogv1.ContractVersionView{UniqueId: req.GetUniqueId(), Version: 4}}, nil
}

func (s *recordingCatalogServer) ListNodeDataContractVersions(_ context.Context, req *catalogv1.ListNodeDataContractVersionsRequest) (*catalogv1.ListNodeDataContractVersionsResponse, error) {
	s.versionsReq = req
	return &catalogv1.ListNodeDataContractVersionsResponse{Versions: []*catalogv1.ContractVersionView{{UniqueId: req.GetUniqueId(), Version: 2}}}, nil
}

func (s *recordingCatalogServer) GetNodeDataContractDiff(_ context.Context, req *catalogv1.GetNodeDataContractDiffRequest) (*catalogv1.GetNodeDataContractDiffResponse, error) {
	s.diffReq = req
	return &catalogv1.GetNodeDataContractDiffResponse{Changes: []*catalogv1.ContractFieldChange{{Path: "description.purpose"}}}, nil
}

func dialRecordingCatalog(t *testing.T, srv *recordingCatalogServer) CatalogClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	catalogv1.RegisterCatalogQueryServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return &catalogGRPCClient{conn: conn, client: catalogv1.NewCatalogQueryClient(conn)}
}

func TestCatalogClient_ContractReadsPutEveryArgumentOnTheWire(t *testing.T) {
	srv := &recordingCatalogServer{}
	c := dialRecordingCatalog(t, srv)
	t.Cleanup(func() { _ = c.Close() })

	contract, err := c.GetNodeDataContract(context.Background(), "analytics.orders", true)
	require.NoError(t, err)
	assert.Equal(t, int64(4), contract.GetContract().GetVersion())
	assert.Equal(t, "analytics.orders", srv.contractReq.GetUniqueId())
	assert.True(t, srv.contractReq.GetIncludeDocument())

	versions, err := c.ListNodeDataContractVersions(context.Background(), "analytics.orders", 7)
	require.NoError(t, err)
	require.Len(t, versions.GetVersions(), 1)
	assert.Equal(t, "analytics.orders", srv.versionsReq.GetUniqueId())
	assert.Equal(t, int32(7), srv.versionsReq.GetLimit())

	diff, err := c.GetNodeDataContractDiff(context.Background(), "analytics.orders", 1, 3)
	require.NoError(t, err)
	require.Len(t, diff.GetChanges(), 1)
	assert.Equal(t, "analytics.orders", srv.diffReq.GetUniqueId())
	assert.Equal(t, int64(1), srv.diffReq.GetFromVersion())
	assert.Equal(t, int64(3), srv.diffReq.GetToVersion())
}
