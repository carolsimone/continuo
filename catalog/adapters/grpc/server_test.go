package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	grpcadapter "github.com/carolsimone/continuo/catalog/adapters/grpc"
	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestServer_RoutesEveryCatalogQueryRPC guards the failure mode where
// Server embeds UnimplementedCatalogQueryServer and a new RPC is added
// to the proto without a matching delegate on *Server — the binary then
// silently returns codes.Unimplemented at runtime, which only e2e catches.
//
// The test brings the gRPC server up on an ephemeral port, connects a real
// client, and invokes every RPC. Any RPC missing a direct delegate would
// return codes.Unimplemented from the embedded UnimplementedCatalogQueryServer.
func TestServer_RoutesEveryCatalogQueryRPC(t *testing.T) {
	client, _ := startTestServer(t)

	// Each call below would return codes.Unimplemented if its delegate were
	// missing on *Server. We don't care about response payloads here — the
	// stub returns empty values — only that the call dispatches to our handler.
	for _, c := range everyRPC(client) {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			if err == nil {
				return // dispatched and stub succeeded — fine
			}
			st, _ := status.FromError(err)
			assert.NotEqualf(t, codes.Unimplemented, st.Code(),
				"%s returned Unimplemented — Server is missing a delegate for it",
				c.name)
		})
	}
}

// The server's identity interceptor places the default tenant on every
// request, so every RPC reads for it.
func TestServer_EveryRPCReadsForTheDefaultTenant(t *testing.T) {
	client, log := startTestServer(t)
	cases := everyRPC(client)
	for _, c := range cases {
		require.NoError(t, c.call(), c.name)
	}
	got := log.all()
	require.Len(t, got, len(cases), "one read per RPC")
	for _, tenantID := range got {
		assert.Equal(t, pkgevents.DefaultTenantID, tenantID)
	}
}

// startTestServer brings the gRPC server up on an ephemeral port over stub
// readers, and returns a client connected to it and the log of the tenants
// the readers were called for.
func startTestServer(t *testing.T) (catalogv1.CatalogQueryClient, *tenantLog) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	log := &tenantLog{}
	handler := grpcadapter.NewQueryHandler(stubNodeReader{log}, stubCodeVersionHistoryReader{log}, stubPrecedentHistoryReader{log}, logger)
	server, err := grpcadapter.NewServer(0, handler, logger) // 0 = ephemeral port
	require.NoError(t, err)
	go func() { _ = server.Start() }()
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(
		server.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return catalogv1.NewCatalogQueryClient(conn), log
}

// everyRPC is one call of every CatalogQuery RPC with a valid request.
func everyRPC(client catalogv1.CatalogQueryClient) []struct {
	name string
	call func() error
} {
	ctx := context.Background()
	return []struct {
		name string
		call func() error
	}{
		{"GetNode", func() error {
			_, err := client.GetNode(ctx, &catalogv1.GetNodeRequest{ServiceName: "svc", SchemaName: "an", TableName: "fct"})
			return err
		}},
		{"GetNodeLocation", func() error {
			_, err := client.GetNodeLocation(ctx, &catalogv1.GetNodeLocationRequest{UniqueId: "x"})
			return err
		}},
		{"GetNodeVersions", func() error {
			_, err := client.GetNodeVersions(ctx, &catalogv1.GetNodeVersionsRequest{UniqueId: "x"})
			return err
		}},
		{"GetNodeVersionDiff", func() error {
			_, err := client.GetNodeVersionDiff(ctx, &catalogv1.GetNodeVersionDiffRequest{UniqueId: "x", FromSeq: 1, ToSeq: 2})
			return err
		}},
		{"GetUpstreamChanges", func() error {
			_, err := client.GetUpstreamChanges(ctx, &catalogv1.GetUpstreamChangesRequest{UniqueId: "x"})
			return err
		}},
		{"GetCodeUnitVersions", func() error {
			_, err := client.GetCodeUnitVersions(ctx, &catalogv1.GetCodeUnitVersionsRequest{UnitId: "x"})
			return err
		}},
		{"GetPrecedents", func() error {
			_, err := client.GetPrecedents(ctx, &catalogv1.GetPrecedentsRequest{Signature: "x"})
			return err
		}},
	}
}

// tenantLog records the tenant of every read the stubs serve.
type tenantLog struct {
	mu      sync.Mutex
	tenants []string
}

func (l *tenantLog) add(tenantID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tenants = append(l.tenants, tenantID)
}

func (l *tenantLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.tenants)
}

// stubNodeReader satisfies grpcadapter.NodeReader.
type stubNodeReader struct{ log *tenantLog }

func (s stubNodeReader) GetNode(_ context.Context, tenantID, _, _, _ string, _ domain.NodeScope) (*domain.NodeMeta, error) {
	s.log.add(tenantID)
	return &domain.NodeMeta{}, nil
}
func (s stubNodeReader) GetNodeLocation(_ context.Context, tenantID, _ string) (*domain.NodeLocation, error) {
	s.log.add(tenantID)
	return &domain.NodeLocation{}, nil
}

// stubCodeVersionHistoryReader satisfies grpcadapter.CodeVersionHistoryReader.
type stubCodeVersionHistoryReader struct{ log *tenantLog }

func (s stubCodeVersionHistoryReader) GetNodeVersions(_ context.Context, tenantID, _ string, _ int32, _ bool) ([]codeversion.VersionView, error) {
	s.log.add(tenantID)
	return nil, nil
}
func (s stubCodeVersionHistoryReader) GetCurrentNodeVersion(_ context.Context, tenantID, _ string, _ bool) ([]codeversion.VersionView, error) {
	s.log.add(tenantID)
	return nil, nil
}
func (s stubCodeVersionHistoryReader) GetNodeVersionDiff(_ context.Context, tenantID, _ string, _, _ int64) (*codeversion.VersionDiff, error) {
	s.log.add(tenantID)
	return &codeversion.VersionDiff{}, nil
}
func (s stubCodeVersionHistoryReader) GetUpstreamChanges(_ context.Context, tenantID, _ string, _ int32, _ time.Time) ([]codeversion.UpstreamChange, error) {
	s.log.add(tenantID)
	return nil, nil
}
func (s stubCodeVersionHistoryReader) GetCodeUnitVersions(_ context.Context, tenantID, _, _ string, _ int32) ([]codeversion.UnitVersionView, error) {
	s.log.add(tenantID)
	return nil, nil
}

// stubPrecedentHistoryReader satisfies grpcadapter.PrecedentHistoryReader.
type stubPrecedentHistoryReader struct{ log *tenantLog }

func (s stubPrecedentHistoryReader) GetPrecedents(_ context.Context, tenantID, _, _, _ string, _ int32, _ bool) ([]casebase.Precedent, error) {
	s.log.add(tenantID)
	return nil, nil
}
