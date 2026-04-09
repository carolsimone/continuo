package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/graph/adapters/neo4j"
	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	grpcserver "github.com/carolsimone/continuo/graph/internal/grpc"
	"github.com/carolsimone/continuo/graph/internal/grpc/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcClient     graphv1.GraphServiceClient
	grpcConn       *grpc.ClientConn
	server         *grpcserver.Server
	neo4jContainer testcontainers.Container
)

// TestMain sets up the test environment
func TestMain(m *testing.M) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Start Neo4j container
	req := testcontainers.ContainerRequest{
		Image:        "neo4j:5.15.0-community",
		ExposedPorts: []string{"7687/tcp"},
		Env: map[string]string{
			"NEO4J_AUTH":                           "neo4j/testpassword",
			"NEO4J_ACCEPT_LICENSE_AGREEMENT":       "yes",
			"NEO4J_dbms_memory_pagecache_size":     "256M",
			"NEO4J_dbms_memory_heap_initial__size": "256M",
			"NEO4J_dbms_memory_heap_max__size":     "512M",
		},
		WaitingFor: wait.ForLog("Started.").WithStartupTimeout(60 * time.Second),
	}

	neo4jC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		logger.Error("Failed to start Neo4j container", "error", err)
		os.Exit(1)
	}
	neo4jContainer = neo4jC

	// Get Neo4j connection details
	host, err := neo4jContainer.Host(ctx)
	if err != nil {
		logger.Error("Failed to get Neo4j host", "error", err)
		os.Exit(1)
	}

	port, err := neo4jContainer.MappedPort(ctx, "7687")
	if err != nil {
		logger.Error("Failed to get Neo4j port", "error", err)
		os.Exit(1)
	}

	neo4jURI := "bolt://" + host + ":" + port.Port()
	logger.Info("Neo4j container started", "uri", neo4jURI)

	// Wait a bit for Neo4j to be fully ready
	time.Sleep(5 * time.Second)

	// Create Neo4j client
	neo4jClient, err := neo4j.NewNeo4jClient(neo4jURI, "neo4j", "testpassword", logger)
	if err != nil {
		logger.Error("Failed to create Neo4j client", "error", err)
		os.Exit(1)
	}

	// Create repository and handler
	graphRepo := neo4j.NewGraphRepository(neo4jClient, logger)
	graphHandler := handlers.NewGraphHandler(graphRepo, logger)

	// Create gRPC server
	server, err = grpcserver.NewServer(0, graphHandler, logger) // Port 0 = random available port
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	// Start gRPC server in background
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Give server time to start
	time.Sleep(1 * time.Second)

	// Create gRPC client connection
	serverAddr := server.Addr()
	logger.Info("Connecting to gRPC server", "addr", serverAddr)
	grpcConn, err = grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("Failed to create gRPC client", "error", err)
		os.Exit(1)
	}

	grpcClient = graphv1.NewGraphServiceClient(grpcConn)

	// Run tests
	code := m.Run()

	// Cleanup
	grpcConn.Close()
	server.Shutdown(ctx)
	neo4jClient.Close(ctx)
	neo4jContainer.Terminate(ctx)

	os.Exit(code)
}

func snapshotScheduleGraph(t *testing.T, ctx context.Context, runID, scheduleName string) {
	t.Helper()

	_, err := grpcClient.SnapshotGraph(ctx, &graphv1.SnapshotGraphRequest{
		RunId:        runID,
		ScheduleName: scheduleName,
	})
	require.NoError(t, err)
}

func TestCreateNode(t *testing.T) {
	ctx := context.Background()

	// Test creating a simple node without dependencies
	t.Run("CreateSimpleNode", func(t *testing.T) {
		req := &graphv1.CreateNodeRequest{
			TableName:    "users",
			SchemaName:   "public",
			ServiceName:  "user_service",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		}

		resp, err := grpcClient.CreateNode(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Node)

		assert.Equal(t, "users", resp.Node.TableName)
		assert.Equal(t, "public", resp.Node.SchemaName)
		assert.Equal(t, "user_service", resp.Node.ServiceName)
		assert.Equal(t, "data_team", resp.Node.Owner)
		assert.Equal(t, "daily", resp.Node.ScheduleName)
		assert.Equal(t, graphv1.Criticality_CRITICALITY_CORE, resp.Node.Criticality)
		assert.NotNil(t, resp.Node.CreatedAt)
		assert.NotNil(t, resp.Node.LastUpdatedAt)
	})

	// Test creating a node with upstream dependencies
	t.Run("CreateNodeWithDependencies", func(t *testing.T) {
		req := &graphv1.CreateNodeRequest{
			TableName:    "orders",
			SchemaName:   "public",
			ServiceName:  "order_service",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_REGULATORY,
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{
					TableName:   "users",
					SchemaName:  "public",
					ServiceName: "user_service",
				},
			},
		}

		resp, err := grpcClient.CreateNode(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Node)

		assert.Equal(t, "orders", resp.Node.TableName)
		assert.Equal(t, graphv1.Criticality_CRITICALITY_REGULATORY, resp.Node.Criticality)
	})

	// Test validation - missing required fields
	t.Run("CreateNodeValidationError", func(t *testing.T) {
		req := &graphv1.CreateNodeRequest{
			TableName:  "invalid",
			SchemaName: "public",
			// Missing required fields
		}

		resp, err := grpcClient.CreateNode(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	// Test updating existing node
	t.Run("UpdateExistingNode", func(t *testing.T) {
		req := &graphv1.CreateNodeRequest{
			TableName:    "users",
			SchemaName:   "public",
			ServiceName:  "user_service_v2",
			Owner:        "platform_team",
			ScheduleName: "hourly",
			Criticality:  graphv1.Criticality_CRITICALITY_SECONDARY,
		}

		resp, err := grpcClient.CreateNode(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, "user_service_v2", resp.Node.ServiceName)
		assert.Equal(t, "platform_team", resp.Node.Owner)
		assert.Equal(t, "hourly", resp.Node.ScheduleName)
	})
}

func TestUpdateNodeTimestamp(t *testing.T) {
	ctx := context.Background()

	// First create a node
	createReq := &graphv1.CreateNodeRequest{
		TableName:    "products",
		SchemaName:   "public",
		ServiceName:  "product_service",
		Owner:        "data_team",
		ScheduleName: "daily",
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
	}
	createResp, err := grpcClient.CreateNode(ctx, createReq)
	require.NoError(t, err)
	originalTimestamp := createResp.Node.LastUpdatedAt.AsTime()

	// Wait a moment to ensure timestamp will be different
	// Neo4j datetime() has millisecond precision, so we need to wait a bit
	time.Sleep(100 * time.Millisecond)

	// Update timestamp
	t.Run("UpdateTimestamp", func(t *testing.T) {
		req := &graphv1.UpdateNodeTimestampRequest{
			TableName:  "products",
			SchemaName: "public",
		}

		resp, err := grpcClient.UpdateNodeTimestamp(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Node)

		assert.Equal(t, "products", resp.Node.TableName)
		// Verify timestamp is updated (should be >= original, allowing for millisecond precision)
		assert.True(t, resp.Node.LastUpdatedAt.AsTime().After(originalTimestamp) ||
			resp.Node.LastUpdatedAt.AsTime().Equal(originalTimestamp),
			"Updated timestamp should be >= original timestamp")
		// Verify other fields remain unchanged
		assert.Equal(t, "product_service", resp.Node.ServiceName)
		assert.Equal(t, "data_team", resp.Node.Owner)
	})

	// Test updating non-existent node
	t.Run("UpdateNonExistentNode", func(t *testing.T) {
		req := &graphv1.UpdateNodeTimestampRequest{
			TableName:  "nonexistent",
			SchemaName: "public",
		}

		resp, err := grpcClient.UpdateNodeTimestamp(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestGetStaleRootNodes(t *testing.T) {
	ctx := context.Background()

	// Create some root nodes (no upstream dependencies)
	t.Run("GetStaleRootNodes", func(t *testing.T) {
		// Create a fresh root node
		freshReq := &graphv1.CreateNodeRequest{
			TableName:    "fresh_root",
			SchemaName:   "public",
			ServiceName:  "test_service",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		}
		_, err := grpcClient.CreateNode(ctx, freshReq)
		require.NoError(t, err)

		// Get stale root nodes (should be empty with threshold of 1 hour)
		req := &graphv1.GetStaleRootNodesRequest{
			HoursThreshold: 1,
		}

		resp, err := grpcClient.GetStaleRootNodes(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// The fresh node should not be in the stale list
		assert.Equal(t, 0, len(resp.Nodes))
	})

	// Test validation
	t.Run("InvalidThreshold", func(t *testing.T) {
		req := &graphv1.GetStaleRootNodesRequest{
			HoursThreshold: 0,
		}

		resp, err := grpcClient.GetStaleRootNodes(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestGetDownstreamDependencies(t *testing.T) {
	ctx := context.Background()

	// Create a dependency chain: source -> intermediate -> downstream
	t.Run("GetDownstreamDependencies", func(t *testing.T) {
		// Create source node
		sourceReq := &graphv1.CreateNodeRequest{
			TableName:    "raw_events",
			SchemaName:   "staging",
			ServiceName:  "ingestion",
			Owner:        "data_team",
			ScheduleName: "hourly",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		}
		_, err := grpcClient.CreateNode(ctx, sourceReq)
		require.NoError(t, err)

		// Create intermediate node that depends on source
		intermediateReq := &graphv1.CreateNodeRequest{
			TableName:    "processed_events",
			SchemaName:   "analytics",
			ServiceName:  "processing",
			Owner:        "data_team",
			ScheduleName: "hourly",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{
					TableName:   "raw_events",
					SchemaName:  "staging",
					ServiceName: "ingestion",
				},
			},
		}
		_, err = grpcClient.CreateNode(ctx, intermediateReq)
		require.NoError(t, err)

		// Create downstream node that depends on intermediate
		downstreamReq := &graphv1.CreateNodeRequest{
			TableName:    "event_summary",
			SchemaName:   "analytics",
			ServiceName:  "reporting",
			Owner:        "data_team",
			ScheduleName: "hourly",
			Criticality:  graphv1.Criticality_CRITICALITY_SECONDARY,
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{
					TableName:   "processed_events",
					SchemaName:  "analytics",
					ServiceName: "processing",
				},
			},
		}
		_, err = grpcClient.CreateNode(ctx, downstreamReq)
		require.NoError(t, err)

		// Get downstream dependencies of source
		req := &graphv1.GetDownstreamDependenciesRequest{
			ScheduleName: "hourly",
			SchemaName:   "staging",
			TableName:    "raw_events",
			MaxDepth:     0, // No limit
		}

		resp, err := grpcClient.GetDownstreamDependencies(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have 2 downstream dependencies
		assert.Equal(t, int32(2), resp.TotalCount)
		assert.Len(t, resp.Dependencies, 2)

		// Verify dependencies are returned in order of depth
		assert.Equal(t, int32(1), resp.Dependencies[0].Depth)
		assert.Equal(t, "processed_events", resp.Dependencies[0].Node.TableName)
		assert.Equal(t, int32(2), resp.Dependencies[1].Depth)
		assert.Equal(t, "event_summary", resp.Dependencies[1].Node.TableName)
	})

	// Test with max depth limit
	t.Run("GetDownstreamDependenciesWithMaxDepth", func(t *testing.T) {
		req := &graphv1.GetDownstreamDependenciesRequest{
			ScheduleName: "hourly",
			SchemaName:   "staging",
			TableName:    "raw_events",
			MaxDepth:     1, // Only immediate children
		}

		resp, err := grpcClient.GetDownstreamDependencies(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should only have 1 downstream dependency (immediate child)
		assert.Equal(t, int32(1), resp.TotalCount)
		assert.Len(t, resp.Dependencies, 1)
		assert.Equal(t, "processed_events", resp.Dependencies[0].Node.TableName)
	})

	// Test validation
	t.Run("MissingRequiredFields", func(t *testing.T) {
		req := &graphv1.GetDownstreamDependenciesRequest{
			ScheduleName: "hourly",
			// Missing schema and table
		}

		resp, err := grpcClient.GetDownstreamDependencies(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestGetScheduleGraph(t *testing.T) {
	ctx := context.Background()

	t.Run("GetScheduleGraph", func(t *testing.T) {
		// Create schedule-owned nodes
		_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
			TableName:    "orders",
			SchemaName:   "sales",
			ServiceName:  "svc",
			Owner:        "team",
			ScheduleName: "test_schedule_graph_unique",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			NodeType:     "dbt-model",
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{TableName: "raw_orders", SchemaName: "seeds", ServiceName: "svc"},
			},
		})
		require.NoError(t, err)

		// Create cross-boundary seed node (no schedule_name owned by this schedule)
		_, err = grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
			TableName:    "raw_orders",
			SchemaName:   "seeds",
			ServiceName:  "svc",
			Owner:        "team",
			ScheduleName: "test_schedule_graph_seed",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			NodeType:     "dbt-seed",
		})
		require.NoError(t, err)

		resp, err := grpcClient.GetScheduleGraph(ctx, &graphv1.GetScheduleGraphRequest{
			ScheduleName: "test_schedule_graph_unique",
		})
		require.NoError(t, err)

		// Should have 2 nodes: 1 schedule-owned + 1 cross-boundary seed
		assert.Len(t, resp.Nodes, 2)

		nodeIDs := make(map[string]bool)
		for _, n := range resp.Nodes {
			nodeIDs[n.ServiceName+"."+n.SchemaName+"."+n.TableName] = true
		}
		assert.True(t, nodeIDs["svc.sales.orders"])
		assert.True(t, nodeIDs["svc.seeds.raw_orders"])

		// Should have 1 edge: orders -> raw_orders
		require.Len(t, resp.Edges, 1)
		assert.Equal(t, "svc.sales.orders", resp.Edges[0].FromNodeId)
		assert.Equal(t, "svc.seeds.raw_orders", resp.Edges[0].ToNodeId)
	})
}

func TestCheckUpstreamFreshness(t *testing.T) {
	ctx := context.Background()

	t.Run("CheckUpstreamFreshness", func(t *testing.T) {
		// Create upstream nodes
		upstream1Req := &graphv1.CreateNodeRequest{
			TableName:    "customer_data",
			SchemaName:   "source",
			ServiceName:  "ingestion",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_REGULATORY,
		}
		_, err := grpcClient.CreateNode(ctx, upstream1Req)
		require.NoError(t, err)

		upstream2Req := &graphv1.CreateNodeRequest{
			TableName:    "transaction_data",
			SchemaName:   "source",
			ServiceName:  "ingestion",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_REGULATORY,
		}
		_, err = grpcClient.CreateNode(ctx, upstream2Req)
		require.NoError(t, err)

		// Create downstream node
		downstreamReq := &graphv1.CreateNodeRequest{
			TableName:    "customer_transactions",
			SchemaName:   "analytics",
			ServiceName:  "processing",
			Owner:        "data_team",
			ScheduleName: "daily",
			Criticality:  graphv1.Criticality_CRITICALITY_CORE,
			UpstreamDependencies: []*graphv1.UpstreamDependency{
				{
					TableName:   "customer_data",
					SchemaName:  "source",
					ServiceName: "ingestion",
				},
				{
					TableName:   "transaction_data",
					SchemaName:  "source",
					ServiceName: "ingestion",
				},
			},
		}
		_, err = grpcClient.CreateNode(ctx, downstreamReq)
		require.NoError(t, err)

		// Check freshness (all should be fresh since just created)
		req := &graphv1.CheckUpstreamFreshnessRequest{
			SchemaName:       "analytics",
			TableName:        "customer_transactions",
			FreshnessMinutes: 30,
		}

		resp, err := grpcClient.CheckUpstreamFreshness(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// All upstreams should be fresh
		assert.True(t, resp.AllFresh)
		assert.Len(t, resp.StaleUpstreams, 0)
	})

	// Test with custom freshness threshold
	t.Run("CheckWithCustomThreshold", func(t *testing.T) {
		req := &graphv1.CheckUpstreamFreshnessRequest{
			SchemaName:       "analytics",
			TableName:        "customer_transactions",
			FreshnessMinutes: 1, // Very strict threshold
		}

		resp, err := grpcClient.CheckUpstreamFreshness(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// With such a strict threshold, might have stale upstreams
		// But since we just created them, they should still be fresh
		assert.True(t, resp.AllFresh)
	})

	// Test validation
	t.Run("MissingRequiredFields", func(t *testing.T) {
		req := &graphv1.CheckUpstreamFreshnessRequest{
			SchemaName: "analytics",
			// Missing table name
		}

		resp, err := grpcClient.CheckUpstreamFreshness(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUpdateNodeStatus(t *testing.T) {
	ctx := context.Background()
	sched := "sched-update-status-" + t.Name()
	runID := "run-" + t.Name()

	// Create a node to update
	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "orders",
		SchemaName:   "public",
		ServiceName:  "dbt",
		Owner:        "team",
		ScheduleName: sched,
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, sched)

	// UpdateNodeStatus should succeed
	_, err = grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: sched,
		SchemaName:   "public",
		TableName:    "orders",
		Status:       "SUCCEEDED",
		RunId:        runID,
	})
	require.NoError(t, err)
}

func TestUpdateNodeStatus_RequiredFields(t *testing.T) {
	ctx := context.Background()

	_, err := grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{})
	require.Error(t, err)
}

func TestGetReadyDownstream_ReturnsReadyNode(t *testing.T) {
	ctx := context.Background()
	sched := "sched-ready-downstream-" + t.Name()
	runID := "run-" + t.Name()

	// Create upstream node
	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "upstream",
		SchemaName:   "public",
		ServiceName:  "dbt",
		Owner:        "team",
		ScheduleName: sched,
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
	})
	require.NoError(t, err)

	// Create downstream node that depends on upstream
	_, err = grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "downstream",
		SchemaName:   "public",
		ServiceName:  "dbt",
		Owner:        "team",
		ScheduleName: sched,
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
		UpstreamDependencies: []*graphv1.UpstreamDependency{
			{TableName: "upstream", SchemaName: "public", ServiceName: "dbt"},
		},
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, sched)

	// Upstream not yet succeeded — downstream should not be ready
	resp, err := grpcClient.GetReadyDownstream(ctx, &graphv1.GetReadyDownstreamRequest{
		ScheduleName: sched,
		SchemaName:   "public",
		TableName:    "upstream",
		RunId:        runID,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Nodes, "downstream should not be ready before upstream succeeds")

	// Mark upstream as SUCCEEDED
	_, err = grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: sched,
		SchemaName:   "public",
		TableName:    "upstream",
		Status:       "SUCCEEDED",
		RunId:        runID,
	})
	require.NoError(t, err)

	// Now downstream should be ready
	resp, err = grpcClient.GetReadyDownstream(ctx, &graphv1.GetReadyDownstreamRequest{
		ScheduleName: sched,
		SchemaName:   "public",
		TableName:    "upstream",
		RunId:        runID,
	})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 1)
	assert.Equal(t, "downstream", resp.Nodes[0].TableName)
	assert.Equal(t, "public", resp.Nodes[0].SchemaName)
	assert.Equal(t, "dbt", resp.Nodes[0].ServiceName)
	assert.Equal(t, "dbt-model", resp.Nodes[0].NodeType)
}

func TestGetReadyDownstream_RequiredFields(t *testing.T) {
	ctx := context.Background()

	_, err := grpcClient.GetReadyDownstream(ctx, &graphv1.GetReadyDownstreamRequest{})
	require.Error(t, err)
}

func TestCheckScheduleCompletion_AllSucceeded(t *testing.T) {
	ctx := context.Background()
	sched := "sched-completion-succeeded-" + t.Name()
	runID := "run-" + t.Name()

	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "node_a",
		SchemaName:   "public",
		ServiceName:  "dbt",
		Owner:        "team",
		ScheduleName: sched,
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, sched)

	_, err = grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: sched, SchemaName: "public", TableName: "node_a", Status: "SUCCEEDED", RunId: runID,
	})
	require.NoError(t, err)

	resp, err := grpcClient.CheckScheduleCompletion(ctx, &graphv1.CheckScheduleCompletionRequest{
		ScheduleName: sched,
		RunId:        runID,
	})
	require.NoError(t, err)
	assert.True(t, resp.IsComplete)
	assert.False(t, resp.HasFailed)
}

func TestCheckScheduleCompletion_HasFailed(t *testing.T) {
	ctx := context.Background()
	sched := "sched-completion-failed-" + t.Name()
	runID := "run-" + t.Name()

	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName: "node_a", SchemaName: "public", ServiceName: "dbt",
		Owner: "team", ScheduleName: sched, Criticality: graphv1.Criticality_CRITICALITY_CORE,
		NodeType: "dbt-model",
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, sched)

	_, err = grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: sched, SchemaName: "public", TableName: "node_a", Status: "FAILED", RunId: runID,
	})
	require.NoError(t, err)

	resp, err := grpcClient.CheckScheduleCompletion(ctx, &graphv1.CheckScheduleCompletionRequest{
		ScheduleName: sched,
		RunId:        runID,
	})
	require.NoError(t, err)
	assert.True(t, resp.IsComplete)
	assert.True(t, resp.HasFailed)
}

func TestCheckScheduleCompletion_StillRunning(t *testing.T) {
	ctx := context.Background()
	sched := "sched-completion-running-" + t.Name()
	runID := "run-" + t.Name()

	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName: "node_a", SchemaName: "public", ServiceName: "dbt",
		Owner: "team", ScheduleName: sched, Criticality: graphv1.Criticality_CRITICALITY_CORE,
		NodeType: "dbt-model",
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, sched)

	_, err = grpcClient.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: sched, SchemaName: "public", TableName: "node_a", Status: "RUNNING", RunId: runID,
	})
	require.NoError(t, err)

	resp, err := grpcClient.CheckScheduleCompletion(ctx, &graphv1.CheckScheduleCompletionRequest{
		ScheduleName: sched,
		RunId:        runID,
	})
	require.NoError(t, err)
	assert.False(t, resp.IsComplete)
}

func TestCheckScheduleCompletion_RequiredFields(t *testing.T) {
	ctx := context.Background()

	_, err := grpcClient.CheckScheduleCompletion(ctx, &graphv1.CheckScheduleCompletionRequest{})
	require.Error(t, err)
}

func TestGetScheduleInitNodes(t *testing.T) {
	ctx := context.Background()
	runID := "run-" + t.Name()

	// Seed: cross-schedule dbt-seed upstream
	_, err := grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "seed_1",
		SchemaName:   "seeds_schema",
		ServiceName:  "seed_service",
		Owner:        "data_team",
		ScheduleName: "seeds-schedule",
		Criticality:  graphv1.Criticality_CRITICALITY_SECONDARY,
		NodeType:     "dbt-seed",
	})
	require.NoError(t, err)

	// model_a: root node (no owned upstream)
	_, err = grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "model_a",
		SchemaName:   "test_schema",
		ServiceName:  "test_service",
		Owner:        "data_team",
		ScheduleName: "test-schedule",
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
	})
	require.NoError(t, err)

	// model_b: depends on model_a — NOT a root node
	_, err = grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "model_b",
		SchemaName:   "test_schema",
		ServiceName:  "test_service",
		Owner:        "data_team",
		ScheduleName: "test-schedule",
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
		UpstreamDependencies: []*graphv1.UpstreamDependency{
			{TableName: "model_a", SchemaName: "test_schema", ServiceName: "test_service"},
		},
	})
	require.NoError(t, err)

	// model_c: depends on seed_1 (cross-schedule) — IS a root node (no owned upstream)
	_, err = grpcClient.CreateNode(ctx, &graphv1.CreateNodeRequest{
		TableName:    "model_c",
		SchemaName:   "test_schema",
		ServiceName:  "test_service",
		Owner:        "data_team",
		ScheduleName: "test-schedule",
		Criticality:  graphv1.Criticality_CRITICALITY_CORE,
		NodeType:     "dbt-model",
		UpstreamDependencies: []*graphv1.UpstreamDependency{
			{TableName: "seed_1", SchemaName: "seeds_schema", ServiceName: "seed_service"},
		},
	})
	require.NoError(t, err)

	snapshotScheduleGraph(t, ctx, runID, "test-schedule")

	// Call the new RPC
	resp, err := grpcClient.GetScheduleInitNodes(ctx, &graphv1.GetScheduleInitNodesRequest{
		ScheduleName: "test-schedule",
		RunId:        runID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.AllNodes, 4, "all_nodes: 3 owned + 1 seed")
	assert.Len(t, resp.RootNodes, 2, "root_nodes: model_a and model_c")
	assert.Len(t, resp.SeedNodes, 1, "seed_nodes: seed_1")

	// Verify seed node content
	require.Len(t, resp.SeedNodes, 1)
	assert.Equal(t, "seed_1", resp.SeedNodes[0].TableName)
	assert.Equal(t, "dbt-seed", resp.SeedNodes[0].NodeType)

	// Validation: empty schedule_name returns error
	_, err = grpcClient.GetScheduleInitNodes(ctx, &graphv1.GetScheduleInitNodesRequest{
		ScheduleName: "",
	})
	assert.Error(t, err)
}
