package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testClients holds all client connections
type testClients struct {
	graphClient  graphv1.GraphServiceClient
	stateClient  statev1.StateServiceClient
	redisClient  *goredis.Client
	neo4jDriver  neo4jdriver.DriverWithContext
	startupDB    *sqlx.DB
	executorDB   *sqlx.DB
	dependencyDB *sqlx.DB
	k8sDB        *sqlx.DB
	stateDB      *sqlx.DB
	logger       *slog.Logger
}

// setupClients initializes all client connections
func setupClients(t *testing.T, ctx context.Context) *testClients {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Get hosts from environment (use service names for docker-compose)
	graphHost := getEnv("GRAPH_HOST", "graph")
	stateHost := getEnv("STATE_HOST", "state")
	redisHost := getEnv("REDIS_HOST", "redis")
	neo4jHost := getEnv("NEO4J_HOST", "neo4j")
	pgHost := getEnv("POSTGRES_HOST", "postgres")

	// Setup Graph gRPC client
	graphConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50052", graphHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to graph service")

	// Setup State gRPC client
	stateConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50051", stateHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to state service")

	// Setup Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: fmt.Sprintf("%s:6379", redisHost),
		DB:   0,
	})
	require.NoError(t, redisClient.Ping(ctx).Err(), "Failed to connect to Redis")

	// Setup Neo4j driver
	neo4jDriver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://%s:7687", neo4jHost),
		neo4jdriver.BasicAuth("neo4j", "atlas_password", ""),
	)
	require.NoError(t, err, "Failed to connect to Neo4j")

	// Setup PostgreSQL connections for each database
	startupDB := connectPostgres(t, pgHost, "continuo_startup")
	executorDB := connectPostgres(t, pgHost, "continuo_executor")
	dependencyDB := connectPostgres(t, pgHost, "continuo_dependency")
	k8sDB := connectPostgres(t, pgHost, "continuo_k8s")
	stateDB := connectPostgres(t, pgHost, "continuo_state")

	return &testClients{
		graphClient:  graphv1.NewGraphServiceClient(graphConn),
		stateClient:  statev1.NewStateServiceClient(stateConn),
		redisClient:  redisClient,
		neo4jDriver:  neo4jDriver,
		startupDB:    startupDB,
		executorDB:   executorDB,
		dependencyDB: dependencyDB,
		k8sDB:        k8sDB,
		stateDB:      stateDB,
		logger:       logger,
	}
}

// connectPostgres establishes a PostgreSQL connection
func connectPostgres(t *testing.T, host, database string) *sqlx.DB {
	connStr := fmt.Sprintf(
		"host=%s port=5432 dbname=%s user=runner password=runner sslmode=disable",
		host, database,
	)
	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err, "Failed to connect to PostgreSQL database: %s", database)
	return db
}

// closeClients closes all client connections
func (c *testClients) close(ctx context.Context) {
	c.redisClient.Close()
	c.neo4jDriver.Close(ctx)
	c.startupDB.Close()
	c.executorDB.Close()
	c.dependencyDB.Close()
	c.k8sDB.Close()
	c.stateDB.Close()
}

// getEnv returns environment variable or default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
