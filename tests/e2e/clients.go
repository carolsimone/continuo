package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
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
	orchestratorClient orchestratorv1.OrchestratorQueryClient
	stateClient        statev1.StateServiceClient
	redisClient        *goredis.Client
	neo4jDriver        neo4jdriver.DriverWithContext
	executorDB         *sqlx.DB
	orchestratorDB     *sqlx.DB
	k8sDB              *sqlx.DB
	stateDB            *sqlx.DB
	logger             *slog.Logger
	uiBase             string
}

// setupClients initializes all client connections
func setupClients(t *testing.T, ctx context.Context) *testClients {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Get hosts from environment (use service names for docker-compose)
	orchestratorHost := getEnv("ORCHESTRATOR_HOST", "orchestrator")
	stateHost := getEnv("STATE_HOST", "state")
	redisHost := getEnv("REDIS_HOST", "redis")
	neo4jHost := getEnv("NEO4J_HOST", "neo4j")
	pgHost := getEnv("POSTGRES_HOST", "postgres")
	uiBase := getEnv("UI_HTTP_BASE", "http://ui:8090")

	// Setup Orchestrator gRPC client
	orchestratorConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50052", orchestratorHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to orchestrator service")

	// Setup State gRPC client
	stateConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50051", stateHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to state service")

	// Setup Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:6379", redisHost),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})
	require.NoError(t, redisClient.Ping(ctx).Err(), "Failed to connect to Redis")

	// Setup Neo4j driver
	neo4jDriver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://%s:7687", neo4jHost),
		neo4jdriver.BasicAuth("neo4j", "atlas_password", ""),
	)
	require.NoError(t, err, "Failed to connect to Neo4j")

	// Setup PostgreSQL connections for each database
	executorDB := connectPostgres(t, pgHost, "continuo_executor")
	orchestratorDB := connectPostgres(t, pgHost, "continuo_orchestrator")
	k8sDB := connectPostgres(t, pgHost, "continuo_k8s")
	stateDB := connectPostgres(t, pgHost, "continuo_state")

	return &testClients{
		orchestratorClient: orchestratorv1.NewOrchestratorQueryClient(orchestratorConn),
		stateClient:        statev1.NewStateServiceClient(stateConn),
		redisClient:        redisClient,
		neo4jDriver:        neo4jDriver,
		executorDB:         executorDB,
		orchestratorDB:     orchestratorDB,
		k8sDB:              k8sDB,
		stateDB:            stateDB,
		logger:             logger,
		uiBase:             uiBase,
	}
}

// connectPostgres establishes a PostgreSQL connection
func connectPostgres(t *testing.T, host, database string) *sqlx.DB {
	pgPassword := getEnv("POSTGRES_PASSWORD", "continuo")
	connStr := fmt.Sprintf(
		"host=%s port=5432 dbname=%s user=continuo_svc password=%s sslmode=disable",
		host, database, pgPassword,
	)
	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err, "Failed to connect to PostgreSQL database: %s", database)
	return db
}

// closeClients closes all client connections
func (c *testClients) close(ctx context.Context) {
	c.redisClient.Close()
	c.neo4jDriver.Close(ctx)
	c.executorDB.Close()
	c.orchestratorDB.Close()
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
