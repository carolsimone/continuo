package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	deadletterv1 "github.com/carolsimone/continuo/dead-letter-controller/api/deadletter/v1"
	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
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
	orchestratorClient     orchestratorv1.OrchestratorQueryClient
	runClient              runv1.RunServiceClient
	agentRemediationClient remediationv1.RemediationProposalsClient
	deadLetterClient       deadletterv1.DeadLetterServiceClient
	redisClient            *goredis.Client
	neo4jDriver            neo4jdriver.DriverWithContext
	executionDB            *sqlx.DB
	orchestratorDB         *sqlx.DB
	runControllerDB        *sqlx.DB
	releaseDB              *sqlx.DB
	dbtDB                  *sqlx.DB
	remediationDB          *sqlx.DB
	agentRemediationDB     *sqlx.DB
	deadLetterDB           *sqlx.DB
	s3Client               *s3.Client
	releaseBase            string
	logger                 *slog.Logger
	uiBase                 string
}

// setupClients initializes all client connections and closes them when the
// test and its cleanups have ended.
func setupClients(t *testing.T, ctx context.Context) *testClients {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Get hosts from environment (use service names for docker-compose)
	orchestratorHost := getEnv("ORCHESTRATOR_HOST", "orchestrator")
	runControllerHost := getEnv("RUN_CONTROLLER_HOST", "run-controller")
	agentRemediationHost := getEnv("AGENT_REMEDIATION_HOST", "agent-remediation")
	deadLetterHost := getEnv("DEAD_LETTER_HOST", "dead-letter-controller")
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

	// Setup run-controller gRPC client (RunService, port 50051)
	runControllerConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50051", runControllerHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to run-controller service")

	// Setup agent-remediation gRPC client (RemediationProposals, port 50054)
	agentRemediationConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50054", agentRemediationHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to agent-remediation service")

	// Setup dead-letter-controller gRPC client (DeadLetterService, port 50055)
	deadLetterConn, err := grpc.NewClient(
		fmt.Sprintf("%s:50055", deadLetterHost),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to connect to dead-letter-controller service")

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
	executionDB := connectPostgres(t, pgHost, "continuo_execution")
	orchestratorDB := connectPostgres(t, pgHost, "continuo_orchestrator")
	runControllerDB := connectPostgres(t, pgHost, "continuo_run_controller")
	releaseDB := connectPostgres(t, pgHost, "continuo_release")
	dbtDB := connectPostgres(t, pgHost, getEnv("E2E_WAREHOUSE_DB", "continuo_dbt"))
	remediationDB := connectPostgres(t, pgHost, "continuo_remediation")
	agentRemediationDB := connectPostgres(t, pgHost, "continuo_agent_remediation")
	deadLetterDB := connectPostgres(t, pgHost, "continuo_dead_letter")

	clients := &testClients{
		orchestratorClient:     orchestratorv1.NewOrchestratorQueryClient(orchestratorConn),
		runClient:              runv1.NewRunServiceClient(runControllerConn),
		agentRemediationClient: remediationv1.NewRemediationProposalsClient(agentRemediationConn),
		deadLetterClient:       deadletterv1.NewDeadLetterServiceClient(deadLetterConn),
		redisClient:            redisClient,
		neo4jDriver:            neo4jDriver,
		executionDB:            executionDB,
		orchestratorDB:         orchestratorDB,
		runControllerDB:        runControllerDB,
		releaseDB:              releaseDB,
		dbtDB:                  dbtDB,
		remediationDB:          remediationDB,
		agentRemediationDB:     agentRemediationDB,
		deadLetterDB:           deadLetterDB,
		s3Client:               newMinioS3Client(),
		releaseBase:            getEnv("RELEASE_CONTROLLER_BASE", "http://release-controller:8088"),
		logger:                 logger,
		uiBase:                 uiBase,
	}
	// Registered here, before the test can register a cleanup of its own, so
	// cleanups run first in, first out: every one that reads or deletes rows
	// finds the pools open. A test never closes the clients itself.
	t.Cleanup(func() { clients.close(ctx) })
	return clients
}

// newMinioS3Client builds an S3 client pointed at the e2e MinIO endpoint
// with path-style addressing (the dev MinIO is reached by hostname, not
// virtual-hosted-style bucket subdomains). Credentials and endpoint mirror
// the dbt-compile-and-load / topology-controller configuration.
func newMinioS3Client() *s3.Client {
	cfg := aws.Config{
		Region: getEnv("AWS_DEFAULT_REGION", "us-east-1"),
		Credentials: awscreds.NewStaticCredentialsProvider(
			getEnv("AWS_ACCESS_KEY_ID", "minioadmin"),
			getEnv("AWS_SECRET_ACCESS_KEY", "minioadmin"), ""),
	}
	endpoint := getEnv("S3_ENDPOINT_URL", "http://minio:9000")
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
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

// close closes all client connections. Best-effort: this runs at test
// teardown, so a close failure on one connection shouldn't stop the others
// from being released. The Neo4j driver is closed on a context of its own, kept
// apart from ctx's cancellation and bounded to ten seconds, because a test
// registers close with t.Cleanup and its own ctx is cancelled by then.
func (c *testClients) close(ctx context.Context) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_ = c.redisClient.Close()
	_ = c.neo4jDriver.Close(closeCtx)
	_ = c.executionDB.Close()
	_ = c.orchestratorDB.Close()
	_ = c.runControllerDB.Close()
	_ = c.releaseDB.Close()
	_ = c.dbtDB.Close()
	_ = c.remediationDB.Close()
	_ = c.agentRemediationDB.Close()
	_ = c.deadLetterDB.Close()
}

// getEnv returns environment variable or default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
