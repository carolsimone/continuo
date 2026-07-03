package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	orchestratorClient  orchestratorv1.OrchestratorQueryClient
	stateClient         statev1.StateServiceClient
	redisClient         *goredis.Client
	neo4jDriver         neo4jdriver.DriverWithContext
	executorDB          *sqlx.DB
	orchestratorDB      *sqlx.DB
	k8sDB               *sqlx.DB
	stateDB             *sqlx.DB
	releaseDB           *sqlx.DB
	dbtDB               *sqlx.DB
	remediationDB       *sqlx.DB
	remediationAgentDB  *sqlx.DB
	s3Client            *s3.Client
	releaseBase         string
	logger              *slog.Logger
	uiBase              string
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
	releaseDB := connectPostgres(t, pgHost, "continuo_release")
	dbtDB := connectPostgres(t, pgHost, getEnv("DBT_POSTGRES_DB", "continuo_dbt"))
	remediationDB := connectPostgres(t, pgHost, "continuo_remediation")
	remediationAgentDB := connectPostgres(t, pgHost, "continuo_remediation_agent")

	return &testClients{
		orchestratorClient: orchestratorv1.NewOrchestratorQueryClient(orchestratorConn),
		stateClient:        statev1.NewStateServiceClient(stateConn),
		redisClient:        redisClient,
		neo4jDriver:        neo4jDriver,
		executorDB:         executorDB,
		orchestratorDB:     orchestratorDB,
		k8sDB:              k8sDB,
		stateDB:            stateDB,
		releaseDB:          releaseDB,
		dbtDB:              dbtDB,
		remediationDB:      remediationDB,
		remediationAgentDB: remediationAgentDB,
		s3Client:           newLocalstackS3Client(),
		releaseBase:        getEnv("RELEASE_CONTROLLER_BASE", "http://release-controller:8088"),
		logger:             logger,
		uiBase:             uiBase,
	}
}

// newLocalstackS3Client builds an S3 client pointed at the e2e LocalStack
// endpoint with path-style addressing (LocalStack does not support
// virtual-hosted-style buckets). Credentials and endpoint mirror the
// dbt-compile-and-load / manifest-controller configuration.
func newLocalstackS3Client() *s3.Client {
	cfg := aws.Config{
		Region: getEnv("AWS_DEFAULT_REGION", "us-east-1"),
		Credentials: awscreds.NewStaticCredentialsProvider(
			getEnv("AWS_ACCESS_KEY_ID", "test"),
			getEnv("AWS_SECRET_ACCESS_KEY", "test"), ""),
	}
	endpoint := getEnv("S3_ENDPOINT_URL", "http://localstack:4566")
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

// closeClients closes all client connections. Best-effort: this runs at test
// teardown, so a close failure on one connection shouldn't stop the others
// from being released.
func (c *testClients) close(ctx context.Context) {
	_ = c.redisClient.Close()
	_ = c.neo4jDriver.Close(ctx)
	_ = c.executorDB.Close()
	_ = c.orchestratorDB.Close()
	_ = c.k8sDB.Close()
	_ = c.stateDB.Close()
	_ = c.releaseDB.Close()
	_ = c.dbtDB.Close()
	_ = c.remediationDB.Close()
	_ = c.remediationAgentDB.Close()
}

// getEnv returns environment variable or default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
