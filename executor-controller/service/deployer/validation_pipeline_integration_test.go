//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	executorpg "github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	executorredis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturingDeployer is a fake deploy.Deployer that records every
// DeployValidation spec so the test can assert the dispatcher projects the
// queued validation rows onto the correct Kubernetes Job descriptions. K8s is
// never touched; the real apiserver call is replaced by this in-memory capture.
type capturingDeployer struct {
	mu          sync.Mutex
	validations []deploy.ValidationJobSpec
}

func (c *capturingDeployer) Deploy(context.Context, deploy.JobSpec) error {
	panic("production Deploy must not be invoked for validation rows")
}

func (c *capturingDeployer) DeployValidation(_ context.Context, spec deploy.ValidationJobSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.validations = append(c.validations, spec)
	return nil
}

func (c *capturingDeployer) DeploySeedBuild(_ context.Context, spec deploy.ValidationJobSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.validations = append(c.validations, spec)
	return nil
}

func (c *capturingDeployer) DeployCompile(_ context.Context, spec deploy.ValidationJobSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.validations = append(c.validations, spec)
	return nil
}

func (c *capturingDeployer) CountActive(context.Context) (int, error) { return 0, nil }

func (c *capturingDeployer) specs() []deploy.ValidationJobSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]deploy.ValidationJobSpec, len(c.validations))
	copy(out, c.validations)
	return out
}

// pipelineLogger is a quiet logger shared by the bindings under test.
func pipelineLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// noopSchemaCreator stubs the candidate-schema pre-create. The pipeline test
// drives the executor command queue, not the dbt warehouse, so the binding's
// warehouse-side schema creation is a no-op here.
type noopSchemaCreator struct{}

func (noopSchemaCreator) EnsureCandidateSchema(context.Context, string) error { return nil }

// newValidationRequestedBinding builds the production validation.requested:v1
// binding over the live *sqlx.DB.
func newValidationRequestedBinding(db *sqlx.DB) func(context.Context, goredis.XMessage) error {
	logger := pipelineLogger()
	uowFactory := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) }
	return executorredis.NewValidationRequestedBinding(
		uowFactory, handlers.NewValidationRequestedHandler(logger), noopSchemaCreator{}, logger)
}

// newNodeCompletedBinding builds the production validation.node.completed:v1
// binding over the live *sqlx.DB.
func newNodeCompletedBinding(db *sqlx.DB) func(context.Context, goredis.XMessage) error {
	logger := pipelineLogger()
	uowFactory := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) }
	return executorredis.NewValidationNodeCompletedBinding(
		uowFactory, handlers.NewValidationNodeCompletedHandler(logger), logger)
}

// newCapturingDispatcher wires a Dispatcher exactly as production does — real
// Postgres deployment + aggregate repos bound to its per-batch tx — but with a
// fake K8s deployer that records the validation specs.
func newCapturingDispatcher(db *sqlx.DB, dep deploy.Deployer) *deployer.Dispatcher {
	logger := pipelineLogger()
	return deployer.NewDispatcher(
		db, dep,
		func(exec outbox.Executor) repository.DeploymentRepository {
			return executorpg.NewDeploymentsRepository(exec, logger)
		},
		func(exec outbox.Executor) repository.ValidationAggregateRepository {
			return executorpg.NewValidationAggregateRepository(exec)
		},
		50, logger, deployer.DispatcherConfig{},
	)
}

// pipelineNode is one entry in the canonical validation.requested:v1 nodes[].
type pipelineNode struct {
	UniqueID    string `json:"unique_id"`
	ServiceName string `json:"service_name"`
	NodeType    string `json:"node_type"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	ImageTag    string `json:"image_tag"`
}

// requestedXMessage assembles a validation.requested:v1 XMessage with the full
// canonical payload (image_tags, candidate_schema, dbt_flags) for the given
// nodes — the exact wire shape release-controller emits.
func requestedXMessage(t *testing.T, msgID, releaseID, candidateSchema string, nodes []pipelineNode) goredis.XMessage {
	t.Helper()
	order := make([]string, 0, len(nodes))
	imageTags := map[string]string{}
	for _, n := range nodes {
		order = append(order, n.UniqueID)
		imageTags[n.ServiceName] = n.ImageTag
	}
	body := map[string]any{
		"release_id":        releaseID,
		"mode":              "validation",
		"nodes":             nodes,
		"node_ids_in_order": order,
		"image_tags":        imageTags,
		"candidate_schema":  candidateSchema,
		"dbt_flags":         []string{"--empty"},
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return goredis.XMessage{ID: msgID, Values: map[string]any{"payload": string(raw)}}
}

// nodeTerminalXMessage assembles a validation.node.completed:v1 XMessage — the
// per-node terminal a real k8s-controller emits after status-checking the Job.
func nodeTerminalXMessage(t *testing.T, msgID, releaseID, nodeID, outcome string) goredis.XMessage {
	t.Helper()
	body := map[string]any{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": "s3://logs/" + releaseID + "/" + nodeID,
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return goredis.XMessage{ID: msgID, Values: map[string]any{"payload": string(raw)}}
}

func countByStream(t *testing.T, db *sqlx.DB, stream string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM executor_outbox WHERE stream_name=$1`, stream).Scan(&n))
	return n
}

// countByStreamAndKind counts the rows on stream whose payload's "kind" field
// matches kind. The unified validation.result:v1 stream carries two message
// kinds — "node" (one per settled node) and "complete" (the terminal decision,
// emitted last) — so callers that care about only one of them must filter by
// kind rather than count every row on the stream.
func countByStreamAndKind(t *testing.T, db *sqlx.DB, stream, kind string) int {
	t.Helper()
	rows, err := db.Query(`SELECT payload FROM executor_outbox WHERE stream_name=$1`, stream)
	require.NoError(t, err)
	defer rows.Close()

	n := 0
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		var v struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, json.Unmarshal(payload, &v))
		if v.Kind == kind {
			n++
		}
	}
	require.NoError(t, rows.Err())
	return n
}

func countDeployments(t *testing.T, db *sqlx.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(query, args...).Scan(&n))
	return n
}

// waitValidationRowsDue blocks until no pending validation row for releaseID has
// a next_attempt_at in the DB's future. next_attempt_at is stamped with the Go
// process clock and GetDueBatch compares against the DB's NOW(); on a VM-backed
// Docker host those clocks can differ by tens of milliseconds, so a row enqueued
// "now" can read as not-yet-due for a moment. Production dispatches on a 5s tick
// where this is moot; the test dispatches immediately, so wait for the DB to
// agree the rows are due before driving the dispatcher.
func waitValidationRowsDue(t *testing.T, db *sqlx.DB, releaseID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		notYetDue := countDeployments(t, db,
			`SELECT COUNT(*) FROM executor_deployments
			 WHERE release_id=$1 AND mode='validation' AND status='pending' AND next_attempt_at > NOW()`,
			releaseID)
		if notYetDue == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("validation rows for %s still not due after 5s (host/DB clock skew)", releaseID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestValidationPipeline_EndToEnd drives the whole executor side of the
// validation pipeline against testcontainer Postgres with a fake K8s deployer:
// validation.requested:v1 → per-node deployments → dispatch (validation Jobs +
// node.deployed:v1 triggers) → simulated per-node terminals → aggregate gating →
// exactly-once validation.result:v1 (kind=complete). The validation.node.completed:v1
// terminals are SIMULATED here exactly as a real k8s-controller would emit them
// after observing the node.deployed trigger; the routing itself is covered by
// the k8s-controller's own tests. This test owns the executor side.
func TestValidationPipeline_EndToEnd(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	releaseID := "rel-pipeline-1"
	candidateSchema := "_candidate_" + releaseID
	nodes := []pipelineNode{
		{UniqueID: "model.shop.orders", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "orders", ImageTag: "sha-orders"},
		{UniqueID: "model.shop.customers", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "customers", ImageTag: "sha-customers"},
		{UniqueID: "model.shop.line_items", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "line_items", ImageTag: "sha-line-items"},
	}

	// Step 2: validation.requested:v1 → one pending validation deployment per node.
	requested := newValidationRequestedBinding(db)
	require.NoError(t, requested(ctx, requestedXMessage(t, "100-0", releaseID, candidateSchema, nodes)))

	assert.Equal(t, 3, countDeployments(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode='validation' AND release_id=$1`, releaseID),
		"one validation deployment row per node")
	for _, n := range nodes {
		assert.Equal(t, 1, countDeployments(t, db,
			`SELECT COUNT(*) FROM executor_deployments WHERE mode='validation' AND release_id=$1 AND node_id=$2 AND status='pending'`,
			releaseID, n.UniqueID), "node %s enqueued pending", n.UniqueID)
	}

	// Step 3: dispatch the batch. Each row → a DeployValidation call + a
	// node.deployed:v1 trigger; rows move to status=deployed; NO terminal yet.
	// Wait for the rows to be due first: next_attempt_at is stamped with the Go
	// process clock while GetDueBatch compares against the DB's NOW(), and on a
	// VM-backed Docker host (colima) those can differ by tens of ms. Production
	// dispatches on a 5s tick so the skew is irrelevant there; the test dispatches
	// immediately, so wait until the DB agrees the rows are due.
	waitValidationRowsDue(t, db, releaseID)
	dep := &capturingDeployer{}
	require.NoError(t, newCapturingDispatcher(db, dep).ProcessBatch(ctx))

	specs := dep.specs()
	require.Len(t, specs, 3, "three validation Jobs dispatched")
	byNode := map[string]deploy.ValidationJobSpec{}
	for _, s := range specs {
		byNode[s.NodeID] = s
	}
	for _, n := range nodes {
		s, ok := byNode[n.UniqueID]
		require.True(t, ok, "spec for node %s", n.UniqueID)
		assert.Equal(t, handlers.BuildValidationJobName(releaseID, n.UniqueID), s.JobName, "job_name derived deterministically")
		assert.Equal(t, releaseID, s.ReleaseID)
		assert.Equal(t, n.NodeType, s.NodeType)
		assert.Equal(t, n.TableName, s.TableName)
		assert.Equal(t, n.SchemaName, s.SchemaName)
		assert.Equal(t, n.ServiceName, s.ServiceName)
		assert.Equal(t, n.ImageTag, s.ImageTag)
		assert.Equal(t, candidateSchema, s.CandidateSchema, "candidate schema threaded to the Job")
	}

	// node.deployed:v1 trigger: one per dispatched validation Job so k8s-controller
	// status-checks each (it never polls).
	assert.Equal(t, 3, countByStream(t, db, streams.NodeDeployedV1),
		"one node.deployed:v1 trigger per dispatched validation Job")
	assert.Equal(t, 3, countDeployments(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode='validation' AND release_id=$1 AND status='deployed'`, releaseID),
		"all rows now deployed")
	assert.Equal(t, 0, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"),
		"no aggregate yet — every node still awaits its terminal")

	// Step 4: simulate k8s-controller terminals for 2 of 3 nodes (outcome=ok).
	nodeCompleted := newNodeCompletedBinding(db)
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "400-0", releaseID, "model.shop.orders", "ok")))
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "401-0", releaseID, "model.shop.customers", "ok")))

	assert.Equal(t, 2, countDeployments(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode='validation' AND release_id=$1 AND outcome='ok' AND outcome_at IS NOT NULL`, releaseID),
		"two nodes recorded their outcome")
	assert.Equal(t, 0, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"),
		"aggregate still gated — third node not yet terminal")

	// Step 5: the third node terminates ok → aggregate fires exactly once.
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "402-0", releaseID, "model.shop.line_items", "ok")))

	assert.Equal(t, 1, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"),
		"validation.result:v1 (kind=complete) emitted exactly once when the last node terminates")
	assertAggregate(t, db, releaseID, "ok", []string{
		"model.shop.orders", "model.shop.customers", "model.shop.line_items",
	})

	// Step 6: redelivery of the third node's terminal (fresh message_id). The
	// sentinel was already claimed, so no second aggregate; the binding dedups/ACKs.
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "403-0", releaseID, "model.shop.line_items", "ok")))
	assert.Equal(t, 1, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"),
		"redelivery must not emit a second aggregate")
}

// TestValidationPipeline_FailurePath drives the same pipeline on a fresh release
// where one node terminates failed: the emitted validation.result:v1
// (kind=complete) must carry aggregate_status=failed.
func TestValidationPipeline_FailurePath(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	releaseID := "rel-pipeline-fail"
	candidateSchema := "_candidate_" + releaseID
	nodes := []pipelineNode{
		{UniqueID: "model.shop.orders", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "orders", ImageTag: "sha-orders"},
		{UniqueID: "model.shop.customers", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "customers", ImageTag: "sha-customers"},
		{UniqueID: "model.shop.line_items", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "public", TableName: "line_items", ImageTag: "sha-line-items"},
	}

	requested := newValidationRequestedBinding(db)
	require.NoError(t, requested(ctx, requestedXMessage(t, "500-0", releaseID, candidateSchema, nodes)))

	waitValidationRowsDue(t, db, releaseID)
	dep := &capturingDeployer{}
	require.NoError(t, newCapturingDispatcher(db, dep).ProcessBatch(ctx))
	require.Len(t, dep.specs(), 3)

	nodeCompleted := newNodeCompletedBinding(db)
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "600-0", releaseID, "model.shop.orders", "ok")))
	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "601-0", releaseID, "model.shop.customers", "failed")))
	assert.Equal(t, 0, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"),
		"still gated until every node is terminal")

	require.NoError(t, nodeCompleted(ctx, nodeTerminalXMessage(t, "602-0", releaseID, "model.shop.line_items", "ok")))

	assert.Equal(t, 1, countByStreamAndKind(t, db, streams.ValidationResultV1, "complete"))
	assertAggregate(t, db, releaseID, "failed", []string{
		"model.shop.orders", "model.shop.customers", "model.shop.line_items",
	})
}

// assertAggregate reads the single validation.result:v1 (kind=complete) outbox
// row for a release and asserts its slim decision payload (release_id +
// aggregate_status, no per-node content). Per-node results reach
// release-controller through the same stream's (kind=node) rows; this checks
// one such row was emitted per expected node so the read model can reconstruct
// the set.
func assertAggregate(t *testing.T, db *sqlx.DB, releaseID, wantStatus string, wantNodeIDs []string) {
	t.Helper()
	var payload []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM executor_outbox WHERE stream_name=$1 AND payload->>'kind'='complete' LIMIT 1`,
		streams.ValidationResultV1).Scan(&payload))

	var got struct {
		ReleaseID       string `json:"release_id"`
		AggregateStatus string `json:"aggregate_status"`
	}
	require.NoError(t, json.Unmarshal(payload, &got))
	assert.Equal(t, releaseID, got.ReleaseID)
	assert.Equal(t, wantStatus, got.AggregateStatus)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(payload, &raw))
	_, present := raw["per_node_results"]
	assert.False(t, present, "terminal event must not re-carry per-node content")

	assert.Equal(t, len(wantNodeIDs), countByStreamAndKind(t, db, streams.ValidationResultV1, "node"),
		"one per-node projection row emitted per settled node")
}
