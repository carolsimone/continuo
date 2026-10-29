package test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	stateredis "github.com/carolsimone/continuo/run-controller/adapters/redis"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	svchandlers "github.com/carolsimone/continuo/run-controller/service/handlers"
	svcports "github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
)

// memoryArtifacts serves topology artifacts from memory, keyed by URI and
// checksum, or fails every read with err when it is set.
type memoryArtifacts struct {
	byRef map[string]svcports.TopologyArtifact
	err   error
}

func (m *memoryArtifacts) Load(_ context.Context, uri, sha256 string) (svcports.TopologyArtifact, error) {
	if m.err != nil {
		return svcports.TopologyArtifact{}, m.err
	}
	a, ok := m.byRef[uri+"|"+sha256]
	if !ok {
		return svcports.TopologyArtifact{}, fmt.Errorf("%w: %s", svcports.ErrTopologyArtifactNotFound, uri)
	}
	return a, nil
}

// promotionHarness drives run-controller's release.promoted:v2 binding against a real
// Postgres. Every delivery gets a fresh stream message id.
type promotionHarness struct {
	artifacts *memoryArtifacts
	binding   pkgredis.MessageHandler
	delivered int
}

func newPromotionHarness(db *sqlx.DB) *promotionHarness {
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, svcports.SystemClock{}, logger, nil)
	}
	artifacts := &memoryArtifacts{byRef: map[string]svcports.TopologyArtifact{}}
	handler := svchandlers.NewReleasePromotedHandler(artifacts, nil, logger)
	return &promotionHarness{artifacts: artifacts, binding: stateredis.NewReleasePromotedBinding(factory, handler, logger)}
}

// publish stores releaseID's artifact under checksum sha and returns its URI.
func (h *promotionHarness) publish(tenantID, releaseID, sha string, nodes ...topology.NodeSpec) string {
	uri := "s3://continuo/tenants/" + tenantID + "/topologies/" + releaseID + "/topology.json.gz"
	h.artifacts.byRef[uri+"|"+sha] = svcports.TopologyArtifact{TenantID: tenantID, ReleaseID: releaseID, Nodes: nodes}
	return uri
}

// deliver hands the binding one release.promoted:v2 entry and returns its
// message id and the binding's error.
func (h *promotionHarness) deliver(t *testing.T, tenantID, releaseID string, seq int64, uri, sha string) (string, error) {
	t.Helper()
	now := time.Now().UTC()
	fields, err := pkgevents.ReleasePromotedFields(tenantID, "release-controller", now, pkgevents.ReleasePromoted{
		ReleaseID: releaseID, PromotedAt: now, PromotionSeq: seq, TopologyURI: uri, TopologySHA256: sha,
		ChangedNodeIDs: []string{},
	})
	require.NoError(t, err)
	h.delivered++
	id := fmt.Sprintf("1760000000000-%d", h.delivered)
	return id, h.binding(context.Background(), goredis.XMessage{ID: id, Values: fields})
}

func promotedNode(schema, table, nodeType string, upstream ...string) topology.NodeSpec {
	zero := 0
	return topology.NodeSpec{
		UniqueID: schema + "." + table, ServiceName: "service-1", SchemaName: schema, TableName: table,
		NodeType: nodeType, TestCount: &zero, ContentHash: "sha256:" + table, ImageTag: "service-1:v1",
		ScheduleName: "daily", UpstreamUniqueIDs: upstream,
	}
}

func livePointerOf(t *testing.T, db *sqlx.DB, tenantID string) topology.Pointer {
	t.Helper()
	var row struct {
		ReleaseID    string `db:"release_id"`
		PromotionSeq int64  `db:"promotion_seq"`
	}
	err := db.Get(&row, `SELECT release_id, promotion_seq FROM live_release WHERE tenant_id = $1`, tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return topology.Pointer{}
	}
	require.NoError(t, err)
	return topology.Pointer{ReleaseID: row.ReleaseID, PromotionSeq: row.PromotionSeq}
}

func countOf(t *testing.T, db *sqlx.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n, query, args...))
	return n
}

// releaseTimes is when a tenant's live release was loaded and made live.
type releaseTimes struct {
	LoadedAt  string `db:"loaded_at"`
	UpdatedAt string `db:"updated_at"`
}

// TestReleasePromotedBinding drives the release.promoted:v2 binding through the
// pointer outcomes and failure classes of a promotion. Each subtest uses its
// own tenant, so they share one database without touching each other's rows.
func TestReleasePromotedBinding(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newPromotionHarness(db)

	t.Run("the first promotion stores the release and moves the pointer", func(t *testing.T) {
		const tenant = "t-first"
		uri := h.publish(tenant, "rel-1", "sha-1",
			promotedNode("raw", "orders", "dbt-seed"),
			promotedNode("stg", "orders", "dbt-model", "raw.orders"),
			topology.NodeSpec{UniqueID: "test.service_1.not_null_orders_id", SchemaName: "stg", TableName: "not_null_orders_id",
				NodeType: "dbt-test", UpstreamUniqueIDs: []string{"stg.orders"}})

		id, err := h.deliver(t, tenant, "rel-1", 1, uri, "sha-1")
		require.NoError(t, err)

		assert.Equal(t, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, livePointerOf(t, db, tenant))
		assert.Equal(t, 2, countOf(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1 AND release_id = 'rel-1'`, tenant),
			"the dbt test is not stored")
		assert.Equal(t, 1, countOf(t, db, `
			SELECT count(*) FROM topology_edges
			WHERE tenant_id = $1 AND release_id = 'rel-1' AND upstream_key = 'raw.orders' AND downstream_key = 'stg.orders'`, tenant))
		assert.Equal(t, 1, countOf(t, db, `
			SELECT count(*) FROM message_processing
			WHERE message_id = $1 AND stream_name = $2 AND state = 'completed'`, id, streams.ReleasePromotedV2),
			"the dedup row is completed in the same transaction")
	})

	t.Run("a redelivery under a new message id changes nothing", func(t *testing.T) {
		const tenant = "t-redelivery"
		uri := h.publish(tenant, "rel-1", "sha-1", promotedNode("raw", "orders", "dbt-seed"))
		_, err := h.deliver(t, tenant, "rel-1", 2, uri, "sha-1")
		require.NoError(t, err)
		const read = `
			SELECT r.loaded_at::text AS loaded_at, l.updated_at::text AS updated_at
			FROM topology_releases r
			JOIN live_release l ON l.tenant_id = r.tenant_id AND l.release_id = r.release_id
			WHERE r.tenant_id = $1`
		var before, after releaseTimes
		require.NoError(t, db.Get(&before, read, tenant))

		_, err = h.deliver(t, tenant, "rel-1", 2, uri, "sha-1")
		require.NoError(t, err)

		require.NoError(t, db.Get(&after, read, tenant))
		assert.Equal(t, before, after)
		assert.Equal(t, 1, countOf(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1`, tenant))
	})

	t.Run("an older promotion is stored as history and leaves the pointer", func(t *testing.T) {
		const tenant = "t-older"
		newer := h.publish(tenant, "rel-new", "sha-new", promotedNode("raw", "orders", "dbt-seed"))
		older := h.publish(tenant, "rel-old", "sha-old", promotedNode("raw", "customers", "dbt-seed"))
		_, err := h.deliver(t, tenant, "rel-new", 5, newer, "sha-new")
		require.NoError(t, err)

		_, err = h.deliver(t, tenant, "rel-old", 4, older, "sha-old")
		require.NoError(t, err, "a late promotion is acknowledged")

		assert.Equal(t, topology.Pointer{ReleaseID: "rel-new", PromotionSeq: 5}, livePointerOf(t, db, tenant))
		assert.Equal(t, 1, countOf(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1 AND release_id = 'rel-old'`, tenant),
			"the older release is stored")
	})

	t.Run("another release under the live seq leaves the pointer", func(t *testing.T) {
		const tenant = "t-conflict"
		a := h.publish(tenant, "rel-a", "sha-a", promotedNode("raw", "orders", "dbt-seed"))
		b := h.publish(tenant, "rel-b", "sha-b", promotedNode("raw", "orders", "dbt-seed"))
		_, err := h.deliver(t, tenant, "rel-a", 3, a, "sha-a")
		require.NoError(t, err)

		_, err = h.deliver(t, tenant, "rel-b", 3, b, "sha-b")
		require.NoError(t, err, "the conflict is logged and the entry acknowledged")

		assert.Equal(t, topology.Pointer{ReleaseID: "rel-a", PromotionSeq: 3}, livePointerOf(t, db, tenant))
	})

	t.Run("a stored release announced with another checksum is permanent", func(t *testing.T) {
		const tenant = "t-checksum"
		first := h.publish(tenant, "rel-1", "sha-1", promotedNode("raw", "orders", "dbt-seed"))
		second := h.publish(tenant, "rel-1", "sha-2", promotedNode("raw", "customers", "dbt-seed"))
		_, err := h.deliver(t, tenant, "rel-1", 1, first, "sha-1")
		require.NoError(t, err)

		_, err = h.deliver(t, tenant, "rel-1", 2, second, "sha-2")
		require.Error(t, err)
		assert.Equal(t, pkgredis.ClassPermanent, pkgredis.Classify(err))
		assert.Equal(t, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, livePointerOf(t, db, tenant))
	})

	t.Run("two nodes with one key are permanent", func(t *testing.T) {
		const tenant = "t-duplicate"
		uri := h.publish(tenant, "rel-1", "sha-1",
			promotedNode("raw", "orders", "dbt-seed"),
			topology.NodeSpec{UniqueID: "RAW.Orders", ServiceName: "service-2", SchemaName: "RAW", TableName: "Orders", NodeType: "python-csv"})

		_, err := h.deliver(t, tenant, "rel-1", 1, uri, "sha-1")
		require.Error(t, err)
		assert.Equal(t, pkgredis.ClassPermanent, pkgredis.Classify(err))
		assert.Zero(t, countOf(t, db, `SELECT count(*) FROM topology_releases WHERE tenant_id = $1`, tenant))
	})

	t.Run("a missing artifact is retried and opens no transaction", func(t *testing.T) {
		id, err := h.deliver(t, "t-missing", "rel-1", 1, "s3://continuo/tenants/t-missing/topologies/rel-1/topology.json.gz", "sha-1")
		require.Error(t, err)
		assert.ErrorIs(t, err, svcports.ErrTopologyArtifactNotFound)
		assert.Equal(t, pkgredis.ClassTransient, pkgredis.Classify(err))
		assert.Zero(t, countOf(t, db, `SELECT count(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
			id, streams.ReleasePromotedV2), "the artifact is read before the transaction opens")
	})

	t.Run("an object-storage outage pauses the consumer", func(t *testing.T) {
		h.artifacts.err = fmt.Errorf("read topology artifact: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
		defer func() { h.artifacts.err = nil }()

		_, err := h.deliver(t, "t-outage", "rel-1", 1, "s3://continuo/k", "sha-1")
		require.Error(t, err)
		assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
	})

	t.Run("a corrupt artifact is permanent", func(t *testing.T) {
		h.artifacts.err = fmt.Errorf("%w: s3://continuo/k: checksum mismatch", svcports.ErrTopologyArtifactCorrupt)
		defer func() { h.artifacts.err = nil }()

		_, err := h.deliver(t, "t-corrupt", "rel-1", 1, "s3://continuo/k", "sha-1")
		require.Error(t, err)
		assert.Equal(t, pkgredis.ClassPermanent, pkgredis.Classify(err))
		assert.True(t, livePointerOf(t, db, "t-corrupt").IsZero())
	})

	t.Run("a pointer move deletes the materializations of removed nodes", func(t *testing.T) {
		const tenant = "t-removed"
		first := h.publish(tenant, "rel-1", "sha-1", promotedNode("raw", "orders", "dbt-seed"), promotedNode("raw", "customers", "dbt-seed"))
		second := h.publish(tenant, "rel-2", "sha-2", promotedNode("raw", "orders", "dbt-seed"))
		_, err := h.deliver(t, tenant, "rel-1", 1, first, "sha-1")
		require.NoError(t, err)
		for _, key := range []string{"raw.orders", "raw.customers"} {
			_, err := db.Exec(`
				INSERT INTO node_materializations (tenant_id, node_key, content_hash, release_id, source, materialized_at)
				VALUES ($1, $2, 'sha256:x', 'rel-1', 'adopted', now())`, tenant, key)
			require.NoError(t, err)
		}

		_, err = h.deliver(t, tenant, "rel-2", 2, second, "sha-2")
		require.NoError(t, err)

		var keys []string
		require.NoError(t, db.Select(&keys, `SELECT node_key FROM node_materializations WHERE tenant_id = $1 ORDER BY node_key`, tenant))
		assert.Equal(t, []string{"raw.orders"}, keys)
	})
}
