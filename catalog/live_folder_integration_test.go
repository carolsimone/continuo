//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	serializationinfra "github.com/carolsimone/continuo/catalog/adapters/serialization"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The contracts group with the live folder as main wires it, against the test
// Neo4j and an object store in memory.

// liveFolderFixture is the contracts handler of one tenant whose live folder
// is the production LiveFolder, publishing with the settings whose
// fingerprint is settings.
type liveFolderFixture struct {
	*contractsFixture
	folder   *handlers.LiveFolder
	settings string
}

// fingerprintOf is the settings fingerprint a LiveFolder publishing with
// server stores.
func fingerprintOf(server *contractversion.Server) string {
	s := contractversion.Server{}
	if server != nil {
		s = *server
	}
	return s.Fingerprint(func(b []byte) string {
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	})
}

// newLiveFolder is a production LiveFolder of the fixture's graph and store,
// publishing with server.
func (f *liveFolderFixture) newLiveFolder(server *contractversion.Server) *handlers.LiveFolder {
	lease := neo4jinfra.NewLiveFolderLease(f.client, quietLogger())
	return handlers.NewLiveFolder(neo4jinfra.NewContractQueryRepository(f.client), s3infra.NewLiveContractsStore(f.mem),
		serializationinfra.ODCSYAML{}, lease, lease, f.metrics, server, time.Now, quietLogger())
}

func newLiveFolderFixture(t *testing.T, server *contractversion.Server) *liveFolderFixture {
	t.Helper()
	c := newContractsFixture(t)
	lease := neo4jinfra.NewLiveFolderLease(c.client, quietLogger())
	folder := handlers.NewLiveFolder(
		neo4jinfra.NewContractQueryRepository(c.client),
		s3infra.NewLiveContractsStore(c.mem),
		serializationinfra.ODCSYAML{},
		lease, lease, c.metrics, server,
		func() time.Time { return time.Date(2026, 11, 27, 16, 40, 14, 0, time.UTC) },
		quietLogger(),
	)
	c.handler = handlers.NewReleasePromotedContractsHandler(
		c.uow, s3infra.NewContractBundleReader(c.mem), folder, c.metrics, time.Minute, quietLogger())
	return &liveFolderFixture{contractsFixture: c, folder: folder, settings: fingerprintOf(server)}
}

// object returns the live folder's object name (a file or _index.json) and
// whether it exists.
func (f *liveFolderFixture) object(name string) (string, bool) {
	f.t.Helper()
	body, err := f.mem.Get(context.Background(),
		objectstore.Ref{Key: "tenants/" + f.tenantID + "/contracts/live/" + name}, 1<<20)
	if err != nil {
		require.ErrorIs(f.t, err, objectstore.ErrNotFound)
		return "", false
	}
	return string(body), true
}

// livePuts lists the keys of the live folder's objects put so far, in order,
// relative to the folder.
func (f *liveFolderFixture) livePuts() []string {
	prefix := "tenants/" + f.tenantID + "/contracts/live/"
	var keys []string
	for _, w := range f.mem.Puts() {
		if name, ok := strings.CutPrefix(w.Ref.Key, prefix); ok {
			keys = append(keys, name)
		}
	}
	return keys
}

// A promotion publishes each live contract's file and then the index, and
// transaction B stores the folder's watermark and settings; a later promotion
// that changes nothing puts only the index; one that changes a contract puts
// its file again, at its new version.
func TestLiveFolder_APromotionPublishesTheFolderAndItsWatermark(t *testing.T) {
	server := &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}
	f := newLiveFolderFixture(t, server)
	ctx := context.Background()

	first := f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c", "analytics.py_weekly": "sha256:a0b1"})
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, first))

	require.Len(t, f.livePuts(), 4)
	assert.Equal(t, "_index.json", f.livePuts()[0], "an interim index lists the new files before they are put")
	assert.ElementsMatch(t, []string{"analytics.py_daily_kpis.odcs.yaml", "analytics.py_weekly.odcs.yaml"}, f.livePuts()[1:3])
	assert.Equal(t, "_index.json", f.livePuts()[3], "the final index is written last")
	file, ok := f.object("analytics.py_daily_kpis.odcs.yaml")
	require.True(t, ok)
	assert.Contains(t, file, "version: \"1\"\n")
	assert.Contains(t, file, "  - {server: warehouse, type: postgres, host: warehouse.internal.example, port: 5432, database: continuo_dbt, schema: analytics}\n")
	assert.Contains(t, file, "      releaseId: rel-1\n")
	raw, ok := f.object("_index.json")
	require.True(t, ok)
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	assert.Equal(t, float64(41), index["promotion_seq"])
	assert.Equal(t, f.settings, index["settings_sha256"])
	assert.Equal(t, []any{
		map[string]any{"unique_id": "analytics.py_daily_kpis", "key": "analytics.py_daily_kpis.odcs.yaml", "version": float64(1), "contract_hash": "sha256:3f1c"},
		map[string]any{"unique_id": "analytics.py_weekly", "key": "analytics.py_weekly.odcs.yaml", "version": float64(1), "contract_hash": "sha256:a0b1"},
	}, index["files"])
	live := `MATCH (m:Meta {tenant_id: $tenant_id, key: 'contracts-live'}) `
	assert.Equal(t, int64(41), f.value(live+`RETURN m.promotion_seq AS v`))
	assert.Equal(t, f.settings, f.value(live+`RETURN m.settings_sha256 AS v`))
	assert.Nil(t, f.value(live+`RETURN m.holder AS v`), "the lease is released")
	assert.Nil(t, f.inflight())

	same := f.release("rel-2", 42, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c", "analytics.py_weekly": "sha256:a0b1"})
	require.NoError(t, f.handler.Handle(ctx, "42-0", nil, same))
	assert.Len(t, f.livePuts(), 5)
	assert.Equal(t, "_index.json", f.livePuts()[4], "an unchanged folder puts only its index")

	changed := f.release("rel-3", 43, map[string]string{"analytics.py_daily_kpis": "sha256:b07e", "analytics.py_weekly": "sha256:a0b1"})
	require.NoError(t, f.handler.Handle(ctx, "43-0", nil, changed))
	assert.Equal(t, []string{"analytics.py_daily_kpis.odcs.yaml", "_index.json"}, f.livePuts()[5:],
		"a changed file the index lists needs no interim index")
	file, _ = f.object("analytics.py_daily_kpis.odcs.yaml")
	assert.Contains(t, file, "version: \"2\"\n")
	assert.Equal(t, int64(43), f.value(live+`RETURN m.promotion_seq AS v`))
}

// The live folder never waits for the topology swap: a node a promotion adds,
// which the contracts group creates inactive, gets its file in that
// promotion's rewrite, and a node a promotion drops loses its file in that
// rewrite while the swap has not retired it yet.
func TestLiveFolder_ANewNodeAndADroppedNodeChangeTheFolderInTheSamePromotion(t *testing.T) {
	f := newLiveFolderFixture(t, nil)
	ctx := context.Background()
	f.exec(f.tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_weekly', active: true})`, nil)
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil,
		f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c", "analytics.py_weekly": "sha256:a0b1"})))
	_, ok := f.object("analytics.py_daily_kpis.odcs.yaml")
	assert.True(t, ok, "a node the graph did not hold, created inactive, is published at once")
	assert.Equal(t, false,
		f.value(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_daily_kpis'}) RETURN n.active AS v`))

	require.NoError(t, f.handler.Handle(ctx, "42-0", nil,
		f.release("rel-2", 42, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c", "analytics.py_new": "sha256:c0de"})))

	_, ok = f.object("analytics.py_weekly.odcs.yaml")
	assert.False(t, ok, "the dropped node's file is deleted while the node is still active")
	assert.Equal(t, true,
		f.value(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_weekly'}) RETURN n.active AS v`))
	file, ok := f.object("analytics.py_new.odcs.yaml")
	require.True(t, ok, "the added node's file is put in the same promotion")
	assert.NotContains(t, file, "servers:", "no server settings, no servers entry")
	raw, _ := f.object("_index.json")
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	assert.Equal(t, []any{
		map[string]any{"unique_id": "analytics.py_daily_kpis", "key": "analytics.py_daily_kpis.odcs.yaml", "version": float64(1), "contract_hash": "sha256:3f1c"},
		map[string]any{"unique_id": "analytics.py_new", "key": "analytics.py_new.odcs.yaml", "version": float64(1), "contract_hash": "sha256:c0de"},
	}, index["files"])
}

// A promotion whose bundle names no contract empties the folder.
func TestLiveFolder_AnEmptyBundleEmptiesTheFolder(t *testing.T) {
	f := newLiveFolderFixture(t, nil)
	ctx := context.Background()
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil,
		f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})))

	require.NoError(t, f.handler.Handle(ctx, "42-0", nil, f.release("rel-2", 42, map[string]string{})))

	_, ok := f.object("analytics.py_daily_kpis.odcs.yaml")
	assert.False(t, ok)
	raw, _ := f.object("_index.json")
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	assert.Equal(t, []any{}, index["files"])
	assert.Equal(t, float64(42), index["promotion_seq"])
}

// At start, a tenant whose folder was written with other settings has every
// file rewritten with the current ones, and the new fingerprint stored.
func TestLiveFolder_ChangedSettingsRewriteTheFolderAtStart(t *testing.T) {
	f := newLiveFolderFixture(t, nil)
	ctx := context.Background()
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil,
		f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})))

	server := &contractversion.Server{Type: "trino", Host: "trino.internal.example", Port: 8080, Database: "iceberg"}
	folder := f.newLiveFolder(server)

	require.NoError(t, folder.RefreshAtStart(ctx, tenantList{f.tenantID}, time.Millisecond))

	file, _ := f.object("analytics.py_daily_kpis.odcs.yaml")
	assert.Contains(t, file, "  - {server: warehouse, type: trino, host: trino.internal.example, port: 8080, catalog: iceberg, schema: analytics}\n")
	raw, _ := f.object("_index.json")
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	assert.Equal(t, "rel-1", index["release_id"], "the release the watermark was recorded with")
	assert.Equal(t, fingerprintOf(server),
		f.value(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'contracts-live'}) RETURN m.settings_sha256 AS v`))
	assert.Equal(t, int64(41),
		f.value(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'contracts-live'}) RETURN m.promotion_seq AS v`))
}

// A promotion whose rewrite writes nothing leaves the stored fingerprint as
// it is, even on a catalog publishing with new settings: its files still
// carry the old ones, so the next start rewrites every file.
func TestLiveFolder_ASkippedRewriteKeepsTheOldSettingsForTheNextStart(t *testing.T) {
	f := newLiveFolderFixture(t, nil)
	ctx := context.Background()
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil,
		f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})))

	server := &contractversion.Server{Type: "trino", Host: "trino.internal.example", Port: 8080, Database: "iceberg"}
	restarted := handlers.NewReleasePromotedContractsHandler(
		f.uow, s3infra.NewContractBundleReader(f.mem), f.newLiveFolder(server), f.metrics, time.Minute, quietLogger())
	// A late promotion, below the folder's watermark, rewrites nothing.
	require.NoError(t, restarted.Handle(ctx, "40-0", nil,
		f.release("rel-0", 40, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})))
	live := `MATCH (m:Meta {tenant_id: $tenant_id, key: 'contracts-live'}) `
	assert.Equal(t, fingerprintOf(nil), f.value(live+`RETURN m.settings_sha256 AS v`), "the files still carry no servers entry")
	assert.Equal(t, "rel-1", f.value(live+`RETURN m.release_id AS v`))

	require.NoError(t, f.newLiveFolder(server).RefreshAtStart(ctx, tenantList{f.tenantID}, time.Millisecond))

	file, _ := f.object("analytics.py_daily_kpis.odcs.yaml")
	assert.Contains(t, file, "type: trino", "the next start rewrites every file")
	assert.Equal(t, fingerprintOf(server), f.value(live+`RETURN m.settings_sha256 AS v`))
}

// tenantList lists a fixed set of tenants.
type tenantList []string

func (l tenantList) Tenants(context.Context) ([]string, error) { return l, nil }
