//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	serializationinfra "github.com/carolsimone/continuo/catalog/adapters/serialization"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
	assert.Equal(t, "_index.json", f.livePuts()[0], "an interim index lists every file about to be put")
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
	assert.Equal(t, []string{"_index.json", "analytics.py_daily_kpis.odcs.yaml", "_index.json"}, f.livePuts()[5:],
		"an interim index marks the file about to be put, then the put, then the final index")
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

// A dbt contract is published from Neo4j through the production renderer like
// a Python one: its file carries the contract in ODCS's key order with the
// install's warehouse entry and the provenance of the model's own file, and an
// ephemeral model, whose contract is stored all the same, gets no file and no
// index entry.
func TestLiveFolder_APromotionPublishesADbtContractButNotAnEphemeralOne(t *testing.T) {
	server := &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}
	f := newLiveFolderFixture(t, server)
	const dbtID, pythonID, ephemeralID = "analytics.governed", "analytics.py_daily_kpis", "analytics.stg_governed"
	doc := f.sharedBundle("rel-1")
	doc.Contracts[ephemeralID] = ephemeralModel(t, doc.Contracts[dbtID], ephemeralID)

	require.NoError(t, f.handler.Handle(context.Background(), "41-0", nil, f.promote(doc, 41)))

	body, ok := f.object(dbtID + ".odcs.yaml")
	require.True(t, ok, "the dbt model's file is put")
	var published map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(body), &published))
	assert.Equal(t, dbtID, published["id"])
	assert.Equal(t, "1", published["version"], "the node's counter")
	assert.Equal(t, []any{map[string]any{
		"server": "warehouse", "type": "postgres", "host": "warehouse.internal.example", "port": 5432,
		"database": "continuo_dbt", "schema": "analytics",
	}}, published["servers"])
	properties, _ := published["customProperties"].([]any)
	require.NotEmpty(t, properties)
	provenance, _ := properties[len(properties)-1].(map[string]any)
	assert.Equal(t, "provenance", provenance["property"], "continuo's provenance ends the custom properties")
	assert.Equal(t, "models/tagged/governed.sql", provenance["value"].(map[string]any)["sourcePath"])
	keys := regexp.MustCompile(`(?m)^([A-Za-z]+):`).FindAllStringSubmatch(body, -1)
	var order []string
	for _, k := range keys {
		order = append(order, k[1])
	}
	assert.Equal(t, []string{"apiVersion", "kind", "id", "name", "version", "status", "servers",
		"team", "tags", "roles", "customProperties", "schema"}, order, "the root keys are in ODCS's order")

	_, ok = f.object(pythonID + ".odcs.yaml")
	assert.True(t, ok, "the Python node's file is put with it")
	_, ok = f.object(ephemeralID + ".odcs.yaml")
	assert.False(t, ok, "an ephemeral model creates no table, so it has no file")
	raw, _ := f.object("_index.json")
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	var listed []string
	for _, file := range index["files"].([]any) {
		listed = append(listed, file.(map[string]any)["unique_id"].(string))
	}
	assert.Equal(t, []string{dbtID, pythonID}, listed, "the index lists the files, not the ephemeral model")
	assert.Equal(t, int64(3), f.versions(), "the ephemeral model's contract is stored all the same")
	assert.Equal(t, int64(41), f.value(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: '`+ephemeralID+`'}) RETURN n.contract_promotion_seq AS v`))
}

// swapTopology applies the topology swap of promotion seq to the tenant's
// graph: one dbt model per entry of schemas, whose key is its unique id and
// whose value is the schema it is declared in.
func (f *liveFolderFixture) swapTopology(releaseID string, seq int64, schemas map[string]string) {
	f.t.Helper()
	var nodes []topology.ReleasePromotedTopologyNode
	for id, schema := range schemas {
		nodes = append(nodes, topology.ReleasePromotedTopologyNode{
			UniqueID: id, SchemaName: schema, TableName: id[strings.Index(id, ".")+1:], ServiceName: "svc",
			NodeType: "dbt-model", ContentHash: "sha256:" + id, ImageTag: "img", Schedule: "daily",
			UpstreamUniqueIDs: []string{},
		})
	}
	marker := uow.Marker{TenantID: f.tenantID, Group: streams.CatalogReleaseTopology, MessageID: uuid.NewString()}
	err := f.uow.Write(context.Background(), marker, true, func(ctx context.Context, tx uow.Tx) error {
		_, err := tx.Topology().PromoteRelease(ctx, f.tenantID, releaseID, seq, nodes, time.Now())
		return err
	})
	require.NoError(f.t, err)
}

// servers returns the servers entries of the published file of uniqueID.
func (f *liveFolderFixture) servers(uniqueID string) []any {
	f.t.Helper()
	body, ok := f.object(uniqueID + ".odcs.yaml")
	require.True(f.t, ok, "%s has a file", uniqueID)
	var published map[string]any
	require.NoError(f.t, yaml.Unmarshal([]byte(body), &published))
	assert.Equal(f.t, uniqueID, published["id"], "the logical id never takes the declared spelling")
	servers, _ := published["servers"].([]any)
	return servers
}

// serverSchema is the schema the servers entry of uniqueID's file names.
func (f *liveFolderFixture) serverSchema(uniqueID string) any {
	f.t.Helper()
	servers := f.servers(uniqueID)
	require.Len(f.t, servers, 1)
	return servers[0].(map[string]any)["schema"]
}

// A dbt model built in a quoted schema is declared in "Analytics" on its :Node
// while its contract id is lower case, and its file's servers entry names the
// declared spelling, the physical schema a consumer binds to. A node the
// contracts group created before any topology swap has no declared schema and
// falls back to the id's. The next promotion, whose topology swap changes only
// the case of the schema, puts the file again, though the contract is the same.
func TestLiveFolder_AServersEntryNamesTheNodesDeclaredSchema(t *testing.T) {
	server := &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}
	f := newLiveFolderFixture(t, server)
	const dbtID, pythonID = "analytics.governed", "analytics.py_daily_kpis"
	f.swapTopology("rel-1", 41, map[string]string{dbtID: "Analytics"})
	doc := f.sharedBundle("rel-1")

	require.NoError(t, f.handler.Handle(context.Background(), "41-0", nil, f.promote(doc, 41)))

	assert.Equal(t, "Analytics", f.serverSchema(dbtID), "the declared schema, not the id's lower case")
	assert.Equal(t, "analytics", f.serverSchema(pythonID), "a node with no declared schema falls back to the id's")
	raw, _ := f.object("_index.json")
	var index map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &index))
	assert.Equal(t, []any{
		map[string]any{"unique_id": dbtID, "key": dbtID + ".odcs.yaml", "version": float64(1),
			"contract_hash": doc.Contracts[dbtID].ContractHash, "physical_schema": "Analytics"},
		map[string]any{"unique_id": pythonID, "key": pythonID + ".odcs.yaml", "version": float64(1),
			"contract_hash": doc.Contracts[pythonID].ContractHash},
	}, index["files"], "the index keeps a spelling only where it differs from the id's")

	f.swapTopology("rel-2", 42, map[string]string{dbtID: "analytics"})
	before := len(f.livePuts())
	doc.ReleaseID = "rel-2"
	require.NoError(t, f.handler.Handle(context.Background(), "42-0", nil, f.promote(doc, 42)))

	assert.Equal(t, "analytics", f.serverSchema(dbtID))
	assert.Equal(t, []string{"_index.json", dbtID + ".odcs.yaml", "_index.json"}, f.livePuts()[before:],
		"only the file whose schema changed is put, between the interim and the final index")
}

// ephemeralModel is the contract of the ephemeral dbt model uniqueID, written
// the way dbt.generate writes a model built as ephemeral: the dbt contract of
// model with its table, schema object and materialization renamed and its
// physicalType `ephemeral`.
func ephemeralModel(t *testing.T, model contractbundle.Contract, uniqueID string) contractbundle.Contract {
	t.Helper()
	table := uniqueID[strings.Index(uniqueID, ".")+1:]
	document := decodedDocument(t, model.Document)
	document["id"], document["name"] = uniqueID, table
	schema, _ := document["schema"].([]any)
	require.NotEmpty(t, schema)
	object, _ := schema[0].(map[string]any)
	object["name"], object["physicalName"], object["physicalType"] = table, table, "ephemeral"
	for _, p := range document["customProperties"].([]any) {
		if prop := p.(map[string]any); prop["property"] == "materialization" {
			prop["value"] = map[string]any{"materialized": "ephemeral"}
		}
	}
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	model.Document = raw
	model.ContractHash = "sha256:ephemeral-" + table
	model.ContentHash = "sha256:code-" + table
	model.Source = contractbundle.Source{Path: "models/" + table + ".sql", SHA256: "sha256:src-" + table}
	return model
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
