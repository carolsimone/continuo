package handlers_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/queries"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: queries.ContractReader ────────────────────────────────────────────

// fakeContractReader answers Live with live, whatever the seq, recording
// each seq it is asked for, and LatestPromotionSeq with latest.
type fakeContractReader struct {
	live   []contractversion.LiveContract
	latest int64
	asked  []int64
}

func (f *fakeContractReader) Current(context.Context, string, string) (contractversion.VersionView, error) {
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (f *fakeContractReader) History(context.Context, string, string, int) ([]contractversion.VersionView, error) {
	return nil, nil
}

func (f *fakeContractReader) ByVersion(context.Context, string, string, int64) (contractversion.VersionView, error) {
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (f *fakeContractReader) Live(_ context.Context, _ string, seq int64) ([]contractversion.LiveContract, error) {
	f.asked = append(f.asked, seq)
	return slices.Clone(f.live), nil
}

func (f *fakeContractReader) LatestPromotionSeq(context.Context, string) (int64, error) {
	return f.latest, nil
}

var _ queries.ContractReader = (*fakeContractReader)(nil)

// ── fakes: ports.LiveContractsStore ──────────────────────────────────────────

// fakeLiveStore keeps one tenant's folder in memory and records each write as
// "put <id>", "delete <id>" or "index", in the order it reached the store.
// failPut fails the put of the ids it names.
type fakeLiveStore struct {
	mu         sync.Mutex
	files      map[string][]byte
	index      *ports.LiveIndex
	readErr    error
	ops        []string
	failPut    map[string]error
	failDelete map[string]error
}

func newFakeLiveStore() *fakeLiveStore {
	return &fakeLiveStore{files: map[string][]byte{}, failPut: map[string]error{}, failDelete: map[string]error{}}
}

func (f *fakeLiveStore) Put(_ context.Context, _, uniqueID string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failPut[uniqueID]; err != nil {
		return err
	}
	f.files[uniqueID] = body
	f.ops = append(f.ops, "put "+uniqueID)
	return nil
}

func (f *fakeLiveStore) Delete(_ context.Context, _, uniqueID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failDelete[uniqueID]; err != nil {
		return err
	}
	delete(f.files, uniqueID)
	f.ops = append(f.ops, "delete "+uniqueID)
	return nil
}

func (f *fakeLiveStore) ReadIndex(context.Context, string) (ports.LiveIndex, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return ports.LiveIndex{}, false, f.readErr
	}
	if f.index == nil {
		return ports.LiveIndex{}, false, nil
	}
	return *f.index, true, nil
}

func (f *fakeLiveStore) PutIndex(_ context.Context, _ string, idx ports.LiveIndex) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index = &idx
	f.ops = append(f.ops, "index")
	return nil
}

// writes returns the writes recorded so far.
func (f *fakeLiveStore) writes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

var _ ports.LiveContractsStore = (*fakeLiveStore)(nil)

// ── fakes: ports.PublishedRenderer ───────────────────────────────────────────

// fakeRenderer renders a published contract as one line naming its id,
// version and whether it carries a servers entry.
type fakeRenderer struct{}

func (fakeRenderer) Render(doc map[string]any) ([]byte, error) {
	_, servers := doc["servers"]
	return fmt.Appendf(nil, "%v v%v servers=%t", doc["id"], doc["version"], servers), nil
}

var _ ports.PublishedRenderer = fakeRenderer{}

// schemaRenderer renders a published contract as one line naming its id and
// the schema of its servers entry, "-" when it has no entry or its entry names
// no schema.
type schemaRenderer struct{}

func (schemaRenderer) Render(doc map[string]any) ([]byte, error) {
	schema := "-"
	if servers, _ := doc["servers"].([]any); len(servers) > 0 {
		entry, _ := servers[0].(map[string]any)
		if s, ok := entry["schema"].(string); ok {
			schema = s
		}
	}
	return fmt.Appendf(nil, "%v schema=%s", doc["id"], schema), nil
}

var _ ports.PublishedRenderer = schemaRenderer{}

// ── fakes: ports.LiveFolderLease and ports.LiveFolderState ───────────────────

// fakeLiveLease holds one tenant's live-folder lease and state. heldElsewhere
// makes every Acquire fail as if another rewrite held the lease.
type fakeLiveLease struct {
	mu            sync.Mutex
	heldElsewhere bool
	holder        string
	acquires      int
	releases      int
	seq           int64
	release       string
	settings      string
	settingsSaved []string
}

func (f *fakeLiveLease) Acquire(_ context.Context, _, holder string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquires++
	if f.heldElsewhere || f.holder != "" {
		return false, nil
	}
	f.holder = holder
	return true, nil
}

func (f *fakeLiveLease) Renew(_ context.Context, _, holder string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder != holder {
		return ports.ErrLeaseLost
	}
	return nil
}

func (f *fakeLiveLease) Release(_ context.Context, _, holder string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder == holder {
		f.holder = ""
		f.releases++
	}
	return nil
}

func (f *fakeLiveLease) Read(context.Context, string) (ports.LiveFolderMark, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return ports.LiveFolderMark{PromotionSeq: f.seq, ReleaseID: f.release, SettingsSHA256: f.settings}, nil
}

func (f *fakeLiveLease) RecordSettings(_ context.Context, _, settings string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settings = settings
	f.settingsSaved = append(f.settingsSaved, settings)
	return nil
}

var (
	_ ports.LiveFolderLease = (*fakeLiveLease)(nil)
	_ ports.LiveFolderState = (*fakeLiveLease)(nil)
)

// ── fakes: ports.GraphTenants ────────────────────────────────────────────────

type fakeTenants []string

func (f fakeTenants) Tenants(context.Context) ([]string, error) { return f, nil }

var _ ports.GraphTenants = fakeTenants(nil)

// ── fixtures ─────────────────────────────────────────────────────────────────

var liveClock = time.Date(2026, 11, 27, 16, 40, 14, 0, time.UTC)

// liveServer is the install's warehouse server.
var liveServer = &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}

// liveContract is uniqueID's current contract at version, with hash.
func liveContract(uniqueID string, version int64, hash string) contractversion.LiveContract {
	return contractversion.LiveContract{
		UniqueID:     uniqueID,
		Version:      version,
		ContractHash: hash,
		Document: map[string]any{
			"id": uniqueID, "version": "0",
			"schema": []any{map[string]any{"name": uniqueID, "physicalType": "table"}},
		},
		Provenance: contractversion.Provenance{ContractHash: hash, ReleaseID: "rel-1", PromotionSeq: 3},
	}
}

// dbtContract is uniqueID's current contract, a dbt node of kind (dbt-model,
// dbt-seed or dbt-snapshot) built as materialized, in the form dbt.generate
// stores: one schema object whose physicalType is "view" or "ephemeral" for
// those two materializations and "table" for every other, the node's kind and
// materialization as continuo custom properties, and the model's file as its
// source.
func dbtContract(uniqueID string, version int64, hash, kind, materialized string) contractversion.LiveContract {
	table := uniqueID[strings.Index(uniqueID, ".")+1:]
	physicalType := "table"
	if materialized == "view" || materialized == "ephemeral" {
		physicalType = materialized
	}
	path := "models/" + table + ".sql"
	return contractversion.LiveContract{
		UniqueID:     uniqueID,
		Version:      version,
		ContractHash: hash,
		Document: map[string]any{
			"apiVersion": "v3.2.0", "kind": "DataContract", "id": uniqueID, "name": table,
			"version": "0", "status": "active",
			"team": map[string]any{"name": "team-a"},
			"tags": []any{"daily"},
			"customProperties": []any{
				map[string]any{"vendor": "continuo", "property": "kind", "value": kind},
				map[string]any{"vendor": "continuo", "property": "filePath", "value": path},
				map[string]any{"vendor": "continuo", "property": "materialization", "value": map[string]any{"materialized": materialized}},
			},
			"schema": []any{map[string]any{
				"name": table, "physicalName": table, "physicalType": physicalType,
				"properties": []any{map[string]any{"name": "id", "logicalType": "integer", "physicalType": "integer"}},
			}},
		},
		Provenance: contractversion.Provenance{ContractHash: hash, ReleaseID: "rel-1", PromotionSeq: 3, SourcePath: path},
	}
}

// declaredIn is c as the contract of a node whose declared schema is schema.
func declaredIn(c contractversion.LiveContract, schema string) contractversion.LiveContract {
	c.PhysicalSchema = schema
	return c
}

// ephemeralContract is uniqueID's contract for an ephemeral model.
func ephemeralContract(uniqueID string) contractversion.LiveContract {
	c := liveContract(uniqueID, 1, "sha256:eph")
	c.Document["schema"] = []any{map[string]any{"name": uniqueID, "physicalType": "ephemeral"}}
	return c
}

// fingerprintOf is the settings fingerprint a LiveFolder publishing with
// server stores: the hex SHA-256 of the server's canonical text, nil counting
// as a server with no host.
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

// liveFolderFixture is a LiveFolder over in-memory fakes, publishing with
// liveServer, whose fingerprint is settings. reported lists what each
// rewrite returned.
type liveFolderFixture struct {
	reader   *fakeContractReader
	store    *fakeLiveStore
	lease    *fakeLiveLease
	metrics  *fakeContractMetrics
	folder   *handlers.LiveFolder
	settings string
	reported []string
}

// rewrite runs the folder's Rewrite for promotion seq, recording the
// settings it reports.
func (f *liveFolderFixture) rewrite(seq int64) error {
	settings, err := f.folder.Rewrite(context.Background(), livePromotion(seq))
	f.reported = append(f.reported, settings)
	return err
}

// folderWith replaces the fixture's folder with one over the same fakes that
// renders with renderer and publishes with server.
func (f *liveFolderFixture) folderWith(renderer ports.PublishedRenderer, server *contractversion.Server) {
	f.settings = fingerprintOf(server)
	f.folder = handlers.NewLiveFolder(f.reader, f.store, renderer, f.lease, f.lease, f.metrics,
		server, func() time.Time { return liveClock }, newTestLogger())
}

func newLiveFolderFixture(live ...contractversion.LiveContract) *liveFolderFixture {
	f := &liveFolderFixture{
		reader:   &fakeContractReader{live: live},
		store:    newFakeLiveStore(),
		lease:    &fakeLiveLease{},
		metrics:  &fakeContractMetrics{},
		settings: fingerprintOf(liveServer),
	}
	f.folder = handlers.NewLiveFolder(f.reader, f.store, fakeRenderer{}, f.lease, f.lease, f.metrics,
		liveServer, func() time.Time { return liveClock }, newTestLogger())
	return f
}

// listed sets the folder's index: written for promotion seq with the
// fixture's settings, listing files.
func (f *liveFolderFixture) listed(seq int64, files ...ports.LiveIndexFile) {
	f.store.index = &ports.LiveIndex{
		SchemaVersion: ports.LiveIndexSchemaVersion, TenantID: "default", PromotionSeq: seq, ReleaseID: "rel-0",
		WrittenAt: liveClock.Add(-time.Hour), SettingsSHA256: f.settings, Files: files,
	}
	for _, file := range files {
		f.store.files[file.UniqueID] = []byte("old")
	}
}

func livePromotion(seq int64) domainModel.PromoteReleaseInput {
	in := contractsInput()
	in.PromotionSeq = seq
	in.ReleaseID = fmt.Sprintf("rel-%d", seq)
	return in
}

// ── tests ────────────────────────────────────────────────────────────────────

// Of two live contracts only the changed one is put, then the index, which
// lists both, sorted by unique id, with the promotion and the settings.
func TestLiveRewrite_PutsOnlyChangedFilesThenTheIndex(t *testing.T) {
	f := newLiveFolderFixture(
		liveContract("analytics.b", 1, "sha256:b1"),
		liveContract("analytics.a", 2, "sha256:a2"),
	)
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.b", Version: 1, ContractHash: "sha256:b1"},
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []int64{3}, f.reader.asked, "the live set is the promotion's nodes")
	assert.Equal(t, []string{"index", "put analytics.a", "index"}, f.store.writes(),
		"an interim index, the put, then the final index")
	assert.Equal(t, "analytics.a v2 servers=true", string(f.store.files["analytics.a"]),
		"the file is the published form: the counter and the install's server")
	assert.Equal(t, ports.LiveIndex{
		SchemaVersion:  ports.LiveIndexSchemaVersion,
		TenantID:       "default",
		PromotionSeq:   3,
		ReleaseID:      "rel-3",
		WrittenAt:      liveClock,
		SettingsSHA256: f.settings,
		Files: []ports.LiveIndexFile{
			{UniqueID: "analytics.a", Version: 2, ContractHash: "sha256:a2"},
			{UniqueID: "analytics.b", Version: 1, ContractHash: "sha256:b1"},
		},
	}, *f.store.index)
	assert.Equal(t, [][2]string{
		{ports.LiveWriteIndex, ports.LiveWriteOK}, {ports.LiveWritePut, ports.LiveWriteOK}, {ports.LiveWriteIndex, ports.LiveWriteOK},
	}, f.metrics.writes())
	assert.Equal(t, 1, f.lease.releases, "the lease is released")
}

func TestLiveRewrite_DeletesTheFileOfANodeNoLongerLive(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.retired", Version: 4, ContractHash: "sha256:r4"},
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"index", "delete analytics.retired", "index"}, f.store.writes(),
		"an interim index lists the file before it is deleted")
	assert.NotContains(t, f.store.files, "analytics.retired")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"}}, f.store.index.Files)
	assert.Equal(t, [][2]string{
		{ports.LiveWriteIndex, ports.LiveWriteOK}, {ports.LiveWriteDelete, ports.LiveWriteOK}, {ports.LiveWriteIndex, ports.LiveWriteOK},
	}, f.metrics.writes())
}

// An ephemeral model gets no file and no index entry; one whose file was
// published before it became ephemeral loses it.
func TestLiveRewrite_SkipsAnEphemeralContract(t *testing.T) {
	f := newLiveFolderFixture(
		liveContract("analytics.table", 1, "sha256:t1"),
		ephemeralContract("analytics.eph"),
		ephemeralContract("analytics.was_table"),
	)
	f.listed(2, ports.LiveIndexFile{UniqueID: "analytics.was_table", Version: 1, ContractHash: "sha256:w1"})

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"index", "put analytics.table", "delete analytics.was_table", "index"}, f.store.writes(),
		"an interim index lists the new file before it is put")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.table", Version: 1, ContractHash: "sha256:t1"}}, f.store.index.Files)
}

// A promotion's dbt contracts are published like any other: a table model, a
// view, a seed and a snapshot each get their file and their index entry, and an
// ephemeral model, which creates no table, gets neither.
func TestLiveRewrite_WritesADbtContractButNotAnEphemeralOne(t *testing.T) {
	f := newLiveFolderFixture(
		dbtContract("analytics.orders", 2, "sha256:o2", "dbt-model", "table"),
		dbtContract("analytics.orders_view", 1, "sha256:v1", "dbt-model", "view"),
		dbtContract("analytics.countries", 1, "sha256:c1", "dbt-seed", "seed"),
		dbtContract("analytics.orders_snapshot", 1, "sha256:s1", "dbt-snapshot", "snapshot"),
		dbtContract("analytics.stg_orders", 1, "sha256:e1", "dbt-model", "ephemeral"),
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"analytics.countries", "analytics.orders", "analytics.orders_snapshot", "analytics.orders_view"},
		slices.Sorted(maps.Keys(f.store.files)), "every dbt contract but the ephemeral model's has its file")
	assert.Equal(t, "analytics.orders v2 servers=true", string(f.store.files["analytics.orders"]),
		"a dbt contract is published with the counter and the install's server, as any contract is")
	assert.Equal(t, []ports.LiveIndexFile{
		{UniqueID: "analytics.countries", Version: 1, ContractHash: "sha256:c1"},
		{UniqueID: "analytics.orders", Version: 2, ContractHash: "sha256:o2"},
		{UniqueID: "analytics.orders_snapshot", Version: 1, ContractHash: "sha256:s1"},
		{UniqueID: "analytics.orders_view", Version: 1, ContractHash: "sha256:v1"},
	}, f.store.index.Files, "the index lists the files, sorted, and not the ephemeral model")
	assert.NotContains(t, f.store.writes(), "put analytics.stg_orders")
}

// A file's servers entry names the schema the node is declared in, the one
// continuo and dbt build in, and not the schema part of the contract id, which
// is lower case. The index keeps the spelling only where it differs from the
// id's schema, so a node declared in a lower-case schema, or in none the graph
// knows yet, has the entry it always had.
func TestLiveRewrite_AServersEntryNamesTheNodesDeclaredSchema(t *testing.T) {
	f := newLiveFolderFixture(
		declaredIn(liveContract("analytics.quoted", 1, "sha256:q1"), "Analytics"),
		declaredIn(liveContract("analytics.lower", 1, "sha256:l1"), "analytics"),
		liveContract("analytics.unknown", 1, "sha256:u1"),
	)
	f.folderWith(schemaRenderer{}, liveServer)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, "analytics.quoted schema=Analytics", string(f.store.files["analytics.quoted"]),
		"the entry names the declared spelling and the id keeps its own")
	assert.Equal(t, "analytics.lower schema=analytics", string(f.store.files["analytics.lower"]))
	assert.Equal(t, "analytics.unknown schema=analytics", string(f.store.files["analytics.unknown"]),
		"a node with no declared schema names the id's schema")
	assert.Equal(t, []ports.LiveIndexFile{
		{UniqueID: "analytics.lower", Version: 1, ContractHash: "sha256:l1"},
		{UniqueID: "analytics.quoted", Version: 1, ContractHash: "sha256:q1", PhysicalSchema: "Analytics"},
		{UniqueID: "analytics.unknown", Version: 1, ContractHash: "sha256:u1"},
	}, f.store.index.Files)
}

// A change of the declared schema's case alone leaves the contract's version
// and hash as they are and changes the file, so the file is put again; an
// unchanged declared schema puts nothing.
func TestLiveRewrite_ACaseOnlyChangeOfTheDeclaredSchemaPutsTheFileAgain(t *testing.T) {
	const id = "analytics.orders"
	f := newLiveFolderFixture(declaredIn(liveContract(id, 1, "sha256:o1"), "analytics"))
	f.folderWith(schemaRenderer{}, liveServer)
	require.NoError(t, f.rewrite(3))
	require.Equal(t, id+" schema=analytics", string(f.store.files[id]))

	f.reader.live = []contractversion.LiveContract{declaredIn(liveContract(id, 1, "sha256:o1"), "Analytics")}
	before := len(f.store.writes())
	require.NoError(t, f.rewrite(4))

	assert.Equal(t, []string{"index", "put " + id, "index"}, f.store.writes()[before:],
		"the file is put between an interim and the final index")
	assert.Equal(t, id+" schema=Analytics", string(f.store.files[id]))
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: id, Version: 1, ContractHash: "sha256:o1", PhysicalSchema: "Analytics"}}, f.store.index.Files)

	before = len(f.store.writes())
	require.NoError(t, f.rewrite(5))
	assert.Equal(t, []string{"index"}, f.store.writes()[before:], "the same declared schema puts no file")

	f.reader.live = []contractversion.LiveContract{declaredIn(liveContract(id, 1, "sha256:o1"), "analytics")}
	before = len(f.store.writes())
	require.NoError(t, f.rewrite(6))
	assert.Equal(t, []string{"index", "put " + id, "index"}, f.store.writes()[before:], "the case changing back puts it again")
	assert.Equal(t, id+" schema=analytics", string(f.store.files[id]))
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: id, Version: 1, ContractHash: "sha256:o1"}}, f.store.index.Files)
}

// A node the contracts group created before the topology swap has no declared
// schema, and the swap that gives it one never rewrites the folder: the next
// promotion's rewrite puts the file whose entry now names a different schema.
func TestLiveRewrite_ADeclaredSchemaThatArrivesAfterTheFirstRewriteIsPublishedByTheNext(t *testing.T) {
	const id = "analytics.orders"
	f := newLiveFolderFixture(liveContract(id, 1, "sha256:o1"))
	f.folderWith(schemaRenderer{}, liveServer)
	require.NoError(t, f.rewrite(3))
	require.Equal(t, id+" schema=analytics", string(f.store.files[id]))

	f.reader.live = []contractversion.LiveContract{declaredIn(liveContract(id, 1, "sha256:o1"), "Analytics")}
	require.NoError(t, f.rewrite(4))

	assert.Equal(t, id+" schema=Analytics", string(f.store.files[id]))
}

// An index whose entries carry no physical schema was written for files whose
// servers entry names the id's schema. Such an entry is current for a node
// whose declared schema agrees with its id's or is not known, so the first
// rewrite after an upgrade puts none of those files, and stale for a node
// declared in a schema that differs, so that rewrite puts its file.
func TestLiveRewrite_AnIndexEntryWithNoPhysicalSchemaIsCurrentForALowerCaseSchema(t *testing.T) {
	f := newLiveFolderFixture(
		declaredIn(liveContract("analytics.lower", 1, "sha256:l1"), "analytics"),
		liveContract("analytics.unknown", 1, "sha256:u1"),
		declaredIn(liveContract("analytics.quoted", 1, "sha256:q1"), "Analytics"),
	)
	f.folderWith(schemaRenderer{}, liveServer)
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.lower", Version: 1, ContractHash: "sha256:l1"},
		ports.LiveIndexFile{UniqueID: "analytics.quoted", Version: 1, ContractHash: "sha256:q1"},
		ports.LiveIndexFile{UniqueID: "analytics.unknown", Version: 1, ContractHash: "sha256:u1"},
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"index", "put analytics.quoted", "index"}, f.store.writes(),
		"only the file that named the wrong schema is put")
	assert.Equal(t, "analytics.quoted schema=Analytics", string(f.store.files["analytics.quoted"]))
	assert.Equal(t, "old", string(f.store.files["analytics.lower"]))
	assert.Equal(t, "old", string(f.store.files["analytics.unknown"]))

	f.store.ops = nil
	require.NoError(t, f.rewrite(4))
	assert.Equal(t, []string{"index"}, f.store.writes(), "the next rewrite finds every file current")
}

// Without a servers entry the declared schema shows in no file, so changing it
// puts nothing and the index keeps no spelling.
func TestLiveRewrite_TheDeclaredSchemaOfAFileWithNoServersEntryIsNotKept(t *testing.T) {
	const id = "analytics.orders"
	f := newLiveFolderFixture(declaredIn(liveContract(id, 1, "sha256:o1"), "analytics"))
	f.folderWith(schemaRenderer{}, nil)
	require.NoError(t, f.rewrite(3))
	require.Equal(t, id+" schema=-", string(f.store.files[id]))

	f.reader.live = []contractversion.LiveContract{declaredIn(liveContract(id, 1, "sha256:o1"), "Analytics")}
	before := len(f.store.writes())
	require.NoError(t, f.rewrite(4))

	assert.Equal(t, []string{"index"}, f.store.writes()[before:])
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: id, Version: 1, ContractHash: "sha256:o1"}}, f.store.index.Files)
}

// A rewrite of every file, for changed settings, names each file's declared
// schema, the index keeping the spelling of the files that differ.
func TestLiveRewrite_ChangedSettingsPutEveryFileWithItsDeclaredSchema(t *testing.T) {
	f := newLiveFolderFixture(
		declaredIn(liveContract("analytics.quoted", 1, "sha256:q1"), "Analytics"),
		liveContract("analytics.plain", 1, "sha256:p1"),
	)
	f.folderWith(schemaRenderer{}, liveServer)
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.plain", Version: 1, ContractHash: "sha256:p1"},
		ports.LiveIndexFile{UniqueID: "analytics.quoted", Version: 1, ContractHash: "sha256:q1", PhysicalSchema: "Analytics"},
	)
	f.store.index.SettingsSHA256 = "settings-of-another-install"

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, "analytics.quoted schema=Analytics", string(f.store.files["analytics.quoted"]))
	assert.Equal(t, "analytics.plain schema=analytics", string(f.store.files["analytics.plain"]))
	assert.Equal(t, "Analytics", f.store.index.Files[1].PhysicalSchema)
	assert.Empty(t, f.store.index.Files[0].PhysicalSchema)
}

// A promotion at or below the folder's watermark rewrites nothing and takes
// no lease: a newer rewrite already finished.
func TestLiveRewrite_APromotionAtOrBelowTheWatermarkWritesNothing(t *testing.T) {
	for _, seq := range []int64{52, 40} {
		f := newLiveFolderFixture(liveContract("analytics.a", 9, "sha256:a9"))
		f.lease.seq = 52

		require.NoError(t, f.rewrite(seq), "seq %d", seq)

		assert.Empty(t, f.store.writes(), "seq %d", seq)
		assert.Zero(t, f.lease.acquires, "seq %d", seq)
	}
}

// A folder whose index already holds this promotion, written with the same
// settings, was finished by an earlier delivery whose transaction B never
// committed: nothing is written again.
func TestLiveRewrite_AFolderAlreadyWrittenForThePromotionWritesNothing(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 2, "sha256:a2"))
	f.listed(3, ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"})

	require.NoError(t, f.rewrite(3))

	assert.Empty(t, f.store.writes())
	assert.Equal(t, 1, f.lease.releases)
}

// A node the promotion added gets its file in the same rewrite, and a node it
// dropped loses its file in the same rewrite.
func TestLiveRewrite_ANewNodeGetsItsFileAndADroppedNodeLosesItInTheSamePromotion(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.new", 1, "sha256:n1"))
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.dropped", Version: 2, ContractHash: "sha256:d2"},
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"index", "put analytics.new", "delete analytics.dropped", "index"}, f.store.writes())
	assert.Equal(t, []ports.LiveIndexFile{
		{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		{UniqueID: "analytics.new", Version: 1, ContractHash: "sha256:n1"},
	}, f.store.index.Files)
}

// A promotion whose bundle names no node empties the folder: every file is
// deleted and the index lists none.
func TestLiveRewrite_AnEmptyPromotionDeletesEveryFile(t *testing.T) {
	f := newLiveFolderFixture()
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.b", Version: 1, ContractHash: "sha256:b1"},
	)

	require.NoError(t, f.rewrite(3))

	writes := f.store.writes()
	require.Len(t, writes, 4)
	assert.Equal(t, "index", writes[0], "the interim index")
	assert.ElementsMatch(t, []string{"delete analytics.a", "delete analytics.b"}, writes[1:3], "deletes run at once, in any order")
	assert.Equal(t, "index", writes[3])
	assert.Empty(t, f.store.index.Files)
	assert.Empty(t, f.store.files)
}

// A promotion older than the newest one whose contracts are recorded writes
// nothing, even when the folder's watermark is behind both: the newer
// promotion's rewrite, redelivered, writes the folder, and the older set
// would put superseded files back or delete current ones.
func TestLiveRewrite_ANewerPromotionsContractsWriteNothing(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.reader.latest = 5

	require.NoError(t, f.rewrite(3))

	assert.Empty(t, f.store.writes())
	assert.Empty(t, f.reader.asked)
	assert.Equal(t, 1, f.lease.releases)
}

// While another rewrite holds the lease the rewrite writes nothing and fails
// with an error the consumer retries.
func TestLiveRewrite_ALeaseHeldElsewhereRetries(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.lease.heldElsewhere = true

	err := f.rewrite(3)

	require.ErrorIs(t, err, handlers.ErrLiveFolderBusy)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, f.store.writes())
	assert.Zero(t, f.lease.releases, "a lease never taken is not released")
}

// An index written with other settings rewrites every file.
func TestLiveRewrite_ChangedSettingsPutEveryFile(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.b", 1, "sha256:b1"))
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.b", Version: 1, ContractHash: "sha256:b1"},
	)
	f.store.index.SettingsSHA256 = "settings-of-another-install"

	require.NoError(t, f.rewrite(3))

	writes := f.store.writes()
	require.Len(t, writes, 4)
	assert.Equal(t, "index", writes[0])
	assert.ElementsMatch(t, []string{"put analytics.a", "put analytics.b"}, writes[1:3])
	assert.Equal(t, "index", writes[3])
	assert.Equal(t, f.settings, f.store.index.SettingsSHA256)
}

// A put object storage refuses fails the rewrite before the final index and
// is counted failed; the interim index written before it lists the file, so
// the next rewrite knows of it. The lease is released.
func TestLiveRewrite_AFailedPutLeavesTheInterimIndex(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 2, "sha256:a2"))
	outage := errors.New("503 slow down")
	f.store.failPut["analytics.a"] = outage

	err := f.rewrite(3)

	require.ErrorIs(t, err, outage)
	assert.Equal(t, []string{"index"}, f.store.writes())
	assert.Equal(t, ports.LiveIndex{
		SchemaVersion: ports.LiveIndexSchemaVersion, TenantID: "default", PromotionSeq: 3, ReleaseID: "rel-3",
		WrittenAt: liveClock, Files: []ports.LiveIndexFile{{UniqueID: "analytics.a"}},
	}, *f.store.index, "the promotion being written, no settings, the new file at version 0")
	assert.Equal(t, [][2]string{{ports.LiveWriteIndex, ports.LiveWriteOK}, {ports.LiveWritePut, ports.LiveWriteFailed}}, f.metrics.writes())
	assert.Equal(t, 1, f.lease.releases)
	assert.Equal(t, []string{""}, f.reported)
}

// A file put by a rewrite that then failed is never orphaned: the next
// rewrite, for a promotion that no longer publishes the node, deletes it,
// because the interim index lists it. The interim index keeps the previous
// promotion and settings, so the next rewrite still puts what changed.
func TestLiveRewrite_AFileOfAFailedRewriteIsDeletedByTheNextOne(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.x", 1, "sha256:x1"))
	f.listed(52,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.z", Version: 1, ContractHash: "sha256:z1"},
	)
	f.store.failDelete["analytics.z"] = errors.New("503 slow down")

	require.Error(t, f.rewrite(53))
	require.Contains(t, f.store.files, "analytics.x", "promotion 53 put x's file, then failed")
	assert.Equal(t, int64(52), f.store.index.PromotionSeq, "the interim index names the last finished rewrite")
	assert.Equal(t, "rel-0", f.store.index.ReleaseID)
	assert.Equal(t, f.settings, f.store.index.SettingsSHA256)
	assert.Equal(t, []ports.LiveIndexFile{
		{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		{UniqueID: "analytics.x"},
		{UniqueID: "analytics.z"},
	}, f.store.index.Files, "the file put and the file about to be deleted, each at version 0")

	// Promotion 54 drops x.
	delete(f.store.failDelete, "analytics.z")
	f.reader.live = []contractversion.LiveContract{liveContract("analytics.a", 1, "sha256:a1")}
	require.NoError(t, f.rewrite(54))

	assert.NotContains(t, f.store.files, "analytics.x", "x's file is deleted")
	assert.NotContains(t, f.store.files, "analytics.z")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"}}, f.store.index.Files)
}

// A rewrite that overwrote a listed file and then failed leaves that file
// marked at version 0, so a later promotion that brings the node back to the
// version the index listed before still puts it again.
func TestLiveRewrite_ARollbackAfterAFailedOverwritePutsTheFileAgain(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.y", 2, "sha256:y2"))
	f.listed(52,
		ports.LiveIndexFile{UniqueID: "analytics.y", Version: 1, ContractHash: "sha256:y1"},
		ports.LiveIndexFile{UniqueID: "analytics.z", Version: 1, ContractHash: "sha256:z1"},
	)
	f.store.failDelete["analytics.z"] = errors.New("503 slow down")

	require.Error(t, f.rewrite(53))
	require.Equal(t, "analytics.y v2 servers=true", string(f.store.files["analytics.y"]), "53 overwrote y at v2, then failed")
	assert.Equal(t, []ports.LiveIndexFile{
		{UniqueID: "analytics.y"},
		{UniqueID: "analytics.z"},
	}, f.store.index.Files, "the overwritten file and the file about to be deleted are marked at version 0")

	// Promotion 54 brings y back to the contract the index listed at v1.
	delete(f.store.failDelete, "analytics.z")
	f.reader.live = []contractversion.LiveContract{liveContract("analytics.y", 1, "sha256:y1")}
	require.NoError(t, f.rewrite(54))

	assert.Equal(t, "analytics.y v1 servers=true", string(f.store.files["analytics.y"]), "v2's file is not left in place")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.y", Version: 1, ContractHash: "sha256:y1"}}, f.store.index.Files)
}

// A rewrite of every file under new settings that failed leaves every file it
// was about to put marked at version 0, so a later rewrite under the old
// settings, which the interim index still names, puts each of them again.
func TestLiveRewrite_ASettingsFlipAfterAFailedRewritePutsEveryFileAgain(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.b", 1, "sha256:b1"))
	oldSettings := fingerprintOf(nil)
	f.listed(52,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.b", Version: 1, ContractHash: "sha256:b1"},
		ports.LiveIndexFile{UniqueID: "analytics.z", Version: 1, ContractHash: "sha256:z1"},
	)
	f.store.index.SettingsSHA256 = oldSettings
	f.store.failDelete["analytics.z"] = errors.New("503 slow down")

	require.Error(t, f.rewrite(53), "the rewrite under liveServer puts every file, then fails")
	require.Equal(t, "analytics.a v1 servers=true", string(f.store.files["analytics.a"]))
	assert.Equal(t, oldSettings, f.store.index.SettingsSHA256, "the interim index keeps the old settings")

	// Catalog goes back to publishing with no server.
	delete(f.store.failDelete, "analytics.z")
	f.folder = handlers.NewLiveFolder(f.reader, f.store, fakeRenderer{}, f.lease, f.lease, f.metrics,
		nil, func() time.Time { return liveClock }, newTestLogger())
	require.NoError(t, f.rewrite(54))

	assert.Equal(t, "analytics.a v1 servers=false", string(f.store.files["analytics.a"]))
	assert.Equal(t, "analytics.b v1 servers=false", string(f.store.files["analytics.b"]))
	assert.Equal(t, oldSettings, f.store.index.SettingsSHA256)
}

// A delete that ran before a rewrite failed is never forgotten: the interim
// index lists the deleted file at version 0, so a later promotion that brings
// the node back unchanged puts its file again instead of trusting an index
// entry whose object is gone.
func TestLiveRewrite_ANodeBroughtBackAfterAFailedDeleteIsPutAgain(t *testing.T) {
	f := newLiveFolderFixture()
	f.lease.seq = 5
	f.listed(5,
		ports.LiveIndexFile{UniqueID: "analytics.x", Version: 1, ContractHash: "sha256:x"},
		ports.LiveIndexFile{UniqueID: "analytics.y", Version: 1, ContractHash: "sha256:y"},
	)
	f.store.failDelete["analytics.y"] = errors.New("503 slow down")

	// Promotion 6 drops both nodes; y's delete fails after x's file is gone.
	require.Error(t, f.rewrite(6))
	require.NotContains(t, f.store.files, "analytics.x", "the failed rewrite deleted x's file")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.x"}, {UniqueID: "analytics.y"}}, f.store.index.Files,
		"the interim index lists both files about to be deleted at version 0")

	// Promotion 7, handled before 6's redelivery, brings x back unchanged.
	delete(f.store.failDelete, "analytics.y")
	f.reader.live = []contractversion.LiveContract{liveContract("analytics.x", 1, "sha256:x")}
	require.NoError(t, f.rewrite(7))

	assert.Equal(t, "analytics.x v1 servers=true", string(f.store.files["analytics.x"]), "x is live again, so its file exists")
	assert.NotContains(t, f.store.files, "analytics.y")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.x", Version: 1, ContractHash: "sha256:x"}}, f.store.index.Files)
}

// An index the store cannot read (corrupt, or of a newer schema) fails every
// rewrite until an operator deletes it, so the failure is logged as an error
// that names the fix: delete the index, and the next rewrite puts every file.
func TestLiveRewrite_AnUnreadableIndexIsLoggedWithItsFix(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	var logs bytes.Buffer
	f.folder = handlers.NewLiveFolder(f.reader, f.store, fakeRenderer{}, f.lease, f.lease, f.metrics,
		liveServer, func() time.Time { return liveClock }, slog.New(slog.NewTextHandler(&logs, nil)))
	f.store.readErr = fmt.Errorf("%w: decode the live index tenants/default/contracts/live/_index.json: bad json",
		ports.ErrLiveIndexUnreadable)

	err := f.rewrite(3)

	require.ErrorIs(t, err, ports.ErrLiveIndexUnreadable)
	assert.Empty(t, f.store.writes(), "nothing is written")
	assert.Contains(t, logs.String(), "level=ERROR")
	assert.Contains(t, logs.String(), "tenants/default/contracts/live/_index.json")
	assert.Contains(t, logs.String(), "delete the index")
}

// A redelivery of a rewrite that failed after its puts puts the new file
// again: its interim entry, at version 0, matches no contract.
func TestLiveRewrite_ARedeliveryAfterAFailedRewritePutsTheNewFileAgain(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.x", 1, "sha256:x1"))
	f.listed(52,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.z", Version: 1, ContractHash: "sha256:z1"},
	)
	f.store.failDelete["analytics.z"] = errors.New("503 slow down")
	require.Error(t, f.rewrite(53))
	delete(f.store.failDelete, "analytics.z")
	before := len(f.store.writes())

	require.NoError(t, f.rewrite(53))

	assert.Equal(t, []string{"index", "put analytics.x", "delete analytics.z", "index"}, f.store.writes()[before:])
	assert.Equal(t, int64(53), f.store.index.PromotionSeq)
}

// Without server settings a file carries no servers entry, and the
// fingerprint is that of no server.
func TestLiveRewrite_NoServerPublishesNoServersEntry(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.folder = handlers.NewLiveFolder(f.reader, f.store, fakeRenderer{}, f.lease, f.lease, f.metrics,
		nil, func() time.Time { return liveClock }, newTestLogger())

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, "analytics.a v1 servers=false", string(f.store.files["analytics.a"]))
	assert.Equal(t, fingerprintOf(nil), f.store.index.SettingsSHA256, "the fingerprint of no server")
	assert.Equal(t, []string{fingerprintOf(nil)}, f.reported)
	assert.NotEqual(t, fingerprintOf(liveServer), fingerprintOf(nil))
	assert.Equal(t, fingerprintOf(&contractversion.Server{Type: "postgres"}), fingerprintOf(nil),
		"a server with no host publishes what no server does")
}

// At start, a tenant whose folder was written with other settings has every
// file rewritten and the new fingerprint stored; a tenant whose folder is
// current, and one whose folder was never written, are left alone.
func TestLiveFolder_ChangedSettingsRewriteEveryFileAtStart(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"), liveContract("analytics.b", 2, "sha256:b2"))
	f.listed(7,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.b", Version: 2, ContractHash: "sha256:b2"},
	)
	f.store.index.SettingsSHA256 = "old-settings"
	f.lease.seq, f.lease.release, f.lease.settings = 7, "rel-7", "old-settings"

	require.NoError(t, f.folder.RefreshAtStart(context.Background(), fakeTenants{"default"}, time.Millisecond))

	writes := f.store.writes()
	require.Len(t, writes, 4)
	assert.Equal(t, "index", writes[0])
	assert.ElementsMatch(t, []string{"put analytics.a", "put analytics.b"}, writes[1:3])
	assert.Equal(t, "index", writes[3])
	assert.Equal(t, int64(7), f.store.index.PromotionSeq, "the index keeps the promotion it was written for")
	assert.Equal(t, "rel-7", f.store.index.ReleaseID, "the release the watermark was recorded with")
	assert.Equal(t, f.settings, f.store.index.SettingsSHA256)
	assert.Equal(t, []string{f.settings}, f.lease.settingsSaved)
	assert.Equal(t, 1, f.lease.releases)
	assert.Equal(t, []int64{7}, f.reader.asked, "the live set of the promotion the folder's watermark names")

	for name, settings := range map[string]string{"current": f.settings, "never written": ""} {
		g := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
		g.lease.settings = settings
		require.NoError(t, g.folder.RefreshAtStart(context.Background(), fakeTenants{"default"}, time.Millisecond), name)
		assert.Empty(t, g.store.writes(), name)
		assert.Zero(t, g.lease.acquires, name)
	}
}

// A start-up rewrite writes nothing for a folder with no watermark, or while
// a newer promotion's contracts wait for their own rewrite.
func TestLiveFolder_AStartRewriteWithNoWatermarkOrANewerPromotionWritesNothing(t *testing.T) {
	noWatermark := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	noWatermark.lease.settings = "old-settings"
	newer := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	newer.lease.seq, newer.lease.settings = 7, "old-settings"
	newer.reader.latest = 8

	for name, f := range map[string]*liveFolderFixture{"no watermark": noWatermark, "newer promotion": newer} {
		require.NoError(t, f.folder.RefreshAtStart(context.Background(), fakeTenants{"default"}, time.Millisecond), name)
		assert.Empty(t, f.store.writes(), name)
		assert.Empty(t, f.reader.asked, name)
		assert.Empty(t, f.lease.settingsSaved, name)
	}
}

// A start-up rewrite that fails is tried again until it succeeds.
func TestLiveFolder_AFailedStartRewriteIsTriedAgain(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.lease.seq, f.lease.settings = 7, "old-settings"
	f.lease.heldElsewhere = true
	go func() {
		for f.leaseAcquires() < 2 {
			time.Sleep(time.Millisecond)
		}
		f.lease.mu.Lock()
		f.lease.heldElsewhere = false
		f.lease.mu.Unlock()
	}()

	require.NoError(t, f.folder.RefreshAtStart(context.Background(), fakeTenants{"default"}, time.Millisecond))

	assert.Equal(t, []string{"index", "put analytics.a", "index"}, f.store.writes())
	assert.Equal(t, []string{f.settings}, f.lease.settingsSaved)
}

// RefreshAtStart stops trying when its context ends.
func TestLiveFolder_StartRewriteStopsAtShutdown(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.lease.settings = "old-settings"
	f.lease.heldElsewhere = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := f.folder.RefreshAtStart(ctx, fakeTenants{"default"}, time.Millisecond)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, f.store.writes())
}

func (f *liveFolderFixture) leaseAcquires() int {
	f.lease.mu.Lock()
	defer f.lease.mu.Unlock()
	return f.lease.acquires
}

// A bundle with no contract still rewrites the folder: with nothing live, only
// the index is written, and transaction B records the folder's watermark.
func TestContractsHandler_AnEmptyBundleWritesOnlyTheIndex(t *testing.T) {
	f := newContractsFixture()
	f.bundles.bundle.Contracts = nil
	live := newLiveFolderFixture()
	h := handlers.NewReleasePromotedContractsHandler(f.uow, f.bundles, live.folder, f.metrics, time.Minute, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), "1-0", nil, contractsInput()))

	assert.Equal(t, []string{"index"}, live.store.writes())
	assert.Empty(t, live.store.index.Files)
	assert.Equal(t, int64(3), live.store.index.PromotionSeq)
	assert.Equal(t, []liveFolderRecord{{seq: 3, release: "rel-1", settings: live.settings}}, f.repo.liveFolder)
}

// A start-up rewrite whose index names an older promotion than the
// watermark writes the watermark's promotion and release into the index,
// never an empty release.
func TestLiveFolder_AStartRewriteNamesTheWatermarksRelease(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.listed(6, ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"})
	f.store.index.SettingsSHA256 = "old-settings"
	f.lease.seq, f.lease.release, f.lease.settings = 7, "rel-7", "old-settings"

	require.NoError(t, f.folder.RefreshAtStart(context.Background(), fakeTenants{"default"}, time.Millisecond))

	assert.Equal(t, int64(7), f.store.index.PromotionSeq)
	assert.Equal(t, "rel-7", f.store.index.ReleaseID)
}

// Rewrite reports the settings the folder's files carry: this folder's when it
// wrote the index or found it written for the promotion with them, nothing
// when it skipped without writing.
func TestLiveRewrite_ReportsTheSettingsOnlyWhenTheFolderCarriesThem(t *testing.T) {
	wrote := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	require.NoError(t, wrote.rewrite(3))

	done := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	done.listed(3, ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"})
	require.NoError(t, done.rewrite(3))

	atWatermark := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	atWatermark.lease.seq = 5
	require.NoError(t, atWatermark.rewrite(3))

	superseded := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	superseded.reader.latest = 5
	require.NoError(t, superseded.rewrite(3))

	assert.Equal(t, []string{wrote.settings}, wrote.reported)
	assert.Equal(t, []string{done.settings}, done.reported)
	assert.Equal(t, []string{""}, atWatermark.reported)
	assert.Equal(t, []string{""}, superseded.reported)
}
