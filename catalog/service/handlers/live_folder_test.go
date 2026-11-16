package handlers_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
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
	assert.Equal(t, []string{"put analytics.a", "index"}, f.store.writes())
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
	assert.Equal(t, [][2]string{{ports.LiveWritePut, ports.LiveWriteOK}, {ports.LiveWriteIndex, ports.LiveWriteOK}}, f.metrics.writes())
	assert.Equal(t, 1, f.lease.releases, "the lease is released")
}

func TestLiveRewrite_DeletesTheFileOfANodeNoLongerLive(t *testing.T) {
	f := newLiveFolderFixture(liveContract("analytics.a", 1, "sha256:a1"))
	f.listed(2,
		ports.LiveIndexFile{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"},
		ports.LiveIndexFile{UniqueID: "analytics.retired", Version: 4, ContractHash: "sha256:r4"},
	)

	require.NoError(t, f.rewrite(3))

	assert.Equal(t, []string{"delete analytics.retired", "index"}, f.store.writes())
	assert.NotContains(t, f.store.files, "analytics.retired")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"}}, f.store.index.Files)
	assert.Equal(t, [][2]string{{ports.LiveWriteDelete, ports.LiveWriteOK}, {ports.LiveWriteIndex, ports.LiveWriteOK}}, f.metrics.writes())
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
	require.Len(t, writes, 3)
	assert.ElementsMatch(t, []string{"delete analytics.a", "delete analytics.b"}, writes[:2], "deletes run at once, in any order")
	assert.Equal(t, "index", writes[2])
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
	assert.ElementsMatch(t, []string{"put analytics.a", "put analytics.b"}, writes[:2])
	assert.Equal(t, "index", writes[2])
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
		SchemaVersion: ports.LiveIndexSchemaVersion, TenantID: "default", WrittenAt: liveClock,
		Files: []ports.LiveIndexFile{{UniqueID: "analytics.a"}},
	}, *f.store.index, "no promotion and no settings yet, the new file at version 0")
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
		{UniqueID: "analytics.z", Version: 1, ContractHash: "sha256:z1"},
	}, f.store.index.Files)

	// Promotion 54 drops x.
	delete(f.store.failDelete, "analytics.z")
	f.reader.live = []contractversion.LiveContract{liveContract("analytics.a", 1, "sha256:a1")}
	require.NoError(t, f.rewrite(54))

	assert.NotContains(t, f.store.files, "analytics.x", "x's file is deleted")
	assert.NotContains(t, f.store.files, "analytics.z")
	assert.Equal(t, []ports.LiveIndexFile{{UniqueID: "analytics.a", Version: 1, ContractHash: "sha256:a1"}}, f.store.index.Files)
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

	assert.Equal(t, []string{"put analytics.x", "delete analytics.z", "index"}, f.store.writes()[before:],
		"no second interim index: x is listed already")
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
	require.Len(t, writes, 3)
	assert.ElementsMatch(t, []string{"put analytics.a", "put analytics.b"}, writes[:2])
	assert.Equal(t, "index", writes[2])
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
