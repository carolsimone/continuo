package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: ports.ContractBundleReader ────────────────────────────────────────

// fakeContractBundleReader answers every Fetch with bundle, or with err when
// set, and records the URI and SHA-256 of each call.
type fakeContractBundleReader struct {
	bundle ports.ContractBundle
	err    error
	calls  [][2]string
}

func (f *fakeContractBundleReader) Fetch(_ context.Context, uri, sha string) (ports.ContractBundle, error) {
	f.calls = append(f.calls, [2]string{uri, sha})
	if f.err != nil {
		return ports.ContractBundle{}, f.err
	}
	return f.bundle, nil
}

var _ ports.ContractBundleReader = (*fakeContractBundleReader)(nil)

// ── fakes: repository.ContractRepository ─────────────────────────────────────

// fakeContractRepository answers ReadStates with states and Write with res,
// recording each plan it is given.
type fakeContractRepository struct {
	states map[string]contractversion.NodeState
	res    contractversion.WriteResult
	reads  [][]string
	plans  []contractversion.WritePlan
	promos []contractversion.Promotion
}

func (f *fakeContractRepository) ReadStates(_ context.Context, _ string, ids []string) (map[string]contractversion.NodeState, error) {
	f.reads = append(f.reads, ids)
	return f.states, nil
}

func (f *fakeContractRepository) Write(_ context.Context, _ string, p contractversion.Promotion, plan contractversion.WritePlan) (contractversion.WriteResult, error) {
	f.promos = append(f.promos, p)
	f.plans = append(f.plans, plan)
	return f.res, nil
}

var _ repository.ContractRepository = (*fakeContractRepository)(nil)

// ── fakes: handlers.LiveContractsRewriter ────────────────────────────────────

// fakeLiveRewriter records each promotion it rewrites for and fails with err
// when set. onRewrite, when set, runs first.
type fakeLiveRewriter struct {
	err       error
	calls     []domainModel.PromoteReleaseInput
	onRewrite func()
}

func (f *fakeLiveRewriter) Rewrite(_ context.Context, in domainModel.PromoteReleaseInput) error {
	if f.onRewrite != nil {
		f.onRewrite()
	}
	f.calls = append(f.calls, in)
	return f.err
}

var _ handlers.LiveContractsRewriter = (*fakeLiveRewriter)(nil)

// ── fakes: ports.ContractMetrics ─────────────────────────────────────────────

// fakeContractMetrics records what the handler counts. onVersions, when set,
// runs at each VersionsWritten call, so a test can see what had committed by
// then.
type fakeContractMetrics struct {
	versions   []int
	steps      []string
	liveWrites [][2]string
	onVersions func()
}

func (f *fakeContractMetrics) VersionsWritten(n int) {
	if f.onVersions != nil {
		f.onVersions()
	}
	f.versions = append(f.versions, n)
}

func (f *fakeContractMetrics) LiveWrite(op, outcome string) {
	f.liveWrites = append(f.liveWrites, [2]string{op, outcome})
}

func (f *fakeContractMetrics) ObserveStep(step string, _ time.Duration) {
	f.steps = append(f.steps, step)
}

var _ ports.ContractMetrics = (*fakeContractMetrics)(nil)

// ── fixtures ─────────────────────────────────────────────────────────────────

const contractsBundleURI = "s3://continuo/tenants/default/topologies/rel-1/contracts.json.gz"

// contractsInput is promotion 3 of rel-1 for tenant default, with a contract
// bundle.
func contractsInput() domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		TenantID:             "default",
		ReleaseID:            "rel-1",
		PromotionSeq:         3,
		TopologyURI:          "s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
		TopologySHA256:       "sha-topology",
		PromotedAt:           time.Date(2026, 11, 20, 10, 2, 11, 0, time.UTC),
		ContractBundleURI:    contractsBundleURI,
		ContractBundleSHA256: "sha-contracts",
	}
}

// contractsBundle is rel-1's bundle: the contracts of two python nodes.
func contractsBundle() ports.ContractBundle {
	return ports.ContractBundle{
		ReleaseID:         "rel-1",
		ContinuoContracts: "0.2.0",
		Contracts: []contractversion.Incoming{
			{UniqueID: "analytics.py_daily_kpis", ContractHash: "sha256:3f1c", ContentHash: "sha256:91d4",
				Family: "python", Service: "service-py", SourcePath: "contracts/py_daily_kpis.odcs.yaml",
				Document: map[string]any{"id": "analytics.py_daily_kpis"}},
			{UniqueID: "analytics.py_weekly", ContractHash: "sha256:a0b1", ContentHash: "sha256:77aa",
				Family: "python", Service: "service-py", SourcePath: "contracts/py_weekly.odcs.yaml",
				Document: map[string]any{"id": "analytics.py_weekly"}},
		},
	}
}

// contractsFixture is a contracts handler over in-memory fakes.
type contractsFixture struct {
	uow     *fakeGraphUnitOfWork
	repo    *fakeContractRepository
	bundles *fakeContractBundleReader
	live    *fakeLiveRewriter
	metrics *fakeContractMetrics
	handler *handlers.ReleasePromotedContractsHandler
}

func newContractsFixture() *contractsFixture {
	f := &contractsFixture{
		uow:     newFakeGraphUnitOfWork(nil),
		repo:    &fakeContractRepository{res: contractversion.WriteResult{VersionsCreated: 2, EdgesCreated: 2, PointersMoved: 2}},
		bundles: &fakeContractBundleReader{bundle: contractsBundle()},
		live:    &fakeLiveRewriter{},
		metrics: &fakeContractMetrics{},
	}
	f.uow.tx.contracts = f.repo
	f.handler = handlers.NewReleasePromotedContractsHandler(f.uow, f.bundles, f.live, f.metrics, newTestLogger())
	return f
}

// commitFailingUnitOfWork runs each write's function and then fails the
// non-final writes, as a transaction whose commit is lost does.
type commitFailingUnitOfWork struct {
	*fakeGraphUnitOfWork
}

var errCommitLost = errors.New("the commit was lost")

func (c commitFailingUnitOfWork) Write(ctx context.Context, m uow.Marker, final bool, fn func(ctx context.Context, tx uow.Tx) error) error {
	if final {
		return c.fakeGraphUnitOfWork.Write(ctx, m, final, fn)
	}
	c.writes = append(c.writes, fakeGraphWrite{Marker: m, Final: final})
	if err := fn(ctx, c.tx); err != nil {
		return err
	}
	return errCommitLost
}

// ── tests ────────────────────────────────────────────────────────────────────

// A promotion announced without a contract bundle records its marker in one
// final write and reads no object.
func TestContractsHandler_APromotionWithNoBundleRecordsOnlyItsMarker(t *testing.T) {
	f := newContractsFixture()
	in := contractsInput()
	in.ContractBundleURI, in.ContractBundleSHA256 = "", ""

	require.NoError(t, f.handler.Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, f.bundles.calls, "no bundle is read")
	assert.Empty(t, f.repo.plans, "nothing is planned")
	assert.Empty(t, f.live.calls, "the live folder is not rewritten")
	require.Len(t, f.uow.writes, 1)
	assert.True(t, f.uow.writes[0].Final)
	assert.Equal(t, streams.CatalogReleaseContracts, f.uow.writes[0].Marker.Group)
	assert.True(t, f.uow.isRecorded(f.uow.writes[0].Marker))
	assert.Empty(t, f.metrics.versions)
}

// A processed message is acknowledged before any object is read.
func TestContractsHandler_AProcessedMessageReadsNothing(t *testing.T) {
	f := newContractsFixture()
	in := contractsInput()
	f.uow.record(uow.Marker{TenantID: in.TenantID, Group: streams.CatalogReleaseContracts, MessageID: "1-0"})

	require.NoError(t, f.handler.Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, f.bundles.calls)
	assert.Empty(t, f.uow.writes)
	assert.Empty(t, f.live.calls)
}

// A bundle the reader refuses as invalid is permanent: the message is
// dead-lettered and nothing is written.
func TestContractsHandler_AnInvalidBundleIsPermanent(t *testing.T) {
	f := newContractsFixture()
	f.bundles.err = fmt.Errorf("%w: checksum mismatch", ports.ErrContractBundleInvalid)

	err := f.handler.Handle(context.Background(), "1-0", nil, contractsInput())

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, f.uow.writes)
	assert.Empty(t, f.live.calls)
}

// A bundle of another release is permanent: recording it would stamp this
// promotion onto another release's contracts.
func TestContractsHandler_ABundleOfAnotherReleaseIsPermanent(t *testing.T) {
	f := newContractsFixture()
	f.bundles.bundle.ReleaseID = "rel-0"

	err := f.handler.Handle(context.Background(), "1-0", nil, contractsInput())

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Contains(t, err.Error(), "rel-0")
	assert.Empty(t, f.uow.writes)
	assert.Empty(t, f.live.calls)
}

// A bundle that is absent from object storage is retried, as a missing code
// bundle is: an object that is merely late becomes readable.
func TestContractsHandler_AMissingBundleIsRetried(t *testing.T) {
	f := newContractsFixture()
	f.bundles.err = fmt.Errorf("%w: s3://continuo/k", ports.ErrContractBundleNotFound)

	err := f.handler.Handle(context.Background(), "1-0", nil, contractsInput())

	require.ErrorIs(t, err, ports.ErrContractBundleNotFound)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, f.uow.writes)
}

// While an export or a restore holds the tenant's lease the handler returns
// uow.ErrWritesPaused without reading the bundle, so the consumer backs off
// and writes no dead letter.
func TestContractsHandler_WritesPausedBacksOff(t *testing.T) {
	f := newContractsFixture()
	f.uow.paused = true

	err := f.handler.Handle(context.Background(), "1-0", nil, contractsInput())

	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, f.bundles.calls)
	assert.Empty(t, f.uow.writes)
}

// A promotion with a bundle writes in two transactions around the live
// rewrite: transaction A plans and writes the versions and marks the message
// in flight (not final), and transaction B records the marker (final).
func TestContractsHandler_WritesTheVersionsThenRewritesThenRecordsTheMarker(t *testing.T) {
	f := newContractsFixture()
	f.repo.states = map[string]contractversion.NodeState{
		"analytics.py_weekly": {UniqueID: "analytics.py_weekly", Watermark: 2, CurrentHash: "sha256:a0b1", CurrentVersion: 1, MaxVersion: 1},
	}
	var writesAtRewrite int
	f.live.onRewrite = func() { writesAtRewrite = len(f.uow.writes) }
	in := contractsInput()

	require.NoError(t, f.handler.Handle(context.Background(), "1-0", nil, in))

	require.Len(t, f.uow.writes, 2)
	assert.False(t, f.uow.writes[0].Final, "transaction A marks the message in flight")
	assert.True(t, f.uow.writes[1].Final, "transaction B records the marker")
	assert.Equal(t, 1, writesAtRewrite, "the live folder is rewritten between the two transactions")
	require.Len(t, f.live.calls, 1)
	assert.Equal(t, in, f.live.calls[0])

	require.Len(t, f.repo.reads, 1)
	assert.Equal(t, []string{"analytics.py_daily_kpis", "analytics.py_weekly"}, f.repo.reads[0])
	require.Len(t, f.repo.plans, 1)
	plan := f.repo.plans[0]
	assert.Len(t, plan.Versions, 2)
	require.Len(t, plan.Edges, 1, "the node whose contract is unchanged takes no edge")
	assert.Equal(t, "analytics.py_daily_kpis", plan.Edges[0].UniqueID)
	assert.Equal(t, 1, plan.Unchanged)
	assert.Equal(t, contractversion.Promotion{
		TenantID: "default", ReleaseID: "rel-1", ContinuoContracts: "0.2.0",
		PromotionSeq: 3, PromotedAt: in.PromotedAt,
	}, f.repo.promos[0])
	assert.Equal(t, []string{ports.ContractStepIngest, ports.ContractStepLiveRewrite}, f.metrics.steps)
}

// The versions counter moves once transaction A has committed, by what it
// created; a transaction A whose commit is lost counts nothing.
func TestContractsHandler_CountsVersionsAfterCommit(t *testing.T) {
	f := newContractsFixture()
	var writesAtCount []int
	f.metrics.onVersions = func() { writesAtCount = append(writesAtCount, len(f.uow.writes)) }

	require.NoError(t, f.handler.Handle(context.Background(), "1-0", nil, contractsInput()))
	assert.Equal(t, []int{2}, f.metrics.versions, "the versions transaction A created")
	assert.Equal(t, []int{1}, writesAtCount, "counted after transaction A, before transaction B")

	lost := newContractsFixture()
	lost.handler = handlers.NewReleasePromotedContractsHandler(
		commitFailingUnitOfWork{lost.uow}, lost.bundles, lost.live, lost.metrics, newTestLogger())
	err := lost.handler.Handle(context.Background(), "1-0", nil, contractsInput())
	require.ErrorIs(t, err, errCommitLost)
	assert.Empty(t, lost.metrics.versions, "nothing is counted for a transaction that did not commit")
	assert.Empty(t, lost.metrics.steps)
	assert.Empty(t, lost.live.calls, "the live folder waits for transaction A")
}

// A live rewrite that fails is retried: the marker is not recorded, so the
// message is redelivered, and the error is not permanent.
func TestContractsHandler_AFailedLiveRewriteIsRetried(t *testing.T) {
	f := newContractsFixture()
	f.live.err = errors.New("object storage unavailable")

	err := f.handler.Handle(context.Background(), "1-0", nil, contractsInput())

	require.Error(t, err)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
	require.Len(t, f.uow.writes, 1, "transaction B does not run")
	assert.False(t, f.uow.isRecorded(f.uow.writes[0].Marker))
	assert.Equal(t, []string{ports.ContractStepIngest}, f.metrics.steps)
}

// A message another delivery finished between the lookup and transaction A
// is acknowledged without a live rewrite.
func TestContractsHandler_AMessageFinishedMeanwhileIsAcknowledged(t *testing.T) {
	f := newContractsFixture()
	in := contractsInput()
	f.bundles.err = nil
	marker := uow.Marker{TenantID: in.TenantID, Group: streams.CatalogReleaseContracts, MessageID: "1-0"}
	reader := &recordingBundleReader{fakeContractBundleReader: f.bundles, onFetch: func() { f.uow.record(marker) }}
	h := handlers.NewReleasePromotedContractsHandler(f.uow, reader, f.live, f.metrics, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, f.repo.plans)
	assert.Empty(t, f.live.calls)
	assert.Empty(t, f.metrics.versions)
}

// recordingBundleReader runs onFetch before it answers.
type recordingBundleReader struct {
	*fakeContractBundleReader
	onFetch func()
}

func (r *recordingBundleReader) Fetch(ctx context.Context, uri, sha string) (ports.ContractBundle, error) {
	r.onFetch()
	return r.fakeContractBundleReader.Fetch(ctx, uri, sha)
}

// A node a newer promotion already reached is superseded: it changes nothing,
// and the handler's log line names the release and how many such nodes it
// had.
func TestContractsHandler_LogsTheSupersededNodes(t *testing.T) {
	f := newContractsFixture()
	f.repo.states = map[string]contractversion.NodeState{
		"analytics.py_daily_kpis": {UniqueID: "analytics.py_daily_kpis", Watermark: 9},
		"analytics.py_weekly":     {UniqueID: "analytics.py_weekly", Watermark: 9},
	}
	var logs bytes.Buffer
	h := handlers.NewReleasePromotedContractsHandler(f.uow, f.bundles, f.live, f.metrics,
		slog.New(slog.NewJSONHandler(&logs, nil)))

	require.NoError(t, h.Handle(context.Background(), "1-0", nil, contractsInput()))

	require.Len(t, f.repo.plans, 1)
	assert.Empty(t, f.repo.plans[0].Edges)
	assert.Equal(t, 2, f.repo.plans[0].Superseded)
	var line map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var l map[string]any
		require.NoError(t, json.Unmarshal(raw, &l))
		if _, ok := l["superseded"]; ok {
			line = l
		}
	}
	require.NotNil(t, line, "a log line counts the superseded nodes")
	assert.Equal(t, "rel-1", line["release_id"])
	assert.Equal(t, 2.0, line["superseded"])
}
