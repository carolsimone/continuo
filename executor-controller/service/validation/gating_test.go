package validation_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- chain-backed fakes ----------------------------------------------------

// chainDepRepo is a map-backed DeploymentRepository: Save writes back into the
// same map ListValidationByRelease reads, so a Save made during propagation is
// visible to the subsequent aggregate count — matching production's
// within-transaction read-after-write. It records the ordered sequence of
// mutating/reading calls so a test can assert ordering relative to the lock.
type chainDepRepo struct {
	// Embeds the port so operations this test never exercises need no stub.
	repository.DeploymentRepository
	nodes map[string]*model.Deployment // keyed by nodeID
	calls *callLog
}

func newChainDepRepo(log *callLog, chain ...*model.Deployment) *chainDepRepo {
	m := make(map[string]*model.Deployment, len(chain))
	for _, d := range chain {
		m[d.NodeID()] = d
	}
	return &chainDepRepo{nodes: m, calls: log}
}

func (r *chainDepRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *chainDepRepo) GetDueJobs(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *chainDepRepo) Save(_ context.Context, d *model.Deployment) error {
	r.calls.record("Save")
	r.nodes[d.NodeID()] = d
	return nil
}
func (r *chainDepRepo) GetByReleaseNode(_ context.Context, _ string, nodeID string, _ model.Mode) (*model.Deployment, error) {
	return r.nodes[nodeID], nil
}
func (r *chainDepRepo) PendingValidationCount(context.Context, string, model.Mode) (int, error) {
	count := 0
	for _, d := range r.nodes {
		switch d.Status() {
		case model.StatusPending, model.StatusBlocked, model.StatusDeployed:
			count++
		}
	}
	return count, nil
}
func (r *chainDepRepo) ListValidationResults(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	out := make([]*model.Deployment, 0, len(r.nodes))
	for _, d := range r.nodes {
		out = append(out, d)
	}
	return out, nil
}
func (r *chainDepRepo) ListValidationByRelease(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	out := make([]*model.Deployment, 0, len(r.nodes))
	for _, d := range r.nodes {
		out = append(out, d)
	}
	return out, nil
}

func (r *chainDepRepo) statusOf(nodeID string) model.Status {
	if d, ok := r.nodes[nodeID]; ok {
		return d.Status()
	}
	return ""
}

var _ repository.DeploymentRepository = (*chainDepRepo)(nil)

// callLog is a shared ordered recorder so a test can assert LockRelease precedes
// any Save/ClaimEmission.
type callLog struct {
	events []string
}

func (l *callLog) record(name string) { l.events = append(l.events, name) }

func (l *callLog) firstIndexOf(name string) int {
	for i, e := range l.events {
		if e == name {
			return i
		}
	}
	return -1
}

// orderedAggRepo records LockRelease/ClaimEmission into the shared callLog.
type orderedAggRepo struct {
	won bool
	log *callLog
}

func (r *orderedAggRepo) LockRelease(context.Context, string, model.Mode) error {
	r.log.record("LockRelease")
	return nil
}
func (r *orderedAggRepo) ClaimEmission(context.Context, string, model.Mode, time.Time) (bool, error) {
	r.log.record("ClaimEmission")
	return r.won, nil
}

var _ repository.ValidationAggregateRepository = (*orderedAggRepo)(nil)

// --- builders --------------------------------------------------------------

func validationNode(t *testing.T, releaseID, nodeID string, status model.Status, ups ...string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		ServiceName:     "dbt",
		SchemaName:      "public",
		TableName:       nodeID,
		NodeType:        "dbt-model",
		ImageTag:        "sha-test",
		JobName:         "validate-" + nodeID,
		CandidateSchema: "_cand_" + releaseID,
		UpstreamNodeIDs: ups,
	}
	now := time.Now()
	hasUpstreams := len(ups) > 0
	d := model.NewValidationDeployment(cmd, nil, now, hasUpstreams)
	switch status {
	case model.StatusDeployed:
		require.NoError(t, d.MarkDeployed(now), "seed %s deployed", nodeID)
	case model.StatusBlocked:
		// NewValidationDeployment with hasUpstreams=true is already blocked.
	case model.StatusPending:
		// Default for hasUpstreams=false.
	}
	return d
}

// markOk drives a deployed node to a terminal ok outcome (records it back).
func markOk(t *testing.T, d *model.Deployment) {
	t.Helper()
	require.NoError(t, d.RecordOutcome("ok", "", "", time.Now()))
}

// --- tests -----------------------------------------------------------------

// (a) SettleNodeTerminal with a failed node skips its transitive blocked
// descendants and, once nothing remains pending, emits a failed aggregate.
func TestSettleNodeTerminal_FailureSkipsTransitiveDescendants_EmitsFailedAggregate(t *testing.T) {
	log := &callLog{}
	a1 := validationNode(t, "rel", "a1", model.StatusDeployed)
	require.NoError(t, a1.FailValidation("boom", time.Now())) // a1 already terminal failed
	a2 := validationNode(t, "rel", "a2", model.StatusBlocked, "a1")
	a3 := validationNode(t, "rel", "a3", model.StatusBlocked, "a2")
	repo := newChainDepRepo(log, a1, a2, a3)
	outboxRepo := &captureOutbox{}
	agg := &orderedAggRepo{won: true, log: log}

	err := validation.SettleNodeTerminal(
		context.Background(), repo, outboxRepo, agg,
		validation.DedupNamespace, "rel", "a1", "failed", time.Now())
	require.NoError(t, err)

	assert.Equal(t, model.StatusSkipped, repo.statusOf("a2"), "a2 skipped — upstream a1 failed")
	assert.Equal(t, model.StatusSkipped, repo.statusOf("a3"), "a3 transitively skipped")

	require.NotNil(t, outboxRepo.last, "validation.completed emitted once nothing pending")
	assert.Contains(t, string(outboxRepo.last.Payload), `"aggregate_status":"failed"`)
}

// (b) SettleNodeTerminal takes the per-release lock BEFORE any propagation or
// emit (guards Bug B): LockRelease must be the first recorded event and must
// precede any Save.
func TestSettleNodeTerminal_LocksBeforePropagationAndEmit(t *testing.T) {
	log := &callLog{}
	a1 := validationNode(t, "rel", "a1", model.StatusDeployed)
	require.NoError(t, a1.FailValidation("boom", time.Now()))
	a2 := validationNode(t, "rel", "a2", model.StatusBlocked, "a1")
	repo := newChainDepRepo(log, a1, a2)
	outboxRepo := &captureOutbox{}
	agg := &orderedAggRepo{won: true, log: log}

	require.NoError(t, validation.SettleNodeTerminal(
		context.Background(), repo, outboxRepo, agg,
		validation.DedupNamespace, "rel", "a1", "failed", time.Now()))

	require.NotEmpty(t, log.events)
	assert.Equal(t, "LockRelease", log.events[0], "lock taken before any propagation/emit work")

	lockIdx := log.firstIndexOf("LockRelease")
	saveIdx := log.firstIndexOf("Save")
	claimIdx := log.firstIndexOf("ClaimEmission")
	require.GreaterOrEqual(t, saveIdx, 0, "propagation saved the skipped descendant")
	assert.Less(t, lockIdx, saveIdx, "LockRelease precedes the first Save")
	require.GreaterOrEqual(t, claimIdx, 0, "claim attempted")
	assert.Less(t, lockIdx, claimIdx, "LockRelease precedes ClaimEmission")
}

// (c) Multi-upstream convergence under sequential settles: c with ups=[a1,b1]
// stays blocked after a1 settles ok, and unblocks only after b1 also settles ok.
// This is the serialized equivalent of the concurrent race: the second settle,
// under the lock, observes the first settle's committed outcome.
func TestSettleNodeTerminal_MultiUpstreamConvergesSequentially(t *testing.T) {
	log := &callLog{}
	a1 := validationNode(t, "rel", "a1", model.StatusDeployed)
	b1 := validationNode(t, "rel", "b1", model.StatusDeployed)
	c := validationNode(t, "rel", "c", model.StatusBlocked, "a1", "b1")
	repo := newChainDepRepo(log, a1, b1, c)
	outboxRepo := &captureOutbox{}
	agg := &orderedAggRepo{won: true, log: log}

	// a1 reaches a terminal ok outcome and settles. c must stay blocked: b1 has no
	// outcome yet.
	markOk(t, a1)
	require.NoError(t, validation.SettleNodeTerminal(
		context.Background(), repo, outboxRepo, agg,
		validation.DedupNamespace, "rel", "a1", "ok", time.Now()))
	assert.Equal(t, model.StatusBlocked, repo.statusOf("c"), "c blocked — b1 not yet ok")

	// b1 reaches a terminal ok outcome and settles. Now both upstreams are ok, so
	// the settle's propagation unblocks c to pending.
	markOk(t, b1)
	require.NoError(t, validation.SettleNodeTerminal(
		context.Background(), repo, outboxRepo, agg,
		validation.DedupNamespace, "rel", "b1", "ok", time.Now()))
	assert.Equal(t, model.StatusPending, repo.statusOf("c"), "c unblocked once both upstreams ok")
}
