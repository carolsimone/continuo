package handlers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiveCandidate_RefusedDuringMaintenance(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Maintenance = true
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	})
	require.True(t, errors.Is(err, maintenance.ErrActive), "%v", err)
	_, getErr := store.GetRelease("rA")
	assert.Error(t, getErr, "a refused release is not stored") // the fake store answers "not found" with an error
}

func TestRetryRemediation_RefusedDuringMaintenance(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	deps.Maintenance = true
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{NodeID: "finance", Attempt: 1, Status: "failed", RemediationRound: 1}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.True(t, errors.Is(err, maintenance.ErrActive), "%v", err)
	assert.Empty(t, outboxEntries(store))
	r, _ := store.GetRelease("rel-1")
	assert.Equal(t, 1, r.RemediationRound(), "a refused retry starts no round")
}

// A round agent-remediation skipped because maintenance was on is closed, so
// once maintenance is off the operator can start the next round.
func TestRetryRemediation_SkippedRoundIsRetryable(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"release_id":"rel-1","reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{NodeID: "finance", Attempt: 1, Status: "skipped", RemediationRound: 1}}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	assert.Equal(t, 2, res.RemediationRound)
}

// During maintenance the queue skips an older candidate and activates the
// fix's release check of a remediation attempt already in progress.
func TestAdvanceQueue_MaintenanceActivatesOnlyFixes(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	deps.Maintenance = true
	store.SeedProductionPointer(release.NewProductionPointer("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedRelease(releasecheck.NewCandidate("rCand", "svc-a", "tag-c", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC()))
	store.SeedRelease(releasecheck.NewFix("rVerify", "svc-b", "img:1", "rGone", 1, "", release.ManifestKindPython, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	cand, _ := store.GetRelease("rCand")
	assert.Equal(t, releasecheck.StatusReceived, cand.Status(), "a candidate release waits while maintenance is on")
	verify, _ := store.GetRelease("rVerify")
	assert.Equal(t, releasecheck.StatusParsing, verify.Status(), "the fix's release check activates")
	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
}

// Candidates held during maintenance activate in creation order once it is off.
func TestAdvanceQueue_CandidatesResumeInOrderAfterMaintenance(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	deps.Maintenance = true
	store.SeedRelease(releasecheck.NewCandidate("rFirst", "svc-a", "t1", false, false, "acme/demo", "aaa", release.ManifestKindDbt, time.Unix(100, 0).UTC()))
	store.SeedRelease(releasecheck.NewCandidate("rSecond", "svc-a", "t2", false, false, "acme/demo", "bbb", release.ManifestKindDbt, time.Unix(150, 0).UTC()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	first, _ := store.GetRelease("rFirst")
	assert.Equal(t, releasecheck.StatusReceived, first.Status())
	assert.Empty(t, outboxEntries(store))

	deps.Maintenance = false
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	first, _ = store.GetRelease("rFirst")
	second, _ := store.GetRelease("rSecond")
	assert.Equal(t, releasecheck.StatusCompiling, first.Status(), "the oldest held candidate activates first")
	assert.Equal(t, releasecheck.StatusReceived, second.Status())
}
