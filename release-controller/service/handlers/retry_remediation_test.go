package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rejectedRelease builds a release that reached StatusRejected with reason
// "compile_failed" and, when payload is non-empty, a stored rejection payload.
func rejectedRelease(t *testing.T, id, payload string) *releasecheck.Check {
	t.Helper()
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	r := releasecheck.NewCandidate(id, "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToCompiling(now))
	require.NoError(t, r.Fail("compile_failed", "", []string{"finance"}, now))
	if payload != "" {
		r.SetRejectionPayload([]byte(payload))
	}
	return r
}

func TestRetryRemediation_DeadEndStartsRoundTwo(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"release_id":"rel-1","stage":"compile","reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{NodeID: "finance", Attempt: 3, Status: "escalated", RemediationRound: 1}}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	require.Equal(t, streams.RemediationRetryRequestedV1, entries[0].StreamName)
	require.Equal(t, "remediation_retry_requested", entries[0].EventType)
}

// TestRetryRemediation_WritesTheTypedRetry pins the row the retry stores: the
// typed remediation.retry_requested:v1 payload, wrapped under the default
// tenant, carrying the later round and the release's stored rejection
// unchanged.
func TestRetryRemediation_WritesTheTypedRetry(t *testing.T) {
	const rejection = `{"release_id":"rel-1","stage":"compile","reason":"compile_failed","per_node":[{"node_id":"finance","error":"boom"}]}`
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", rejection))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{NodeID: "finance", Attempt: 3, Status: "escalated", RemediationRound: 1}}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	e := entries[0]
	require.Equal(t, streams.RemediationRetryRequestedV1, e.StreamName)
	require.Equal(t, "remediation_retry_requested", e.EventType)
	require.Equal(t, "release-controller", e.AggregateType)
	require.Equal(t, handlers.AggregateIDForRelease("rel-1"), e.AggregateID)
	require.Equal(t, uuid.NewSHA1(uuid.NameSpaceOID, []byte("remediation-retry:rel-1:2")), e.ID, "one row per release and round")

	tenantID, payload, err := pkgoutbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	require.Equal(t, events.DefaultTenantID, tenantID)
	var got events.RemediationRetryRequested
	require.NoError(t, json.Unmarshal(payload, &got))
	require.Equal(t, "rel-1", got.ReleaseID)
	require.Equal(t, 2, got.RemediationRound)
	require.Equal(t, rejection, string(got.Rejection), "the stored rejection travels unchanged")

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &raw))
	require.NotContains(t, raw, "reason", "the rejection is nested, not spread into the payload")
}

// TestRetryRemediation_AllHandedOverRoundIsRetryable pins that a round whose
// every attempt ended skipped (the classifier or the engine handed each node
// over without a fix; release-controller reads the status, not the code) has
// nothing in flight and nothing proposed, so the operator can start the next
// round.
func TestRetryRemediation_AllHandedOverRoundIsRetryable(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"release_id":"rel-1","stage":"compile","reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "skipped", RemediationRound: 1},
		{ID: "p2", NodeID: "ledger", Attempt: 1, Status: "skipped", RemediationRound: 1},
	}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())
	require.Len(t, outboxEntries(store), 1)
}

func TestRetryRemediation_RefusesWhenAFixIsOpen(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{NodeID: "finance", Attempt: 1, Status: "failed"},
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "proposed", PullRequests: []ports.FixPR{{PRState: "open", PRURL: "https://x/pr/7"}}},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p2", open.FixID)
	require.Equal(t, "https://x/pr/7", open.PRURL)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_RefusesInFlightAttempt(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{ID: "p1", NodeID: "finance", Attempt: 1, Status: "generating"}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorAs(t, err, new(handlers.ErrFixOpen))
}

func TestRetryRemediation_Refusals(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		rel  func() *releasecheck.Check
		want error
	}{
		{"not rejected", func() *releasecheck.Check {
			return releasecheck.NewCandidate("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
		}, releasecheck.ErrNotRejected},
		{"not healable", func() *releasecheck.Check {
			r := releasecheck.NewCandidate("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
			require.NoError(t, r.TransitionToParsing(now))
			require.NoError(t, r.Fail("invalid_artifact", "", nil, now))
			r.SetRejectionPayload([]byte(`{}`))
			return r
		}, handlers.ErrNotHealable},
		{"an upstream that could not be copied is not healable", func() *releasecheck.Check {
			r := releasecheck.NewCandidate("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
			require.NoError(t, r.TransitionToParsing(now))
			require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, []string{"analytics.b"}, now))
			require.NoError(t, r.Fail(string(pkg_model.RejectReasonUpstreamCloneFailed), "", []string{"analytics.b"}, now))
			r.SetRejectionPayload([]byte(`{"stage":"validation","reason":"upstream_clone_failed"}`))
			return r
		}, handlers.ErrNotHealable},
		{"parse invalid_sql is healable (fails later on no payload)", func() *releasecheck.Check {
			r := releasecheck.NewCandidate("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
			require.NoError(t, r.TransitionToParsing(now))
			require.NoError(t, r.Fail("invalid_sql", "", []string{"a.b"}, now))
			return r
		}, handlers.ErrNotRetryable},
		{"no stored payload", func() *releasecheck.Check { return rejectedRelease(t, "rel-1", "") }, handlers.ErrNotRetryable},
		{"rounds exhausted", func() *releasecheck.Check {
			r := rejectedRelease(t, "rel-1", `{}`)
			_, _ = r.StartRemediationRound(now)
			_, _ = r.StartRemediationRound(now)
			return r
		}, releasecheck.ErrRoundsExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, store := newDeps(now)
			store.SeedRelease(tc.rel())
			_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
			require.ErrorIs(t, err, tc.want)
			require.Empty(t, outboxEntries(store))
		})
	}
	t.Run("unknown release", func(t *testing.T) {
		deps, _ := newDeps(now)
		_, err := handlers.RetryRemediation(context.Background(), deps, "nope")
		require.ErrorIs(t, err, handlers.ErrReleaseNotFound)
	})
}

func TestRetryRemediation_FixReaderUnavailableIsAnError(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{}`))
	deps.Fixes = &fakeFixes{err: errors.New("grpc unavailable")}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrFixReaderUnavailable)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound())
}

func TestRetryRemediation_RoundOneWithNoRowsIsInProgress(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: nil}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrRetryInProgress)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_SecondPostBeforeNewRoundRowsIsRefused(t *testing.T) {
	seedNow := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	r := rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`)
	_, err := r.StartRemediationRound(seedNow)
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())
	store.SeedRelease(r)
	// Only round 1's terminal fix exists — the retry that bumped the
	// release to round 2 has not yet produced round 2's first row.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "escalated", RemediationRound: 1},
	}}

	_, err = handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrRetryInProgress)

	got, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, got.RemediationRound(), "round unchanged")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

// TestRetryRemediation_ReleaseCheckIsNotAnID verifies that a fix's release check
// check's id is refused the same way an unknown id is: a fix's release check is not a
// release. A fix's Fail always lands on StatusFailed, never
// StatusRejected (the release_checks_kind_status_check constraint
// forbids the row existing otherwise), so this rehydrates the row directly to
// exercise the kind guard defensively, the same way a stray row would.
func TestRetryRemediation_ReleaseCheckIsNotAnID(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, store := newDeps(now)
	r := releasecheck.Rehydrate(releasecheck.RehydrateInput{
		ID:                "rel-1",
		Kind:              releasecheck.KindFix,
		Status:            releasecheck.StatusRejected,
		ChangedProject:    "finance",
		VerifiesReleaseID: "rel-orig",
		Attempt:           1,
		RejectionPayload:  []byte(`{"reason":"compile_failed"}`),
		CreatedAt:         now,
	})
	store.SeedRelease(r)

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrReleaseNotFound)
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_OpenPROnEarlierAttemptRefuses(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Attempt 1's PR is still open; attempt 2 (the node's latest attempt) has
	// already failed. The latest-attempt check alone would miss attempt 1 —
	// the retry must still refuse on it.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PullRequests: []ports.FixPR{{PRState: "open", PRURL: "https://x/pr/9"}}, RemediationRound: 1},
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "failed", RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.FixID)
	require.Equal(t, "https://x/pr/9", open.PRURL)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_RejectedPRDoesNotBlockARound(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// The node's latest attempt in the current round is proposed, but the PR
	// it opened was closed without merging: that attempt is a dead end, not an
	// open one, so it must not block a new round.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PullRequests: []ports.FixPR{{PRState: "rejected"}}, RemediationRound: 1},
	}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())
}

func TestRetryRemediation_ProposedWithNoPRStillBlocksARound(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Same shape as the rejected-PR case, but no PR has been opened at all —
	// the attempt is still a live fix a human could review, so it must
	// still refuse the retry.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.FixID)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
}

func TestRetryRemediation_NamesTheLowestNodeIDWhenSeveralAreOpen(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{
		{ID: "p-marketing", NodeID: "marketing", Attempt: 1, Status: "escalated", PullRequests: []ports.FixPR{{PRState: "open", PRURL: "https://x/pr/marketing"}}, RemediationRound: 1},
		{ID: "p-finance", NodeID: "finance", Attempt: 1, Status: "escalated", PullRequests: []ports.FixPR{{PRState: "open", PRURL: "https://x/pr/finance"}}, RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p-finance", open.FixID, "finance sorts before marketing")
	require.Equal(t, "https://x/pr/finance", open.PRURL)
}

func TestRetryRemediation_MultiProjectProposedOneProjectStillOpenBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// A batched attempt opened one PR per owning project: project "a"'s PR was
	// rejected, project "b"'s is still open. The still-open "b" PR must block
	// the retry, and the refusal must name that open PR rather than the
	// rejected one.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRProjects: []string{"a", "b"},
		PullRequests: []ports.FixPR{
			{Project: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Project: "b", PRState: "open", PRURL: "https://x/pr/b"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.FixID)
	require.Equal(t, "https://x/pr/b", open.PRURL, "names the still-open PR, not the rejected one")

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_MultiProjectMergedPRAcrossRoundsBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// The attempt escalated (its status blocks nothing on the current-round
	// check), but one of its per-project PRs merged while another was rejected.
	// A merged fix already landed for a project, so a new round would
	// duplicate it.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "escalated",
		PRProjects: []string{"a", "b"},
		PullRequests: []ports.FixPR{
			{Project: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Project: "b", PRState: "merged", PRURL: "https://x/pr/b"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.FixID)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_MultiProjectBothPRsRejectedIsADeadEnd(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Every owning project's PR was rejected: the attempt is a genuine dead end,
	// so a new round is allowed.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRProjects: []string{"a", "b"},
		PullRequests: []ports.FixPR{
			{Project: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Project: "b", PRState: "rejected", PRURL: "https://x/pr/b"},
		},
		RemediationRound: 1,
	}}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())
}

func TestRetryRemediation_MultiProjectOwningProjectWithoutPRBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Two owning projects, but only project "a" has opened (and had rejected) a
	// PR; project "b" has no PR row yet, so its fix could still land. The attempt
	// is a dead end only once every owning project's PR is rejected, so it must
	// still block the retry.
	deps.Fixes = &fakeFixes{items: []ports.FixSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRProjects: []string{"a", "b"},
		PullRequests: []ports.FixPR{
			{Project: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrFixOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.FixID)
	require.Equal(t, "https://x/pr/a", open.PRURL, "with every PR rejected, the refusal names the first one")

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

// A fix that names no owning project is one group named by the empty string:
// with no pull request yet it can still land, so it blocks the retry, and
// once that group's pull request is rejected it is a dead end.
func TestRetryRemediation_FixNamingNoProjectIsOneGroup(t *testing.T) {
	t.Run("no pull request blocks", func(t *testing.T) {
		deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
		store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
		deps.Fixes = &fakeFixes{items: []ports.FixSummary{
			{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", RemediationRound: 1},
		}}

		_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
		var open handlers.ErrFixOpen
		require.ErrorAs(t, err, &open)
		require.Equal(t, "p1", open.FixID)
		require.Empty(t, open.PRURL, "a fix with no pull request names none")
	})

	t.Run("its rejected pull request is a dead end", func(t *testing.T) {
		deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
		store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
		deps.Fixes = &fakeFixes{items: []ports.FixSummary{{
			ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
			PullRequests:     []ports.FixPR{{Project: "", PRState: "rejected", PRURL: "https://x/pr/1"}},
			RemediationRound: 1,
		}}}

		res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
		require.NoError(t, err)
		require.Equal(t, 2, res.RemediationRound)
	})
}

func TestIsHealableReason(t *testing.T) {
	for _, reason := range []string{"compile_failed", "seed_build_failed", "validation_failed", "duplicate_table", "invalid_sql", "unqualified_reference"} {
		assert.True(t, handlers.IsHealableReason(reason), reason)
	}
	for _, reason := range []string{"invalid_artifact", "internal_error", "parse_rehearsal_failed", "artifact_upload_failed", "unbuildable_cross_project_upstream", "nothing_to_validate", "upstream_clone_failed", ""} {
		assert.False(t, handlers.IsHealableReason(reason), reason)
	}
}
