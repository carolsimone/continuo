package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/require"
)

// rejectedRelease builds a release that reached StatusRejected with reason
// "compile_failed" and, when payload is non-empty, a stored rejection payload.
func rejectedRelease(t *testing.T, id, payload string) *release.Release {
	t.Helper()
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	r := release.New(id, "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToCompiling(now))
	require.NoError(t, r.TransitionToRejected("compile_failed", "", []string{"finance"}, now))
	if payload != "" {
		r.SetRejectionPayload([]byte(payload))
	}
	return r
}

func TestRetryRemediation_DeadEndStartsRoundTwo(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"release_id":"rel-1","stage":"compile","reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{NodeID: "finance", Attempt: 3, Status: "escalated", RemediationRound: 1}}}

	res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, res.RemediationRound)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, r.RemediationRound())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	e := entries[0]
	require.Equal(t, streams.RemediationRetryRequestedV1, e.StreamName)
	require.Equal(t, "remediation_retry_requested", e.EventType)
	var body map[string]any
	require.NoError(t, json.Unmarshal(e.Payload, &body))
	require.EqualValues(t, 2, body["remediation_round"])
	require.Equal(t, "compile_failed", body["reason"])
}

func TestRetryRemediation_RefusesWhenAProposalIsOpen(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{NodeID: "finance", Attempt: 1, Status: "failed"},
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "proposed", PRState: "open", PRURL: "https://x/pr/7"},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p2", open.ProposalID)
	require.Equal(t, "https://x/pr/7", open.PRURL)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_RefusesInFlightAttempt(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{ID: "p1", NodeID: "finance", Attempt: 1, Status: "generating"}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorAs(t, err, new(handlers.ErrProposalOpen))
}

func TestRetryRemediation_Refusals(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		rel  func() *release.Release
		want error
	}{
		{"not rejected", func() *release.Release {
			return release.New("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
		}, release.ErrNotRejected},
		{"not healable", func() *release.Release {
			r := release.New("rel-1", "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
			require.NoError(t, r.TransitionToParsing(now))
			require.NoError(t, r.TransitionToRejected("parse_failed", "", nil, now))
			r.SetRejectionPayload([]byte(`{}`))
			return r
		}, handlers.ErrNotHealable},
		{"no stored payload", func() *release.Release { return rejectedRelease(t, "rel-1", "") }, handlers.ErrNotRetryable},
		{"rounds exhausted", func() *release.Release {
			r := rejectedRelease(t, "rel-1", `{}`)
			_, _ = r.StartRemediationRound(now)
			_, _ = r.StartRemediationRound(now)
			return r
		}, release.ErrRoundsExhausted},
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

func TestRetryRemediation_ProposalReaderUnavailableIsAnError(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{}`))
	deps.Proposals = &fakeProposals{err: errors.New("grpc unavailable")}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrProposalReaderUnavailable)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound())
}

func TestRetryRemediation_RoundOneWithNoRowsIsInProgress(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: nil}

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
	// Only round 1's terminal proposal exists — the retry that bumped the
	// release to round 2 has not yet produced round 2's first row.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "escalated", RemediationRound: 1},
	}}

	_, err = handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrRetryInProgress)

	got, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 2, got.RemediationRound(), "round unchanged")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_ShadowReleaseIsNotHealable(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, store := newDeps(now)
	r := release.New("rel-1", "finance", "abc", false, true, "o/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToCompiling(now))
	require.NoError(t, r.TransitionToRejected("compile_failed", "", []string{"finance"}, now))
	r.SetRejectionPayload([]byte(`{"reason":"compile_failed"}`))
	store.SeedRelease(r)

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	require.ErrorIs(t, err, handlers.ErrNotHealable)
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_OpenPROnEarlierAttemptRefuses(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Attempt 1's PR is still open; attempt 2 (the node's latest attempt) has
	// already failed. The latest-attempt check alone would miss attempt 1 —
	// the retry must still refuse on it.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PRState: "open", PRURL: "https://x/pr/9", RemediationRound: 1},
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "failed", RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.ProposalID)
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
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PRState: "rejected", RemediationRound: 1},
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
	// the attempt is still a live proposal a human could review, so it must
	// still refuse the retry.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PRState: "", RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.ProposalID)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
}

func TestRetryRemediation_NamesTheLowestNodeIDWhenSeveralAreOpen(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
		{ID: "p-marketing", NodeID: "marketing", Attempt: 1, Status: "escalated", PRState: "open", PRURL: "https://x/pr/marketing", RemediationRound: 1},
		{ID: "p-finance", NodeID: "finance", Attempt: 1, Status: "escalated", PRState: "open", PRURL: "https://x/pr/finance", RemediationRound: 1},
	}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p-finance", open.ProposalID, "finance sorts before marketing")
	require.Equal(t, "https://x/pr/finance", open.PRURL)
}

func TestRetryRemediation_MultiServiceProposedOneServiceStillOpenBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// A batched attempt opened one PR per owning service: service "a"'s PR was
	// rejected, service "b"'s is still open. The singular PRState mirrors the
	// first service's row ("rejected"); reading it alone would wrongly call the
	// whole attempt a dead end. The still-open "b" PR must block the retry, and
	// the refusal must name that open PR rather than the rejected one.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRState: "rejected", PRURL: "https://x/pr/a",
		PRServices: []string{"a", "b"},
		PullRequests: []ports.ProposalPR{
			{Service: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Service: "b", PRState: "open", PRURL: "https://x/pr/b"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.ProposalID)
	require.Equal(t, "https://x/pr/b", open.PRURL, "names the still-open PR, not the rejected one")

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_MultiServiceMergedPRAcrossRoundsBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// The attempt escalated (its status blocks nothing on the current-round
	// check), but one of its per-service PRs merged while another was rejected.
	// A merged fix already landed for a service, so a new round would duplicate
	// it — the singular PRState ("rejected", mirroring the first service) hides
	// the merged PR from an aggregate-blind reader.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "escalated",
		PRState: "rejected", PRURL: "https://x/pr/a",
		PRServices: []string{"a", "b"},
		PullRequests: []ports.ProposalPR{
			{Service: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Service: "b", PRState: "merged", PRURL: "https://x/pr/b"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.ProposalID)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_MultiServiceBothPRsRejectedIsADeadEnd(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Every owning service's PR was rejected: the attempt is a genuine dead end,
	// so a new round is allowed.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRState: "rejected", PRURL: "https://x/pr/a",
		PRServices: []string{"a", "b"},
		PullRequests: []ports.ProposalPR{
			{Service: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
			{Service: "b", PRState: "rejected", PRURL: "https://x/pr/b"},
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

func TestRetryRemediation_MultiServiceOwningServiceWithoutPRBlocks(t *testing.T) {
	deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
	store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
	// Two owning services, but only service "a" has opened (and had rejected) a
	// PR; service "b" has no PR row yet, so its fix could still land. The attempt
	// is a dead end only once every owning service's PR is rejected, so it must
	// still block the retry.
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{
		ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed",
		PRState: "rejected", PRURL: "https://x/pr/a",
		PRServices: []string{"a", "b"},
		PullRequests: []ports.ProposalPR{
			{Service: "a", PRState: "rejected", PRURL: "https://x/pr/a"},
		},
		RemediationRound: 1,
	}}}

	_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
	var open handlers.ErrProposalOpen
	require.ErrorAs(t, err, &open)
	require.Equal(t, "p1", open.ProposalID)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound(), "no round spent")
	require.Empty(t, outboxEntries(store), "nothing emitted")
}

func TestRetryRemediation_LegacyProposalWithoutPerServicePRs(t *testing.T) {
	// A pre-split proposal carries no PullRequests, only the singular fields, and
	// must decide exactly as it did before the per-service split.
	t.Run("proposed with singular rejected is a dead end", func(t *testing.T) {
		deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
		store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
		deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
			{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PRState: "rejected", RemediationRound: 1},
		}}

		res, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
		require.NoError(t, err)
		require.Equal(t, 2, res.RemediationRound)
	})

	t.Run("proposed with singular open blocks", func(t *testing.T) {
		deps, store := newDeps(time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC))
		store.SeedRelease(rejectedRelease(t, "rel-1", `{"reason":"compile_failed"}`))
		deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{
			{ID: "p1", NodeID: "finance", Attempt: 1, Status: "proposed", PRState: "open", PRURL: "https://x/pr/legacy", RemediationRound: 1},
		}}

		_, err := handlers.RetryRemediation(context.Background(), deps, "rel-1")
		var open handlers.ErrProposalOpen
		require.ErrorAs(t, err, &open)
		require.Equal(t, "p1", open.ProposalID)
		require.Equal(t, "https://x/pr/legacy", open.PRURL)
	})
}
