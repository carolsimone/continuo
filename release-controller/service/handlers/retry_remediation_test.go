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
	deps.Proposals = &fakeProposals{items: []ports.ProposalSummary{{NodeID: "finance", Attempt: 3, Status: "escalated"}}}

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
	require.Error(t, err)

	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	require.Equal(t, 1, r.RemediationRound())
}
