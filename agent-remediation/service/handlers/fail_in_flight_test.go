package handlers

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// TestFailInFlight_FinalizesGeneratingRow verifies the recovery a dropped
// trigger takes: the release's in-flight 'generating' row is finalized to
// 'failed' with the reason recorded, and the unit of work is committed. Without
// it the row reports a fix as forever generating and the release's retry stays
// blocked behind that phantom in-flight attempt.
func TestFailInFlight_FinalizesGeneratingRow(t *testing.T) {
	u := newFakeUoW()
	require.NoError(t, u.pr.InsertGenerating(context.Background(), proposal.Proposal{
		ReleaseID: "rel-x", Attempt: 1, Status: proposal.StatusGenerating,
	}))

	d := Deps{NewUoW: func() uow.UnitOfWork { return u }, Logger: slog.Default()}
	n, err := FailInFlight(context.Background(), d, "rel-x", "poison-dropped after exhausting redelivery")

	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, proposal.StatusFailed, u.pr.generating[0].Status)
	require.Equal(t, "poison-dropped after exhausting redelivery", u.pr.generating[0].Rationale)
	require.True(t, u.committed, "the failing write must be committed")
}

// TestFailInFlight_NoGeneratingRowIsNoOp verifies idempotency: a release with no
// in-flight row (already finalized, or never started one) reports zero rows
// moved and no error, so a redundant drop notification is harmless.
func TestFailInFlight_NoGeneratingRowIsNoOp(t *testing.T) {
	u := newFakeUoW()
	d := Deps{NewUoW: func() uow.UnitOfWork { return u }, Logger: slog.Default()}

	n, err := FailInFlight(context.Background(), d, "rel-absent", "poison-dropped")

	require.NoError(t, err)
	require.Equal(t, 0, n)
}
