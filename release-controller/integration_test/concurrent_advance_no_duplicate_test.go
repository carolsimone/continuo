//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression for the audit's Important #1 finding: AdvanceQueue called
// concurrently from multiple paths (HTTP POST + stream-consumer ack) must
// not double-promote the same queued release. The tx-scoped advisory lock
// in UnitOfWork.LockReleaseQueue serialises the critical section so that
// for N concurrent calls against a single Received candidate, exactly one
// promotes the release and exactly one compile.requested:v1 outbox row
// is written.
func TestIntegration_ConcurrentAdvance_PromotesAtMostOnce(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// Seed a single Received candidate.
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "service-1",
		ReleaseID: "rA",
		ImageTag:  "sha-rA",
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe1234",
	}))

	// Truncate any outbox rows from setup() so the count assertion below is
	// scoped to what concurrent AdvanceQueue produces.
	_, err := db.Exec(`TRUNCATE release_controller_outbox RESTART IDENTITY`)
	require.NoError(t, err)

	// Fire 10 concurrent AdvanceQueue calls.
	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- handlers.AdvanceQueue(context.Background(), deps)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// Exactly one outbox row for compile.requested:v1 — the lock prevented
	// duplicate promotion. AdvanceQueue now activates a Received release into
	// Compiling and emits compile.requested:v1 (the compile leg runs before
	// parsing); release.requested:v1 is emitted later, by HandleCompileResult.
	var count int
	require.NoError(t, db.Get(&count,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`,
		streams.CompileRequestedV1,
	))
	assert.Equal(t, 1, count, "AdvanceQueue must not write duplicate compile.requested:v1 entries under concurrent invocation")

	// And the release transitioned to Compiling exactly once.
	r, err := deps.NewUoW().ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusCompiling, r.Status())
}
