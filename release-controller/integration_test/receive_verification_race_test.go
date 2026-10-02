//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertSpy records, per submission, whether ReceiveVerification reported its
// run as newly received. Only a submission whose insert took effect emits that
// span, so the spy names the winner of a race.
type insertSpy struct {
	ports.NoOpTelemetry
	mu       sync.Mutex
	inserted []string
}

type submitterKey struct{}

func (s *insertSpy) ReleaseReceived(ctx context.Context, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inserted = append(s.inserted, ctx.Value(submitterKey{}).(string))
}

// Two first submissions of one verification run id, with different image
// tags, race through ReceiveVerification against real Postgres. Both may find
// no row on their Load; the insert-only Create lets exactly one write. A
// verification resubmit is accepted whatever its facts, so both calls
// succeed, but only the winner inserts and the stored row stays exactly as the
// winner wrote it.
func TestIntegration_ConcurrentFirstVerificationSubmissionsOfOneIDHaveOneWinner(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	spy := &insertSpy{}
	deps.Telemetry = spy

	for round := 0; round < 20; round++ {
		_, err := db.Exec("DELETE FROM release_pipeline_runs WHERE run_id = 'verify-race-1'")
		require.NoError(t, err)
		spy.inserted = nil

		tags := []string{"img:a", "img:b"}
		errs := make([]error, len(tags))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, tag := range tags {
			wg.Add(1)
			go func(i int, tag string) {
				defer wg.Done()
				<-start
				ctx := context.WithValue(context.Background(), submitterKey{}, tag)
				errs[i] = handlers.ReceiveVerification(ctx, deps, handlers.ReceiveVerificationInput{
					RunID: "verify-race-1", Service: "service-1", ImageTag: tag, Kind: "dbt",
					VerifiesReleaseID: "rel-0", Attempt: 1,
				})
			}(i, tag)
		}
		close(start)
		wg.Wait()
		require.NoError(t, errs[0], "round %d", round)
		require.NoError(t, errs[1], "round %d", round)

		require.Len(t, spy.inserted, 1, "round %d: exactly one submission may insert the run", round)
		winnerTag := spy.inserted[0]

		r, err := deps.NewUoW().RunRepo().Get(context.Background(), "verify-race-1")
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, pipeline.KindVerification, r.Kind(), "round %d", round)
		assert.Equal(t, winnerTag, r.ImageTags()["service-1"], "round %d: the loser overwrote the winner's row", round)
		assert.Len(t, r.Transitions(), 1, "round %d: the loser must not rewrite the winner's history", round)

		var n int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM release_pipeline_runs WHERE run_id = 'verify-race-1'").Scan(&n))
		assert.Equal(t, 1, n, "round %d", round)
	}
}
