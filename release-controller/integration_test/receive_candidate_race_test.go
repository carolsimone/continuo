//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two first submissions of one release id, with different image tags, race
// through ReceiveCandidate against real Postgres. Both may find no row on
// their Load; the insert-only Create lets exactly one write. The other must
// be answered ErrReleaseIDConflict and must leave the winner's row as the
// winner wrote it.
func TestIntegration_ConcurrentFirstSubmissionsOfOneIDHaveOneWinner(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	for round := 0; round < 20; round++ {
		_, err := db.Exec("DELETE FROM release_pipeline_runs WHERE run_id = 'race-1'")
		require.NoError(t, err)

		tags := []string{"img:a", "img:b"}
		errs := make([]error, len(tags))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, tag := range tags {
			wg.Add(1)
			go func(i int, tag string) {
				defer wg.Done()
				<-start
				errs[i] = handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
					Service: "service-1", ReleaseID: "race-1", ImageTag: tag,
					Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
				})
			}(i, tag)
		}
		close(start)
		wg.Wait()

		winner := -1
		for i, err := range errs {
			if err == nil {
				require.Equal(t, -1, winner, "round %d: both submissions were accepted", round)
				winner = i
				continue
			}
			require.ErrorIs(t, err, handlers.ErrReleaseIDConflict, "round %d", round)
		}
		require.NotEqual(t, -1, winner, "round %d: neither submission was accepted: %v", round, errs)

		r, err := deps.NewUoW().RunRepo().Get(context.Background(), "race-1")
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, tags[winner], r.ImageTags()["service-1"], "round %d", round)
		assert.Len(t, r.Transitions(), 1, "round %d: the loser must not rewrite the winner's history", round)
	}
}
