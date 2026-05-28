//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/require"
)

// Regression for per-request UoW fix: concurrent handlers must each get their
// own UoW so the per-request tx state never collides.
func TestIntegration_ConcurrentReceiveAndAdvance_NoTxCollision(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := "r" + string(rune('A'+i))
			errs <- handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
				ReleaseID:      id,
				ChangedNodeIDs: []string{"a"},
				ImageTags:      map[string]string{"service-1": "sha-" + id},
				ManifestsURI:   "u",
			})
		}(i)
		go func() {
			defer wg.Done()
			errs <- handlers.AdvanceQueue(context.Background(), deps)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "no handler should see 'transaction already in progress'")
	}
}
