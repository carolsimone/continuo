//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_FIFO_SerializesConcurrentCandidates(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// 5 candidates posted "concurrently". Each call to deps.NewUoW() returns a
	// fresh UnitOfWork, mirroring production: every HTTP request owns its own
	// UoW instance with isolated tx state.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r%02d", i)
			_ = handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
				Service: "service-1", ReleaseID: id, ImageTag: "sha-" + id,
			})
		}(i)
	}
	wg.Wait()

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	active, err := deps.NewUoW().ReleaseRepo().ActiveRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, active, "exactly one release must be active")
	assert.Equal(t, release.StatusParsing, active.Status())

	// Calling AdvanceQueue again must not start a second active release
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	active2, _ := deps.NewUoW().ReleaseRepo().ActiveRelease(context.Background())
	assert.Equal(t, active.ID(), active2.ID(), "no second active release while first is in flight")
}
