//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_FIFO_SerializesConcurrentCandidates(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// 5 candidates posted "concurrently". Each goroutine uses its own UnitOfWork
	// to mirror production behaviour: every HTTP request receives its own UoW
	// instance, so no per-request state is shared across goroutines.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r%02d", i)
			goroutineDeps := &handlers.Deps{
				UoW:       postgres.NewUnitOfWork(db, slog.Default()),
				Clock:     ports.SystemClock{},
				Telemetry: ports.NoOpTelemetry{},
				Logger:    slog.Default(),
			}
			_ = handlers.ReceiveCandidate(context.Background(), goroutineDeps, handlers.ReceiveCandidateInput{
				ReleaseID: id, ChangedNodeIDs: []string{"a"}, ImageTags: map[string]string{"service-1": "sha-" + id}, ManifestsURI: "u",
			})
		}(i)
	}
	wg.Wait()

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	active, err := deps.UoW.ReleaseRepo().ActiveRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, active, "exactly one release must be active")
	assert.Equal(t, release.StatusParsing, active.Status())

	// Calling AdvanceQueue again must not start a second active release
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	active2, _ := deps.UoW.ReleaseRepo().ActiveRelease(context.Background())
	assert.Equal(t, active.ID(), active2.ID(), "no second active release while first is in flight")
}
