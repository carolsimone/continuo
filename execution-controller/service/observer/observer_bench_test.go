//go:build integration

package observer

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/require"
)

// BenchmarkProcess_SettlesAnEndedAttempt measures the terminal worker on the
// work that ends every attempt: Process of the name of a production attempt
// whose cached Job and pod have succeeded. It reads the pod log, uploads it
// and, in one transaction, records what the cache shows, classifies the
// attempt, writes its succeeded report and saves the deployment. The
// Kubernetes ports and the uploader are fakes, so the figure is the worker's
// own cost: its database reads and writes and the work around them. Before
// each iteration one started deployment is stored, outside the timer. Run it
// with
//
//	go test -tags integration -run '^$' -bench SettlesAnEndedAttempt -benchtime 200x ./service/observer/
func BenchmarkProcess_SettlesAnEndedAttempt(b *testing.B) {
	ctx := context.Background()
	h := newHarness(b)
	h.logs.full = "1 of 1 OK created sql table model e2e_schema.ftable_e"
	ended := model.JobObservation{JobPresent: true, JobSucceeded: true, Pods: []model.PodObservation{
		endedPod(model.PodSucceeded, h.now.Add(-50*time.Second), h.now.Add(-20*time.Second), 0, "Completed")}}

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		dep := h.started(b, 0, h.now.Add(-time.Minute))
		h.cache.set(dep.JobName(), ended)
		b.StartTimer()
		require.NoError(b, h.o.Process(ctx, dep.JobName()))
	}
	b.StopTimer()

	var settled, reported int
	require.NoError(b, h.db.Get(&settled,
		`SELECT count(*) FROM deployments WHERE status = $1 AND outcome = 'ok'`, string(model.StatusDone)))
	require.NoError(b, h.db.Get(&reported,
		`SELECT count(*) FROM execution_outbox WHERE event_type = $1`, event.EventTypeTaskStatusReport))
	require.Equalf(b, b.N, settled, "every one of the %d attempts must be settled done with outcome ok; %d were", b.N, settled)
	require.Equalf(b, b.N, reported, "every one of the %d attempts must write exactly one succeeded report; %d reports were written", b.N, reported)
	require.Lenf(b, h.up.uploaded, b.N, "every one of the %d attempts must upload its log once; %d objects were uploaded", b.N, len(h.up.uploaded))
}
