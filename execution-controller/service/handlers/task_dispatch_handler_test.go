package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noJobs is a JobCache that holds no Job.
type noJobs struct{}

func (noJobs) Synced() bool                        { return true }
func (noJobs) Observe(string) model.JobObservation { return model.JobObservation{} }

// recordingOutbox keeps every outbox entry written.
type recordingOutbox struct {
	outbox.Repository
	created []*outbox.Entry
}

func (r *recordingOutbox) Create(_ context.Context, e *outbox.Entry) error {
	r.created = append(r.created, e)
	return nil
}

// A new attempt delivered to two replicas at once: the delivery whose insert
// loses the race answers with the report the winner stored. It writes exactly
// that report, no queued report of its own, and stores no deployment of its
// own, so the dispatcher requests no second Job.
func TestTaskDispatch_ALostInsertRaceResendsTheStoredReport(t *testing.T) {
	evt := events.TaskDispatch{RunID: uuid.New(), TaskID: uuid.New(), Attempt: 0,
		KnownStatus: pkgmodel.AttemptStatusDispatched, ScheduleName: "daily", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", NodeType: "dbt-model", Operation: "run", ImageTag: "svc:1",
		JobName: "svc-sch-tbl-7f3e1a2b", TimeoutSeconds: 3600}
	cmd := evt.Command()
	accepted := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	started := accepted.Add(time.Minute)
	winner := model.NewDeployment(cmd, nil, accepted)
	winner.Observe(model.AttemptFacts{StartedAt: &started})
	winner.RecordReport(winner.ReportOf(pkgmodel.AttemptStatusRunning, ""))
	key := attemptKey(cmd.TaskID, cmd.Attempt)
	repo := &stubDeploymentsRepo{storedByRace: map[string]*model.Deployment{key: winner}}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, out.created, 1, "exactly one report is written")
	want, err := json.Marshal(serialization.ReportFromDomain(cmd, *winner.LastReport()))
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(out.created[0].Payload), "the report the winner stored")
	assert.Equal(t, 1, repo.addAttempts, "the attempt was offered once")
	assert.Equal(t, map[string]*model.Deployment{key: winner}, repo.stored,
		"only the winner's deployment is stored, so no second Job is requested")
	assert.Empty(t, repo.added)
}
