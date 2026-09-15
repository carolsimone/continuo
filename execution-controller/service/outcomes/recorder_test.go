package outcomes_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// discardLogger is a logger that discards all output, for tests that only
// care about the recorder's return value and side effects.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The stubs below are small in-memory repositories scoped to this package's
// tests. outcomes is a separate package from service/handlers, so it keeps its
// own copies rather than sharing test-only types across package boundaries.

// stubDeploymentsRepo is a configurable in-memory DeploymentRepository. It
// returns byKey[releaseID|nodeID|mode] from GetByReleaseNode (or sql.ErrNoRows
// when absent), and records every Save call.
type stubDeploymentsRepo struct {
	byKey map[string]*model.Deployment
	saved []*model.Deployment
}

func depKey(releaseID, nodeID string, mode model.Mode) string {
	return releaseID + "|" + nodeID + "|" + string(mode)
}

func (r *stubDeploymentsRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *stubDeploymentsRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *stubDeploymentsRepo) Save(_ context.Context, d *model.Deployment) error {
	r.saved = append(r.saved, d)
	return nil
}
func (r *stubDeploymentsRepo) GetByReleaseNode(_ context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error) {
	d, ok := r.byKey[depKey(releaseID, nodeID, mode)]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return d, nil
}
func (r *stubDeploymentsRepo) PendingValidationCount(context.Context, string, model.Mode) (int, error) {
	return 0, nil
}
func (r *stubDeploymentsRepo) ListValidationResults(_ context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error) {
	var out []*model.Deployment
	for _, d := range r.byKey {
		if d.ReleaseID() == releaseID && d.Mode() == mode {
			out = append(out, d)
		}
	}
	return out, nil
}
func (r *stubDeploymentsRepo) ListValidationByRelease(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}

// stubAggRepo always wins the emission claim so the settle gate fires.
type stubAggRepo struct {
	lockCalls  int
	claimCalls int
}

func (r *stubAggRepo) LockRelease(context.Context, string, model.Mode) error {
	r.lockCalls++
	return nil
}
func (r *stubAggRepo) ClaimEmission(context.Context, string, model.Mode, time.Time) (bool, error) {
	r.claimCalls++
	return true, nil
}

// newBlockedValidationDeployment builds a validation deployment in
// status=deployed (ready to receive an outcome) for (releaseID, nodeID). Despite
// the name, it is the "ready" fixture the recorder needs — NewValidationDeployment
// with no upstreams starts pending, and RecordOutcome requires status=deployed, so
// it is driven to deployed via MarkDeployed before being handed to the recorder.
func newBlockedValidationDeployment(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: "tbl", NodeType: "dbt-model",
		ImageTag: "sha-test", JobName: "validate-" + nodeID,
	}
	now := time.Now()
	d := model.NewValidationDeployment(cmd, nil, now, false)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestRecord_ValidationOutcomeIsSavedAndSettled(t *testing.T) {
	dep := newBlockedValidationDeployment(t, "rel-1", "svc.tbl")
	repo := &stubDeploymentsRepo{byKey: map[string]*model.Deployment{depKey("rel-1", "svc.tbl", model.ModeValidation): dep}}
	u := &fakes.FakeUnitOfWork{Deployments: repo, Outbox: &fakes.FakeOutboxRepository{}, ValidationAggregate: &stubAggRepo{}}

	err := outcomes.NewRecorder(discardLogger()).Record(context.Background(), u, outcomes.NodeOutcome{
		Mode: model.ModeValidation, ReleaseID: "rel-1", NodeID: "svc.tbl", Outcome: "ok", DBTLogURI: "logs/x.log",
	})
	require.NoError(t, err)
	require.NotNil(t, dep.OutcomeAt())
	require.Equal(t, "ok", dep.Outcome())
	// rel-1 has exactly this one node, and stubDeploymentsRepo reports zero
	// pending, so the settle writes both the per-node validation.result row and
	// (this being the release's last node) the terminal aggregate row — two
	// entries, both in the same unit of work as the outcome.
	require.Equal(t, 2, u.Outbox.(*fakes.FakeOutboxRepository).CreateCallCount,
		"per-node validation.result row and the terminal aggregate must be written in the same unit of work")
}

func TestRecord_SecondCallIsIdempotent(t *testing.T) {
	dep := newBlockedValidationDeployment(t, "rel-1", "svc.tbl")
	require.NoError(t, dep.RecordOutcome("ok", "", "", "", time.Now()))
	repo := &stubDeploymentsRepo{byKey: map[string]*model.Deployment{depKey("rel-1", "svc.tbl", model.ModeValidation): dep}}
	u := &fakes.FakeUnitOfWork{Deployments: repo, Outbox: &fakes.FakeOutboxRepository{}, ValidationAggregate: &stubAggRepo{}}
	require.NoError(t, outcomes.NewRecorder(discardLogger()).Record(context.Background(), u, outcomes.NodeOutcome{
		Mode: model.ModeValidation, ReleaseID: "rel-1", NodeID: "svc.tbl", Outcome: "failed",
	}))
	assert.Equal(t, 0, u.Outbox.(*fakes.FakeOutboxRepository).CreateCallCount, "redelivery must not re-settle")
	assert.Empty(t, repo.saved, "redelivery must not re-Save")
}

func TestRecord_UnknownReleaseNodeIsNoOp(t *testing.T) {
	repo := &stubDeploymentsRepo{byKey: map[string]*model.Deployment{}}
	u := &fakes.FakeUnitOfWork{Deployments: repo, Outbox: &fakes.FakeOutboxRepository{}, ValidationAggregate: &stubAggRepo{}}

	err := outcomes.NewRecorder(discardLogger()).Record(context.Background(), u, outcomes.NodeOutcome{
		Mode: model.ModeValidation, ReleaseID: "rel-unknown", NodeID: "svc.tbl", Outcome: "ok",
	})
	require.NoError(t, err, "an unknown (release,node) row is logged and ignored, not errored")
	assert.Empty(t, repo.saved)
	assert.Equal(t, 0, u.Outbox.(*fakes.FakeOutboxRepository).CreateCallCount)
}

// newDeployedSeedBuild builds a seed-build deployment in status=deployed, ready
// to receive an outcome, for (releaseID, nodeID).
func newDeployedSeedBuild(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: nodeID, NodeType: "dbt-seed",
		ImageTag: "sha-seed", JobName: "seed-" + nodeID,
	}
	now := time.Now()
	d := model.NewSeedBuildDeployment(cmd, nil, now)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestRecord_SeedBuildOutcomeSettlesAggregateWhenComplete(t *testing.T) {
	dep := newDeployedSeedBuild(t, "rel-1", "seed.shop.fx")
	repo := &stubDeploymentsRepo{byKey: map[string]*model.Deployment{depKey("rel-1", "seed.shop.fx", model.ModeSeedBuild): dep}}
	agg := &stubAggRepo{}
	u := &fakes.FakeUnitOfWork{Deployments: repo, Outbox: &fakes.FakeOutboxRepository{}, ValidationAggregate: agg}

	err := outcomes.NewRecorder(discardLogger()).Record(context.Background(), u, outcomes.NodeOutcome{
		Mode: model.ModeSeedBuild, ReleaseID: "rel-1", NodeID: "seed.shop.fx", Outcome: "ok", DBTLogURI: "s3://logs/fx",
	})
	require.NoError(t, err)
	require.Len(t, repo.saved, 1)
	assert.Equal(t, "ok", repo.saved[0].Outcome())
	assert.Equal(t, 1, agg.claimCalls, "seed-build aggregate gate ran once the seed is terminal")
	entries := u.Outbox.(*fakes.FakeOutboxRepository)
	assert.Equal(t, 1, entries.CreateCallCount, "seed.build.completed:v1 emitted")
}

// newDeployedCompile builds a compile deployment in status=deployed, ready to
// receive an outcome, for (releaseID, nodeID).
func newDeployedCompile(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: nodeID, NodeType: "dbt-model",
		ImageTag: "sha-compile", JobName: "compile-" + nodeID,
	}
	now := time.Now()
	d := model.NewCompileDeployment(cmd, nil, now)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestRecord_CompileOutcomeCarriesFailedContainer(t *testing.T) {
	dep := newDeployedCompile(t, "rel-1", "model.shop.fx")
	repo := &stubDeploymentsRepo{byKey: map[string]*model.Deployment{depKey("rel-1", "model.shop.fx", model.ModeCompile): dep}}
	agg := &stubAggRepo{}
	var captured *outbox.Entry
	outboxRepo := &fakes.FakeOutboxRepository{CreateFunc: func(_ context.Context, e *outbox.Entry) error {
		captured = e
		return nil
	}}
	u := &fakes.FakeUnitOfWork{Deployments: repo, Outbox: outboxRepo, ValidationAggregate: agg}

	err := outcomes.NewRecorder(discardLogger()).Record(context.Background(), u, outcomes.NodeOutcome{
		Mode: model.ModeCompile, ReleaseID: "rel-1", NodeID: "model.shop.fx",
		Outcome: "failed", DBTLogURI: "s3://logs/fx", FailedContainer: "parse-prod",
	})
	require.NoError(t, err)
	require.Len(t, repo.saved, 1)
	assert.Equal(t, "parse-prod", repo.saved[0].FailedContainer())
	require.NotNil(t, captured)
	assert.Equal(t, streams.CompileCompletedV1, captured.StreamName)
	assert.Contains(t, string(captured.Payload), `"failed_container":"parse-prod"`)
}
