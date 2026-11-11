package validation_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedBuildDep builds a terminal seed-build deployment with the given outcome.
func seedBuildDep(t *testing.T, releaseID, nodeID, outcome string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		CandidateSchema: "_candidate_" + releaseID,
		JobName:         "seed-" + nodeID,
		NodeType:        "dbt-seed",
		ImageTag:        "t",
	}
	dep := model.NewSeedBuildDeployment(cmd, nil, time.Now())
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(time.Now()))
	require.NoError(t, dep.RecordOutcome(outcome, "", "", "", time.Now()))
	return dep
}

func TestEmitSeedBuildAggregate_AllOk_EmitsStatusOk(t *testing.T) {
	dep := seedBuildDep(t, "rel", "seed.a", "ok")
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), depRepo, outboxRepo, aggRepo, "rel", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last, "expected a seed.build.completed:v1 outbox entry")
	assert.Equal(t, streams.SeedBuildCompletedV1, outboxRepo.last.StreamName)
	assert.Equal(t, "seed_build_completed", outboxRepo.last.EventType)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"ok"`)
	assert.Contains(t, string(outboxRepo.last.Payload), `"candidate_schema":"_candidate_rel"`)
	assert.Contains(t, string(outboxRepo.last.Payload), `"release_id":"rel"`)
}

func TestEmitSeedBuildAggregate_AnyFailed_EmitsStatusFailed(t *testing.T) {
	ok := seedBuildDep(t, "rel", "seed.a", "ok")
	bad := seedBuildDep(t, "rel", "seed.b", "failed")
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{ok, bad}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), depRepo, outboxRepo, aggRepo, "rel", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"failed"`)
}

// perNodeOf decodes the per_node entries of an aggregate event's payload.
func perNodeOf(t *testing.T, payload []byte) []map[string]any {
	t.Helper()
	var p struct {
		PerNode []map[string]any `json:"per_node"`
	}
	require.NoError(t, json.Unmarshal(payload, &p))
	return p.PerNode
}

func TestEmitSeedBuildAggregate_CarriesEachNodesRunDuration(t *testing.T) {
	t0 := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	cmd := command.ValidationDeployTask{
		ReleaseID: "rel", NodeID: "seed.a", CandidateSchema: "_candidate_rel",
		JobName: "seed-a", NodeType: "dbt-seed", ImageTag: "t",
	}
	dep := model.NewSeedBuildDeployment(cmd, nil, t0)
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(t0))
	require.NoError(t, dep.RecordOutcome("ok", "", "", "", t0.Add(1500*time.Millisecond)))
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}

	require.NoError(t, validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), depRepo, outboxRepo, &fakeAggRepo{won: true}, "rel", time.Now()))

	require.NotNil(t, outboxRepo.last)
	perNode := perNodeOf(t, outboxRepo.last.Payload)
	require.Len(t, perNode, 1)
	assert.Equal(t, float64(1500), perNode[0]["duration_ms"])
}

func TestEmitSeedBuildAggregate_ANodeThatNeverStartedCarriesNoDuration(t *testing.T) {
	t0 := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	cmd := command.ValidationDeployTask{
		ReleaseID: "rel", NodeID: "seed.a", CandidateSchema: "_candidate_rel",
		JobName: "seed-a", NodeType: "dbt-seed", ImageTag: "t",
	}
	dep := model.NewSeedBuildDeployment(cmd, nil, t0)
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.FailSeedBuild("not deployable", t0.Add(time.Second)))
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}

	require.NoError(t, validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), depRepo, outboxRepo, &fakeAggRepo{won: true}, "rel", time.Now()))

	require.NotNil(t, outboxRepo.last)
	perNode := perNodeOf(t, outboxRepo.last.Payload)
	require.Len(t, perNode, 1)
	assert.Equal(t, "failed", perNode[0]["status"])
	assert.NotContains(t, perNode[0], "duration_ms", "a node that never started has no run time to report")
}

func TestEmitSeedBuildAggregate_ASkewedOutcomeCarriesNoDuration(t *testing.T) {
	t0 := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	cmd := command.ValidationDeployTask{
		ReleaseID: "rel", NodeID: "seed.a", CandidateSchema: "_candidate_rel",
		JobName: "seed-a", NodeType: "dbt-seed", ImageTag: "t",
	}
	dep := model.NewSeedBuildDeployment(cmd, nil, t0)
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(t0.Add(5*time.Second)))
	require.NoError(t, dep.RecordOutcome("ok", "", "", "", t0))
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}

	require.NoError(t, validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), depRepo, outboxRepo, &fakeAggRepo{won: true}, "rel", time.Now()))

	require.NotNil(t, outboxRepo.last)
	perNode := perNodeOf(t, outboxRepo.last.Payload)
	require.Len(t, perNode, 1)
	assert.NotContains(t, perNode[0], "duration_ms", "an outcome before the start must not become a negative duration")
}

func TestEmitSeedBuildAggregate_UsesSeedBuildNamespace(t *testing.T) {
	// A distinct namespace from the validation one keeps the two legs' outbox
	// aggregate_ids independent for the same release.
	assert.NotEqual(t, validation.DedupNamespace, validation.SeedBuildDedupNamespace,
		"seed-build namespace must differ from validation namespace")
}

// TestSeedBuildAggregate_ScopedToMode asserts the seed-build aggregate counts and
// lists ONLY ModeSeedBuild rows: a co-existing ModeValidation row for the same
// release must not affect the seed-build pending count or per-node results, and
// vice versa. The mode-aware fake records which mode each repo call was asked for.
func TestSeedBuildAggregate_ScopedToMode(t *testing.T) {
	seedOk := seedBuildDep(t, "rel", "seed.a", "ok")
	valRow := validationDepForMode(t, "rel", "node.x", "failed")

	repo := &modeScopedDepRepo{
		byMode: map[model.Mode][]*model.Deployment{
			model.ModeSeedBuild:  {seedOk},
			model.ModeValidation: {valRow},
		},
	}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	// Seed-build leg: only the seed row counts -> status ok despite the failed validation row.
	err := validation.EmitSeedBuildAggregateIfComplete(
		context.Background(), repo, outboxRepo, aggRepo, "rel", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last)
	assert.Equal(t, streams.SeedBuildCompletedV1, outboxRepo.last.StreamName)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"ok"`,
		"seed-build aggregate must ignore the failed ModeValidation row")
	assert.NotContains(t, string(outboxRepo.last.Payload), "node.x",
		"validation node must not leak into seed-build per_node")

	// Validation leg: only the validation row counts -> status failed despite the ok seed row.
	outbox2 := &captureOutbox{}
	err = validation.EmitValidationAggregateIfComplete(
		context.Background(), repo, outbox2, &fakeAggRepo{won: true},
		validation.DedupNamespace, "rel", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outbox2.last)
	assert.Equal(t, streams.ValidationResultV1, outbox2.last.StreamName)
	assert.Contains(t, string(outbox2.last.Payload), `"aggregate_status":"failed"`,
		"validation aggregate must ignore the ok ModeSeedBuild row")
	assert.NotContains(t, string(outbox2.last.Payload), "seed.a",
		"seed node must not leak into validation per_node_results")
}

func validationDepForMode(t *testing.T, releaseID, nodeID, outcome string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		CandidateSchema: "_candidate_" + releaseID,
		JobName:         "val-" + nodeID,
		NodeType:        "dbt-model",
		ImageTag:        "t",
	}
	dep := model.NewValidationDeployment(cmd, nil, time.Now(), false)
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(time.Now()))
	require.NoError(t, dep.RecordOutcome(outcome, "", "", "", time.Now()))
	return dep
}

// modeScopedDepRepo serves pending-count and results per mode, asserting the
// aggregate threads the right mode through every query.
type modeScopedDepRepo struct {
	byMode map[model.Mode][]*model.Deployment
}

func (r *modeScopedDepRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *modeScopedDepRepo) LockOpenOfRun(context.Context, string) ([]*model.Deployment, error) {
	return nil, nil
}

func (r *modeScopedDepRepo) LoadByJobName(context.Context, string) (*model.Deployment, error) {
	return nil, sql.ErrNoRows
}

func (r *modeScopedDepRepo) Save(context.Context, *model.Deployment) error { return nil }
func (r *modeScopedDepRepo) GetByReleaseNode(context.Context, string, string, model.Mode) (*model.Deployment, error) {
	return nil, nil
}
func (r *modeScopedDepRepo) PendingValidationCount(_ context.Context, _ string, mode model.Mode) (int, error) {
	// Every row in this fake is terminal, so nothing is pending for either mode.
	_ = r.byMode[mode]
	return 0, nil
}
func (r *modeScopedDepRepo) ListValidationResults(_ context.Context, _ string, mode model.Mode) ([]*model.Deployment, error) {
	return r.byMode[mode], nil
}
func (r *modeScopedDepRepo) ListValidationByRelease(_ context.Context, _ string, mode model.Mode) ([]*model.Deployment, error) {
	return r.byMode[mode], nil
}

func (*modeScopedDepRepo) AddAttempt(context.Context, *model.Deployment) (bool, error) {
	panic("AddAttempt not used by these tests")
}
func (*modeScopedDepRepo) LoadByTaskAttempt(context.Context, string, int) (*model.Deployment, bool, error) {
	panic("LoadByTaskAttempt not used by these tests")
}
func (*modeScopedDepRepo) HasAttemptAfter(context.Context, string, int) (bool, error) {
	panic("HasAttemptAfter not used by these tests")
}
