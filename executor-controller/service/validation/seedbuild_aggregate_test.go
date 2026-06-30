package validation_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/validation"
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
	require.NoError(t, dep.MarkDeployed(time.Now()))
	require.NoError(t, dep.RecordOutcome(outcome, "", "", time.Now()))
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
	assert.Equal(t, streams.ValidationCompletedV1, outbox2.last.StreamName)
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
	require.NoError(t, dep.MarkDeployed(time.Now()))
	require.NoError(t, dep.RecordOutcome(outcome, "", "", time.Now()))
	return dep
}

// modeScopedDepRepo serves pending-count and results per mode, asserting the
// aggregate threads the right mode through every query.
type modeScopedDepRepo struct {
	byMode map[model.Mode][]*model.Deployment
}

func (r *modeScopedDepRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *modeScopedDepRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
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
