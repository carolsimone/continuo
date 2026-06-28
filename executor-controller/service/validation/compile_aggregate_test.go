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

// compileDep builds a terminal compile deployment with the given outcome.
func compileDep(t *testing.T, releaseID, nodeID, outcome string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		CandidateSchema: "_candidate_" + releaseID,
		JobName:         "compile-" + nodeID,
		NodeType:        "dbt-model",
		ImageTag:        "t",
	}
	dep := model.NewCompileDeployment(cmd, nil, time.Now())
	require.NoError(t, dep.MarkDeployed(time.Now()))
	require.NoError(t, dep.RecordOutcome(outcome, "", "", time.Now()))
	return dep
}

// TestSettleCompileNode_AllOk_EmitsStatusOk pins the critical contract: the
// compile leg emits compile.completed:v1 with the "status" key (NOT
// "aggregate_status") so release-controller's HandleCompileResultInput, which
// decodes Status under json:"status", reads "ok" — a successful compile must not
// decode to "" and be wrongly rejected.
func TestSettleCompileNode_AllOk_EmitsStatusOk(t *testing.T) {
	dep := compileDep(t, "rel", "compile.svc", "ok")
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.SettleCompileNodeTerminal(
		context.Background(), depRepo, outboxRepo, aggRepo,
		"rel", "compile.svc", "ok", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last, "expected a compile.completed:v1 outbox entry")
	assert.Equal(t, streams.CompileCompletedV1, outboxRepo.last.StreamName)
	assert.Equal(t, "compile_completed", outboxRepo.last.EventType)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"ok"`,
		"compile payload MUST use the \"status\" key release-controller reads")
	assert.NotContains(t, string(outboxRepo.last.Payload), `"aggregate_status"`,
		"compile payload MUST NOT use \"aggregate_status\" (release-controller can't read it)")
	assert.Contains(t, string(outboxRepo.last.Payload), `"release_id":"rel"`)
}

// TestSettleCompileNode_Failed_EmitsStatusFailed verifies a failed compile node
// emits status "failed" under the same "status" key.
func TestSettleCompileNode_Failed_EmitsStatusFailed(t *testing.T) {
	dep := compileDep(t, "rel", "compile.svc", "failed")
	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.SettleCompileNodeTerminal(
		context.Background(), depRepo, outboxRepo, aggRepo,
		"rel", "compile.svc", "failed", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last)
	assert.Equal(t, streams.CompileCompletedV1, outboxRepo.last.StreamName)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"failed"`)
}

// TestCompileAggregate_UsesDistinctNamespace asserts the compile dedup namespace
// is distinct from both the validation and seed-build namespaces so the three
// legs of one release derive independent outbox aggregate_ids.
func TestCompileAggregate_UsesDistinctNamespace(t *testing.T) {
	assert.NotEqual(t, validation.DedupNamespace, validation.CompileDedupNamespace,
		"compile namespace must differ from validation namespace")
	assert.NotEqual(t, validation.SeedBuildDedupNamespace, validation.CompileDedupNamespace,
		"compile namespace must differ from seed-build namespace")
}

// TestCompileAggregate_ScopedToMode asserts the compile aggregate counts and
// lists ONLY ModeCompile rows: a co-existing ModeValidation / ModeSeedBuild row
// for the same release must not affect the compile aggregate, and the compile
// row must not leak into the other legs.
func TestCompileAggregate_ScopedToMode(t *testing.T) {
	compileOk := compileDep(t, "rel", "compile.svc", "ok")
	valRow := validationDepForMode(t, "rel", "node.x", "failed")
	seedRow := seedBuildDep(t, "rel", "seed.a", "failed")

	repo := &modeScopedDepRepo{
		byMode: map[model.Mode][]*model.Deployment{
			model.ModeCompile:    {compileOk},
			model.ModeValidation: {valRow},
			model.ModeSeedBuild:  {seedRow},
		},
	}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	// Compile leg: only the compile row counts -> status ok despite the failed
	// validation and seed-build rows for the same release.
	err := validation.SettleCompileNodeTerminal(
		context.Background(), repo, outboxRepo, aggRepo,
		"rel", "compile.svc", "ok", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last)
	assert.Equal(t, streams.CompileCompletedV1, outboxRepo.last.StreamName)
	assert.Contains(t, string(outboxRepo.last.Payload), `"status":"ok"`,
		"compile aggregate must ignore the failed ModeValidation/ModeSeedBuild rows")
	assert.NotContains(t, string(outboxRepo.last.Payload), "node.x",
		"validation node must not leak into compile per_node")
	assert.NotContains(t, string(outboxRepo.last.Payload), "seed.a",
		"seed node must not leak into compile per_node")

	// Validation leg: only the validation row counts -> the failed row makes the
	// validation aggregate failed, unaffected by the ok compile row.
	outbox2 := &captureOutbox{}
	err = validation.EmitValidationAggregateIfComplete(
		context.Background(), repo, outbox2, &fakeAggRepo{won: true},
		validation.DedupNamespace, "rel", time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outbox2.last)
	assert.Equal(t, streams.ValidationCompletedV1, outbox2.last.StreamName)
	assert.NotContains(t, string(outbox2.last.Payload), "compile.svc",
		"compile node must not leak into validation per_node_results")
}
