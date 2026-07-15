package validation_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/validation"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// --- fakes -----------------------------------------------------------------

// fakeDepRepo is an in-memory DeploymentRepository for aggregate unit tests.
type fakeDepRepo struct {
	pending int
	results []*model.Deployment
}

func (r *fakeDepRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *fakeDepRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *fakeDepRepo) Save(context.Context, *model.Deployment) error { return nil }
func (r *fakeDepRepo) GetByReleaseNode(context.Context, string, string, model.Mode) (*model.Deployment, error) {
	return nil, nil
}
func (r *fakeDepRepo) PendingValidationCount(_ context.Context, _ string, _ model.Mode) (int, error) {
	return r.pending, nil
}
func (r *fakeDepRepo) ListValidationResults(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	return r.results, nil
}
func (r *fakeDepRepo) ListValidationByRelease(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}

var _ repository.DeploymentRepository = (*fakeDepRepo)(nil)

// fakeAggRepo is an in-memory ValidationAggregateRepository for aggregate unit tests.
type fakeAggRepo struct {
	won bool
}

func (r *fakeAggRepo) LockRelease(context.Context, string, model.Mode) error { return nil }
func (r *fakeAggRepo) ClaimEmission(context.Context, string, model.Mode, time.Time) (bool, error) {
	return r.won, nil
}

var _ repository.ValidationAggregateRepository = (*fakeAggRepo)(nil)

// captureOutbox captures the last outbox entry created, and the full ordered
// sequence of entries so a test can assert on every row a settle produced.
type captureOutbox struct {
	last *outbox.Entry
	all  []*outbox.Entry
}

func (c *captureOutbox) Create(_ context.Context, e *outbox.Entry) error {
	c.last = e
	c.all = append(c.all, e)
	return nil
}
func (c *captureOutbox) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (c *captureOutbox) MarkProcessed(context.Context, uuid.UUID) error        { return nil }
func (c *captureOutbox) MarkProcessedBatch(context.Context, []uuid.UUID) error { return nil }
func (c *captureOutbox) MarkFailed(context.Context, uuid.UUID, string) error   { return nil }
func (c *captureOutbox) IncrementRetry(context.Context, uuid.UUID) error       { return nil }

var _ outbox.Repository = (*captureOutbox)(nil)

// --- tests -----------------------------------------------------------------

func TestEmitValidationAggregate_IncludesCandidateSchema(t *testing.T) {
	// Build a validation Deployment carrying CandidateSchema via command.ValidationDeployTask.
	cmd := command.ValidationDeployTask{
		ReleaseID:       "rel",
		NodeID:          "n1",
		CandidateSchema: "_candidate_rel",
		JobName:         "j",
		NodeType:        "dbt-model",
		ImageTag:        "t",
	}
	dep := model.NewValidationDeployment(cmd, nil, time.Now(), false)
	// Drive it to a terminal ok outcome so ListValidationResults would include it
	// and Outcome() == "ok".
	require.NoError(t, dep.MarkDeployed(time.Now()))
	require.NoError(t, dep.RecordOutcome("ok", "", "", time.Now()))

	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.EmitValidationAggregateIfComplete(
		context.Background(),
		depRepo,
		outboxRepo,
		aggRepo,
		validation.DedupNamespace,
		"rel",
		time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last, "expected an outbox entry to be created")
	require.Contains(t, string(outboxRepo.last.Payload), `"candidate_schema":"_candidate_rel"`)
}

// TestEmitValidationAggregate_CarriesDecisionOnly asserts the terminal
// validation.completed:v1 payload carries only the decision (aggregate_status)
// and candidate_schema — never per-node content. Per-node results (including
// dbt_log_uri / run_results_uri) reach release-controller through the separate
// validation.node.result:v1 projection stream, so re-carrying them here would be
// redundant.
func TestEmitValidationAggregate_CarriesDecisionOnly(t *testing.T) {
	cmd := command.ValidationDeployTask{
		ReleaseID:       "rel",
		NodeID:          "n1",
		CandidateSchema: "_candidate_rel",
		JobName:         "j",
		NodeType:        "dbt-model",
		ImageTag:        "t",
	}
	dep := model.NewValidationDeployment(cmd, nil, time.Now(), false)
	require.NoError(t, dep.MarkDeployed(time.Now()))
	require.NoError(t, dep.RecordOutcome("failed", "s3://logs/n1", "run-results/n1.json", time.Now()))

	depRepo := &fakeDepRepo{pending: 0, results: []*model.Deployment{dep}}
	outboxRepo := &captureOutbox{}
	aggRepo := &fakeAggRepo{won: true}

	err := validation.EmitValidationAggregateIfComplete(
		context.Background(),
		depRepo,
		outboxRepo,
		aggRepo,
		validation.DedupNamespace,
		"rel",
		time.Now(),
	)
	require.NoError(t, err)
	require.NotNil(t, outboxRepo.last, "expected an outbox entry to be created")
	payload := string(outboxRepo.last.Payload)
	require.Contains(t, payload, `"aggregate_status":"failed"`, "terminal event carries the decision")
	require.Contains(t, payload, `"candidate_schema":"_candidate_rel"`)
	require.NotContains(t, payload, "per_node_results", "terminal event must not re-carry per-node content")
	require.NotContains(t, payload, "run_results_uri", "per-node URIs travel on the projection stream, not the terminal event")
}
