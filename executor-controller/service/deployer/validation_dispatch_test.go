package deployer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fakes -----------------------------------------------------------------

type fakeValidationDeployer struct {
	deployErr       error
	deployCalls     int
	validationCalls int
}

func (f *fakeValidationDeployer) Deploy(context.Context, deploy.JobSpec) error {
	f.deployCalls++
	return f.deployErr
}
func (f *fakeValidationDeployer) DeployValidation(context.Context, deploy.ValidationJobSpec) error {
	f.validationCalls++
	return f.deployErr
}
func (f *fakeValidationDeployer) CountActive(context.Context) (int, error) { return 0, nil }

// fakeDeploymentRepo is an in-memory DeploymentRepository sufficient for the
// validation dispatch/aggregate unit tests.
type fakeDeploymentRepo struct {
	saved      []*model.Deployment
	saveErr    error
	pending    int
	pendingErr error
	results    []*model.Deployment
	resultsErr error
}

func (r *fakeDeploymentRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *fakeDeploymentRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *fakeDeploymentRepo) Save(_ context.Context, d *model.Deployment) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saved = append(r.saved, d)
	return nil
}
func (r *fakeDeploymentRepo) GetByReleaseNode(context.Context, string, string) (*model.Deployment, error) {
	return nil, nil
}
func (r *fakeDeploymentRepo) PendingValidationCount(context.Context, string) (int, error) {
	return r.pending, r.pendingErr
}
func (r *fakeDeploymentRepo) ListValidationResults(context.Context, string) ([]*model.Deployment, error) {
	return r.results, r.resultsErr
}

var _ repository.DeploymentRepository = (*fakeDeploymentRepo)(nil)

type fakeAggRepo struct {
	won        bool
	claimErr   error
	claimCalls int
}

func (r *fakeAggRepo) ClaimEmission(context.Context, string, time.Time) (bool, error) {
	r.claimCalls++
	return r.won, r.claimErr
}

var _ repository.ValidationAggregateRepository = (*fakeAggRepo)(nil)

type fakeOutboxRepo struct {
	created   []*outbox.Entry
	createErr error
}

func (r *fakeOutboxRepo) Create(_ context.Context, e *outbox.Entry) error {
	if r.createErr != nil {
		return r.createErr
	}
	r.created = append(r.created, e)
	return nil
}
func (r *fakeOutboxRepo) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (r *fakeOutboxRepo) MarkProcessed(context.Context, uuid.UUID) error      { return nil }
func (r *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string) error { return nil }
func (r *fakeOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error     { return nil }

var _ outbox.Repository = (*fakeOutboxRepo)(nil)

func silentDispatcher(dep deploy.Deployer) *Dispatcher {
	return &Dispatcher{
		deployer: dep,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		backoff:  model.BackoffPolicy{Base: time.Second, Cap: time.Minute},
		now:      time.Now,
	}
}

func deployableValidation() command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID: "rel_1", NodeID: "node_1", ServiceName: "dbt",
		SchemaName: "public", TableName: "orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", JobName: "validate-public-orders",
		CandidateSchema: "_cand_rel_1", DeferStateURI: "s3://prev/",
	}
}

// --- Task 13: dispatchValidation -------------------------------------------

func TestDispatcher_DispatchOne_ValidationMode_CallsDeployValidation(t *testing.T) {
	fk := &fakeValidationDeployer{}
	d := silentDispatcher(fk)
	repo := &fakeDeploymentRepo{}
	dep := model.NewValidationDeployment(deployableValidation(), nil, time.Now())

	require.NoError(t, d.dispatchOne(context.Background(), repo, &fakeOutboxRepo{}, &fakeAggRepo{}, dep))

	assert.Equal(t, 1, fk.validationCalls, "DeployValidation invoked once")
	assert.Equal(t, 0, fk.deployCalls, "production Deploy never invoked for a validation row")
}

func TestDispatcher_DispatchOne_ValidationMode_OnSuccess_WritesNodeDeployedTriggerOnly(t *testing.T) {
	fk := &fakeValidationDeployer{}
	d := silentDispatcher(fk)
	repo := &fakeDeploymentRepo{}
	outboxRepo := &fakeOutboxRepo{}
	vc := deployableValidation()
	dep := model.NewValidationDeployment(vc, nil, time.Now())

	require.NoError(t, d.dispatchOne(context.Background(), repo, outboxRepo, &fakeAggRepo{}, dep))

	require.Len(t, repo.saved, 1)
	assert.Equal(t, model.StatusDeployed, repo.saved[0].Status(), "success marks deployed")
	assert.Equal(t, "", repo.saved[0].Outcome(), "no terminal outcome yet — it arrives via validation.node.completed:v1")

	// Success writes EXACTLY ONE row: the node.deployed:v1 check trigger so
	// k8s-controller status-checks the validation Job. No task_status_updated /
	// RUNNING announcement (that is production-only).
	require.Len(t, outboxRepo.created, 1, "success writes exactly one outbox row")
	e := outboxRepo.created[0]
	assert.Equal(t, "node_deployed", e.EventType)
	assert.Equal(t, streams.NodeDeployedV1, e.StreamName)
	assert.Equal(t, "task", e.AggregateType)

	// The trigger carries the validation job_name and the deterministic synthetic
	// task/schedule UUIDs derived from (release_id, node_id).
	wantTaskID, wantScheduleID := model.ValidationSyntheticIDs(vc.ReleaseID, vc.NodeID)
	assert.Equal(t, wantTaskID, e.AggregateID, "outbox aggregate id is the synthetic task id")
	var payload struct {
		TaskID     string `json:"task_id"`
		ScheduleID string `json:"schedule_id"`
		JobName    string `json:"job_name"`
		NodeType   string `json:"node_type"`
		ImageTag   string `json:"image_tag"`
	}
	require.NoError(t, json.Unmarshal(e.Payload, &payload))
	assert.Equal(t, wantTaskID.String(), payload.TaskID)
	assert.Equal(t, wantScheduleID.String(), payload.ScheduleID)
	assert.Equal(t, vc.JobName, payload.JobName)
	assert.Equal(t, vc.NodeType, payload.NodeType)
	assert.Equal(t, vc.ImageTag, payload.ImageTag)
}

func TestDispatcher_DispatchOne_ValidationMode_OnPermanentFailure_RecordsOutcomeFailedAndAggregates(t *testing.T) {
	fk := &fakeValidationDeployer{deployErr: errors.Join(errors.New("bad image"), pkgevents.ErrPermanent)}
	d := silentDispatcher(fk)
	failed := model.NewValidationDeployment(deployableValidation(), nil, time.Now())
	require.NoError(t, failed.FailValidation("bad image", time.Now())) // the row as ListValidationResults would return it
	repo := &fakeDeploymentRepo{pending: 0, results: []*model.Deployment{failed}}
	outboxRepo := &fakeOutboxRepo{}
	agg := &fakeAggRepo{won: true}
	dep := model.NewValidationDeployment(deployableValidation(), nil, time.Now())

	require.NoError(t, d.dispatchOne(context.Background(), repo, outboxRepo, agg, dep))

	require.Len(t, repo.saved, 1)
	assert.Equal(t, model.StatusFailed, repo.saved[0].Status(), "permanent deploy failure is terminal")
	assert.Equal(t, "failed", repo.saved[0].Outcome(), "terminal outcome recorded as failed")
	assert.Equal(t, 1, agg.claimCalls, "aggregate emit attempted once last node is terminal")
	require.Len(t, outboxRepo.created, 1, "aggregate validation.completed:v1 row written")
	assert.Equal(t, streams.ValidationCompletedV1, outboxRepo.created[0].StreamName)
}

// --- Task 14: maybeEmitValidationAggregate ---------------------------------

func recordedResult(t *testing.T, nodeID, outcome, logURI string) *model.Deployment {
	t.Helper()
	cmd := deployableValidation()
	cmd.NodeID = nodeID
	now := time.Now()
	d := model.NewValidationDeployment(cmd, nil, now)
	require.NoError(t, d.MarkDeployed(now))
	require.NoError(t, d.RecordOutcome(outcome, logURI, now))
	return d
}

func TestMaybeEmit_NoOpWhenPendingRemain(t *testing.T) {
	d := silentDispatcher(&fakeValidationDeployer{})
	repo := &fakeDeploymentRepo{pending: 2}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}

	require.NoError(t, d.maybeEmitValidationAggregate(context.Background(), repo, outboxRepo, agg, "rel_1", time.Now()))

	assert.Equal(t, 0, agg.claimCalls, "no claim while nodes remain pending")
	assert.Empty(t, outboxRepo.created, "no emission while pending")
}

func TestMaybeEmit_EmitsAggregateOk_WhenAllOutcomesOk(t *testing.T) {
	d := silentDispatcher(&fakeValidationDeployer{})
	repo := &fakeDeploymentRepo{
		pending: 0,
		results: []*model.Deployment{
			recordedResult(t, "node_a", "ok", "s3://logs/a"),
			recordedResult(t, "node_b", "ok", ""),
		},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}

	require.NoError(t, d.maybeEmitValidationAggregate(context.Background(), repo, outboxRepo, agg, "rel_1", time.Now()))

	require.Len(t, outboxRepo.created, 1)
	e := outboxRepo.created[0]
	assert.Equal(t, "release", e.AggregateType)
	assert.Equal(t, "validation_completed", e.EventType)
	assert.Equal(t, streams.ValidationCompletedV1, e.StreamName)

	var payload struct {
		ReleaseID       string `json:"release_id"`
		AggregateStatus string `json:"aggregate_status"`
		PerNodeResults  []struct {
			NodeID    string `json:"node_id"`
			Status    string `json:"status"`
			DBTLogURI string `json:"dbt_log_uri"`
		} `json:"per_node_results"`
	}
	require.NoError(t, json.Unmarshal(e.Payload, &payload))
	assert.Equal(t, "rel_1", payload.ReleaseID)
	assert.Equal(t, "ok", payload.AggregateStatus)
	require.Len(t, payload.PerNodeResults, 2)
	assert.Equal(t, "s3://logs/a", payload.PerNodeResults[0].DBTLogURI)
	assert.Equal(t, "", payload.PerNodeResults[1].DBTLogURI, "empty log uri omitted -> decodes to zero value")

	// omitempty: the node with no log produces no dbt_log_uri key at all.
	var raw struct {
		PerNodeResults []map[string]any `json:"per_node_results"`
	}
	require.NoError(t, json.Unmarshal(e.Payload, &raw))
	_, present := raw.PerNodeResults[1]["dbt_log_uri"]
	assert.False(t, present, "absent log uri key omitted")
}

func TestMaybeEmit_EmitsAggregateFailed_WhenAnyOutcomeFailed(t *testing.T) {
	d := silentDispatcher(&fakeValidationDeployer{})
	repo := &fakeDeploymentRepo{
		pending: 0,
		results: []*model.Deployment{
			recordedResult(t, "node_a", "ok", "s3://logs/a"),
			recordedResult(t, "node_b", "failed", "s3://logs/b"),
		},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}

	require.NoError(t, d.maybeEmitValidationAggregate(context.Background(), repo, outboxRepo, agg, "rel_1", time.Now()))

	require.Len(t, outboxRepo.created, 1)
	var payload struct {
		AggregateStatus string `json:"aggregate_status"`
	}
	require.NoError(t, json.Unmarshal(outboxRepo.created[0].Payload, &payload))
	assert.Equal(t, "failed", payload.AggregateStatus, "any failed node => aggregate failed")
}

func TestMaybeEmit_SentinelClaimReturnsFalse_NoDuplicateOutboxRow(t *testing.T) {
	d := silentDispatcher(&fakeValidationDeployer{})
	repo := &fakeDeploymentRepo{pending: 0, results: []*model.Deployment{recordedResult(t, "node_a", "ok", "")}}
	agg := &fakeAggRepo{won: false} // another caller already emitted
	outboxRepo := &fakeOutboxRepo{}

	require.NoError(t, d.maybeEmitValidationAggregate(context.Background(), repo, outboxRepo, agg, "rel_1", time.Now()))

	assert.Equal(t, 1, agg.claimCalls, "claim attempted")
	assert.Empty(t, outboxRepo.created, "claim lost => no duplicate outbox row")
}
