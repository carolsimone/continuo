package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// runFinishedPayload is the wire shape of pipeline.run.finished:v1: enough
// for a consumer to reclaim what the run left behind and to tell what ended.
// Every field is always present — a candidate carries an empty
// verifies_release_id and a zero attempt rather than omitting the keys, so a
// consumer can decode the same shape regardless of which kind of run ended.
type runFinishedPayload struct {
	RunID             string    `json:"run_id"`
	RunKind           string    `json:"run_kind"`
	Outcome           string    `json:"outcome"`
	Service           string    `json:"service"`
	CandidateSchema   string    `json:"candidate_schema"`
	VerifiesReleaseID string    `json:"verifies_release_id"`
	Attempt           int       `json:"attempt"`
	FinishedAt        time.Time `json:"finished_at"`
}

// enqueueRunFinished writes the pipeline.run.finished:v1 outbox row for a
// run that has just reached a terminal status. Every terminal path calls it
// after the transition and before Commit, in the same transaction as the
// status write, so the announcement and the row cannot disagree. The
// candidate schema is always named: executor-controller drops it whatever
// the outcome, and a drop of a schema that no longer exists is a no-op.
func enqueueRunFinished(ctx context.Context, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	if !r.Status().IsTerminal() {
		return fmt.Errorf("run %s is not terminal (%s)", r.ID(), r.Status())
	}
	payload, err := json.Marshal(runFinishedPayload{
		RunID:             r.ID(),
		RunKind:           string(r.Kind()),
		Outcome:           string(r.Status()),
		Service:           r.ChangedService(),
		CandidateSchema:   CandidateSchemaFor(r.ID()),
		VerifiesReleaseID: r.VerifiesReleaseID(),
		Attempt:           r.Attempt(),
		FinishedAt:        now.UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal run finished payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(r.ID()),
		EventType:     "pipeline_run_finished",
		Payload:       payload,
		StreamName:    streams.PipelineRunFinishedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert (run finished): %w", err)
	}
	return nil
}

// emitReleaseRejected writes the release.rejected:v1 outbox row for a
// candidate that has just been rejected, and records the payload on the
// candidate so a later remediation round can replay it. A verification's
// failure is not a release rejection — nothing downstream of a release
// listens for it — so for a verification this writes nothing; its failure
// travels on pipeline.run.finished:v1 alone.
func emitReleaseRejected(ctx context.Context, u uow.UnitOfWork, r *pipeline.Run, payload []byte, now time.Time) error {
	if r.Kind() != pipeline.KindCandidate {
		return nil
	}
	r.SetRejectionPayload(payload)
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(r.ID()),
		EventType:     "release_rejected",
		Payload:       payload,
		StreamName:    streams.ReleaseRejectedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert (release rejected): %w", err)
	}
	return nil
}

// recordTerminalTelemetry emits the span a terminal decision deserves: a
// release rejection or promotion for a candidate, a verification outcome for
// a verification. Called after Commit.
func recordTerminalTelemetry(ctx context.Context, d *Deps, r *pipeline.Run, nodeCount int) {
	switch {
	case r.Kind() == pipeline.KindVerification:
		d.Telemetry.VerificationFinished(ctx, r.ID(), string(r.Status()), nodeCount)
	case r.Status() == pipeline.StatusPromoted:
		d.Telemetry.ReleasePromoted(ctx, r.ID(), nodeCount)
	case r.Status() == pipeline.StatusRejected:
		d.Telemetry.ReleaseRejected(ctx, r.ID(), r.FailReason(), r.FailingNodes())
	}
}
