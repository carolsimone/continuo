package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// checkFinishedPayload is the wire shape of release_check.finished:v1: enough
// for a consumer to reclaim what the check left behind and to tell what ended.
// A fix's release check names the try it proves by round and try.
// Every field is always present — a candidate carries an empty
// verifies_release_id and a zero round and try rather than
// omitting the keys, so a consumer can decode the same shape regardless of
// which kind of check ended.
type checkFinishedPayload struct {
	CheckID           string    `json:"check_id"`
	CheckKind         string    `json:"check_kind"`
	Outcome           string    `json:"outcome"`
	Project           string    `json:"project"`
	CandidateSchema   string    `json:"candidate_schema"`
	VerifiesReleaseID string    `json:"verifies_release_id"`
	Round             int       `json:"round"`
	Try               int       `json:"try"`
	FinishedAt        time.Time `json:"finished_at"`
}

// enqueueCheckFinished writes the release_check.finished:v1 outbox row for a
// check that has just reached a terminal status. Every terminal path calls it
// after the transition and before Commit, in the same transaction as the
// status write, so the announcement and the row cannot disagree. The
// candidate schema is always named: execution-controller drops it whatever
// the outcome, and a drop of a schema that no longer exists is a no-op.
func enqueueCheckFinished(ctx context.Context, u uow.UnitOfWork, r *releasecheck.Check, now time.Time) error {
	if !r.Status().IsTerminal() {
		return fmt.Errorf("check %s is not terminal (%s)", r.ID(), r.Status())
	}
	round := 0
	if r.Kind() == releasecheck.KindFix {
		round = r.Round()
	}
	payload, err := json.Marshal(checkFinishedPayload{
		CheckID:           r.ID(),
		CheckKind:         string(r.Kind()),
		Outcome:           string(r.Status()),
		Project:           r.ChangedProject(),
		CandidateSchema:   CandidateSchemaFor(r.ID()),
		VerifiesReleaseID: r.VerifiesReleaseID(),
		Round:             round,
		Try:               r.Try(),
		FinishedAt:        now.UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal check finished payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(r.ID()),
		EventType:     "release_check_finished",
		Payload:       payload,
		StreamName:    streams.ReleaseCheckFinishedV1,
		Status:        "pending",
	}); err != nil {
		return fmt.Errorf("outbox insert (check finished): %w", err)
	}
	return nil
}

// emitReleaseRejected writes the release.rejected:v1 outbox row for a
// candidate that has just been rejected, and records the payload on the
// candidate so a later round can replay it. A fix's
// failure is not a release rejection — nothing downstream of a release
// listens for it — so for a fix's release check this writes nothing; its failure
// travels on release_check.finished:v1 alone.
func emitReleaseRejected(ctx context.Context, u uow.UnitOfWork, r *releasecheck.Check, payload []byte) error {
	if r.Kind() != releasecheck.KindCandidate {
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
	}); err != nil {
		return fmt.Errorf("outbox insert (release rejected): %w", err)
	}
	return nil
}

// recordTerminalTelemetry calls Telemetry for a terminal decision: a release
// rejection or promotion for a candidate, a fix outcome for a fix's
// release check. Called after Commit.
func recordTerminalTelemetry(ctx context.Context, d *Deps, r *releasecheck.Check, nodeCount int) {
	switch {
	case r.Kind() == releasecheck.KindFix:
		d.Telemetry.FixFinished(ctx, r.ID(), string(r.Status()), nodeCount)
	case r.Status() == releasecheck.StatusPromoted:
		d.Telemetry.ReleasePromoted(ctx, r.ID(), nodeCount)
	case r.Status() == releasecheck.StatusRejected:
		d.Telemetry.ReleaseRejected(ctx, r.ID(), r.FailReason(), r.FailingNodes())
	}
}
