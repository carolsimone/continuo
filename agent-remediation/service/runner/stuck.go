package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// stuckEnder ends a remediation that can go no further. The runner uses it
// before every step it starts, the worker on the remediation of a drive that
// failed, and the sweeper on the remediations nobody drives. Whoever ends a
// remediation counts it as a system failure on metrics, once its transaction
// committed.
type stuckEnder struct {
	newUoW  func() uow.UnitOfWork
	clock   ports.Clock
	logger  *slog.Logger
	metrics ports.RemediationMetrics
}

// endIfStuck, in one transaction after holding the lease as owner, reads the
// remediation and, when it is running and stuck, records it as a system
// failure: the remediation moves to system_failure with the reason, its
// current try is left in the status it is in, for a redrive of the trigger to
// resume, the fix row of that try is failed with the reason and how to
// resume it, and the heal trigger the remediation answers to is dead-lettered
// on consumer.dead_letter:v1 (deadLetterEntry). Nothing claims, polls or
// sweeps a remediation that is not running, so the try waits for the redrive.
// ended reports whether it ended the remediation.
func (e stuckEnder) endIfStuck(ctx context.Context, id uuid.UUID, owner string) (ended bool, err error) {
	u := e.newUoW()
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	now := e.clock.Now()
	repo := u.RemediationRepo()
	if err := repo.Hold(ctx, id, owner, now, leaseDuration); err != nil {
		return false, err
	}
	snap, err := repo.Load(ctx, id)
	if err != nil {
		return false, fmt.Errorf("load: %w", err)
	}
	rem := snap.Remediation
	if rem.Status != remediation.StatusRunning {
		return false, nil
	}
	why := whyStuck(snap, now)
	if why.reason == "" {
		return false, nil
	}
	if !rem.Status.CanMoveTo(remediation.StatusSystemFailure) {
		return false, fmt.Errorf("remediation cannot move from %s to %s", rem.Status, remediation.StatusSystemFailure)
	}
	rem.Status = remediation.StatusSystemFailure
	rem.Failure = why.reason
	rem.UpdatedAt = now
	if err := repo.SaveRemediation(ctx, rem); err != nil {
		return false, fmt.Errorf("save remediation: %w", err)
	}
	if err := failFixRow(ctx, u.FixRepo(), snap, deadLetteredReason(why.reason, rem.ID)); err != nil {
		return false, err
	}
	entry, err := deadLetterEntry(snap, why, now)
	if err != nil {
		return false, err
	}
	if err := u.OutboxRepo().Create(ctx, entry); err != nil {
		return false, fmt.Errorf("dead-letter the trigger: %w", err)
	}
	if err := u.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	e.metrics.SystemFailure(rem.Flow, string(why.kind))
	e.logger.Error("remediation ended as a system failure; its trigger is dead-lettered, and a redrive resumes it",
		"remediation", id, "tenant", rem.TenantID, "release", rem.ReleaseID, "round", rem.Round,
		"trigger_message_id", rem.TriggerMessageID, "reason", why.reason)
	return true, nil
}

// endIfStuck applies the stuck rules to run, which the runner read holding the
// lease, before it starts a step on the remediation or moves its try on. When
// one holds at the runner's clock, the remediation is ended through the path
// the worker and the sweeper use: one transaction that holds the lease, reads
// the remediation again and, when it is still stuck, records the system
// failure, fails the try's row and dead-letters the trigger. ended reports
// whether it was ended. A lost lease is repository.ErrLeaseLost, with nothing
// written.
func (r *Runner) endIfStuck(ctx context.Context, run tryRun) (ended bool, err error) {
	if whyStuck(run.snap, r.clock.Now()).reason == "" {
		return false, nil
	}
	return stuckEnder{newUoW: r.d.NewUoW, clock: r.clock, logger: r.logger, metrics: r.metrics}.endIfStuck(ctx, run.snap.Remediation.ID, r.d.Owner)
}

// deadLetteredReason is what the fix row of a remediation that ended as a
// system failure is failed with: why it ended, and how to resume it.
func deadLetteredReason(reason string, id uuid.UUID) string {
	return fmt.Sprintf("%s; dead-lettered as remediation %s, redrive it once the cause is fixed", reason, id)
}

// deadLetterEntry is the outbox row that dead-letters the heal trigger the
// remediation answers to: the message the runner's consumer read from
// heal.triggered:v1 in its group, named by its entry id and key, with
// its stream fields and its payload, so a redrive republishes the trigger as
// it was; why the remediation stopped, with the remediation, its try and the
// step that stopped it; and how many times that failed. The row is stored
// under the remediation's tenant, with an id derived from the remediation and
// the message, so each message the remediation answers to is dead-lettered
// once. The outbox publisher renders it as the consumer.dead_letter:v1 entry.
func deadLetterEntry(snap remediation.Snapshot, why stuck, now time.Time) (*outbox.Entry, error) {
	rem := snap.Remediation
	fields := maps.Clone(rem.TriggerFields)
	if fields == nil {
		fields = map[string]string{}
	}
	fields["payload"] = string(rem.TriggerPayload)
	messageID, key, _ := strings.Cut(rem.TriggerMessageID, "@")
	body, err := json.Marshal(events.DeadLetteredMessage{
		TenantID:      rem.TenantID,
		OccurredAt:    now,
		Stream:        streams.HealTriggeredV1,
		Group:         streams.AgentRemediationHealTriggered,
		MessageID:     messageID,
		Key:           key,
		Fields:        fields,
		FailureKind:   pkg_model.DeadLetterKindTransientExhausted,
		Error:         deadLetterError(snap, why),
		DeliveryCount: deliveryCount(rem, why),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal the dead letter: %w", err)
	}
	stored, err := outbox.WrapTenant(rem.TenantID, body)
	if err != nil {
		return nil, fmt.Errorf("store the dead letter under its tenant: %w", err)
	}
	return &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("dead_lettered|"+rem.ID.String()+"|"+rem.TriggerMessageID)),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(rem.ReleaseID),
		EventType:     event.DeadLetteredEventType,
		Payload:       stored,
		StreamName:    streams.ConsumerDeadLetterV1,
		Status:        "pending",
	}, nil
}

// deadLetterError is the dead letter's error: the remediation, its current
// try and the step that stopped it, when they are known, then why it stopped.
func deadLetterError(snap remediation.Snapshot, why stuck) string {
	var b strings.Builder
	fmt.Fprintf(&b, "remediation %s", snap.Remediation.ID)
	if snap.Try != nil {
		fmt.Fprintf(&b, ", try %d", snap.Try.Try)
	}
	if why.step != "" {
		fmt.Fprintf(&b, ", step %q", why.step)
	}
	b.WriteString(": ")
	b.WriteString(why.reason)
	return b.String()
}

// deliveryCount is the dead letter's delivery count: the failures of what
// stopped the remediation, else the failures its own row counts, else one.
func deliveryCount(rem remediation.Remediation, why stuck) int64 {
	switch {
	case why.failures > 0:
		return int64(why.failures)
	case rem.Failures > 0:
		return int64(rem.Failures)
	}
	return 1
}

// failFixRow fails the fix row of the current try with
// reason, cleaned (fix.CleanedReason), by what the row is: a try still generating is failed, and a
// try whose release checks are still being judged is failed as a release
// check that did not pass, with the checks it recorded. A try with no row of its own leaves
// the release's in-flight row failed, if it has one. Any other row already
// says how its try ended.
func failFixRow(ctx context.Context, repo repository.FixRepository, snap remediation.Snapshot, reason string) error {
	// The reason holds the last error of a step, which can echo what a source or a
	// service printed, and the row shows it.
	reason = fix.CleanedReason(reason)
	releaseID := snap.Remediation.ReleaseID
	if snap.Try != nil {
		views, err := repo.List(ctx, repository.FixFilter{ReleaseID: releaseID})
		if err != nil {
			return fmt.Errorf("list the fixes of release %s: %w", releaseID, err)
		}
		for _, v := range views {
			if v.RemediationRound != snap.Remediation.Round || v.Try != snap.Try.Try {
				continue
			}
			switch v.Status {
			case fix.StatusVerifying:
				if _, err := repo.MarkVerifyFailed(ctx, v.ID, reason, v.ReleaseChecks); err != nil {
					return fmt.Errorf("fail the verifying fix row: %w", err)
				}
				return nil
			case fix.StatusGenerating:
				if _, err := repo.FailGenerating(ctx, releaseID, reason); err != nil {
					return fmt.Errorf("fail the in-flight fix row: %w", err)
				}
				return nil
			}
			return nil
		}
	}
	if _, err := repo.FailGenerating(ctx, releaseID, reason); err != nil {
		return fmt.Errorf("fail the in-flight fix row: %w", err)
	}
	return nil
}

// stuckKind is the kind of failure that stopped a remediation, which the
// system failure metric counts it under.
type stuckKind string

const (
	// stuckStepFailures: a step of the current try failed too often.
	stuckStepFailures stuckKind = "step_failures"
	// stuckRemediationFailures: the remediation failed too often in a row
	// before a step could run.
	stuckRemediationFailures stuckKind = "remediation_failures"
	// stuckDeadline: a drafting or checking try ran past its deadline.
	stuckDeadline stuckKind = "deadline"
)

// stuck is why a remediation can make no more progress. reason says it, and
// is empty when the remediation can go on; kind is the kind of failure it is;
// step names the step of the current try that failed too often, when that is
// what stopped it; failures is how many times what stopped it failed, zero for
// a try past its deadline.
type stuck struct {
	reason   string
	kind     stuckKind
	step     string
	failures int
}

// whyStuck says why the remediation can make no more progress: a step of its
// current try that has not finished and failed at least stepFailureLimit
// times, a remediation that failed that many times in a row before a step
// could run (it could not be loaded, or its next try could not open), or a
// drafting or checking try past its deadline. Its reason is empty when the
// remediation can go on.
func whyStuck(snap remediation.Snapshot, now time.Time) stuck {
	for _, st := range snap.Steps {
		if st.Status == remediation.StepDone || st.Failures < stepFailureLimit {
			continue
		}
		reason := fmt.Sprintf("step %q failed %d times", st.Step, st.Failures)
		if st.LastError != "" {
			reason += ", the last time with: " + st.LastError
		}
		return stuck{reason: reason, kind: stuckStepFailures, step: st.Step, failures: st.Failures}
	}
	if rem := snap.Remediation; rem.Failures >= stepFailureLimit {
		reason := fmt.Sprintf("the remediation failed %d times in a row before a step could run", rem.Failures)
		if rem.LastError != "" {
			reason += ", the last time with: " + rem.LastError
		}
		return stuck{reason: reason, kind: stuckRemediationFailures, failures: rem.Failures}
	}
	t := snap.Try
	if t == nil || !t.DeadlineAt.Before(now) {
		return stuck{}
	}
	switch t.Status {
	case remediation.TryDrafting, remediation.TryChecking:
		return stuck{
			reason: fmt.Sprintf("try %d was still %s at its deadline, %s", t.Try, t.Status, t.DeadlineAt.UTC().Format(time.RFC3339)),
			kind:   stuckDeadline,
		}
	}
	return stuck{}
}
