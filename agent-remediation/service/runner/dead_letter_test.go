package runner_test

import (
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// triggerKey is the Redis key the runner's consumer reads heal triggers from,
// which the dedup id of every message it reads ends with.
var triggerKey = pkgredis.Key(streams.HealTriggeredV1)

// deadLetters is every outbox entry on consumer.dead_letter:v1, in the order
// it was written.
func (h *harness) deadLetters() []outbox.Entry {
	var out []outbox.Entry
	for _, e := range h.store.OutboxEntries() {
		if e.StreamName == streams.ConsumerDeadLetterV1 {
			out = append(out, e)
		}
	}
	return out
}

// deadLetterOf reads a consumer.dead_letter:v1 entry as the outbox publisher
// renders it: the message stored under its tenant, rendered as the
// dead letter's Redis fields with agent-remediation as producer, and decoded
// as dead-letter-controller decodes them.
func deadLetterOf(t *testing.T, e outbox.Entry) (events.Envelope, events.ConsumerDeadLetter) {
	t.Helper()
	tenantID, body, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	var m events.DeadLetteredMessage
	require.NoError(t, json.Unmarshal(body, &m))
	m.TenantID = tenantID
	m.Producer = "agent-remediation"
	values, err := events.ConsumerDeadLetterFields(m)
	require.NoError(t, err)
	fields, err := outbox.StringifyFields(values)
	require.NoError(t, err)
	env, dl, err := events.DecodeConsumerDeadLetter(fields)
	require.NoError(t, err)
	return env, dl
}

// redrive accepts the trigger of a dead letter as dead-letter-controller
// redrives it: the message's original fields, with a fresh outbox entry id,
// read back under messageID.
func (h *harness) redrive(e outbox.Entry, messageID string) {
	h.t.Helper()
	_, dl := deadLetterOf(h.t, e)
	fields := maps.Clone(dl.Fields)
	payload := []byte(fields["payload"])
	delete(fields, "payload")
	outboxID := uuid.New()
	fields["outbox_entry_id"] = outboxID.String()
	trig, err := serialization.DecodeTrigger(payload)
	require.NoError(h.t, err)
	trig.TenantID = fields["tenant_id"]
	require.NoError(h.t, h.runner.Accept(ctx, h.acceptedAs(trig, payload, messageID, &outboxID, fields)))
}

// acceptedAs is a heal trigger read from the stream with its payload, message
// id, outbox entry id and stream fields.
func (h *harness) acceptedAs(trig remediation.Trigger, payload []byte, messageID string, outboxID *uuid.UUID, fields map[string]string) runner.Accepted {
	a := h.accepted(trig, messageID)
	a.Payload = payload
	a.OutboxEntryID = outboxID
	a.Fields = fields
	return a
}

// failGateSubmits drives the remediation n times with the gate's submit
// failing each time, so the gate's row counts n failures.
func (h *harness) failGateSubmits(id uuid.UUID, n int) {
	h.t.Helper()
	for range n {
		h.checks.mu.Lock()
		h.checks.submitErr = errors.New("release-controller is down")
		h.checks.mu.Unlock()
		require.Error(h.t, h.drive(id))
	}
}

// sweptAsStuck opens the remediation's first try, records five failures of its
// dbt_log step and sweeps, so the sweeper ends it as a system failure with its
// try drafting.
func (h *harness) sweptAsStuck() uuid.UUID {
	h.t.Helper()
	id := h.drafting()
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)
	require.NoError(h.t, h.sweeper().Sweep(ctx))
	require.Equal(h.t, remediation.StatusSystemFailure, h.remediation(id).Status)
	return id
}

// The sweeper's system failure is a dead letter of the trigger the remediation
// was created from: dead-letter-controller decodes it, it names the stream,
// the group and the message, it carries the message's fields with its
// payload, so a redrive republishes the trigger as it was, and it says why.
func TestSweeper_ASystemFailureWritesOneDeadLetter(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	trig.TenantID = "acme"
	outboxID := uuid.New()
	fields := map[string]string{
		"event_id": "ev-1", "tenant_id": "acme", "producer": "remediation", "schema_version": "3",
		"occurred_at": "2026-11-13T08:59:00Z", "outbox_entry_id": outboxID.String(),
	}
	payload := payloadOf(t, trig)
	messageID := "1700000000000-0@" + triggerKey
	require.NoError(t, h.runner.Accept(ctx, h.acceptedAs(trig, payload, messageID, &outboxID, fields)))
	id := h.store.Remediations()[0].ID
	h.reclaim(id)
	require.NoError(t, h.runner.NextTry(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)
	h.clock.set(t0.Add(time.Minute))

	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	require.Equal(t, remediation.StatusSystemFailure, rem.Status)
	dls := h.deadLetters()
	require.Len(t, dls, 1)
	tenantID, _, err := outbox.UnwrapTenant(dls[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, "acme", tenantID, "the dead letter is stored under the remediation's tenant")

	env, dl := deadLetterOf(t, dls[0])
	assert.Equal(t, streams.HealTriggeredV1, dl.OriginalStream)
	assert.Equal(t, streams.AgentRemediationHealTriggered, dl.OriginalGroup)
	assert.Equal(t, "1700000000000-0", dl.OriginalMessageID)
	assert.Equal(t, messageID, dl.DedupMessageID(), "the dead letter names the message the remediation was created from")
	want := maps.Clone(fields)
	want["payload"] = string(payload)
	assert.Equal(t, want, dl.Fields, "the trigger's fields, its payload included")
	assert.Equal(t, model.DeadLetterKindTransientExhausted, dl.FailureKind)
	assert.Equal(t, int64(5), dl.DeliveryCount, "the failing step's failures")
	assert.Contains(t, dl.Error, "remediation "+id.String())
	assert.Contains(t, dl.Error, "try 1")
	assert.Contains(t, dl.Error, `step "dbt_log"`)
	assert.Contains(t, dl.Error, rem.Failure)
	assert.Equal(t, "acme", env.TenantID)
	assert.Equal(t, events.ConsumerDeadLetterEventID("acme", streams.AgentRemediationHealTriggered,
		streams.HealTriggeredV1, messageID), env.EventID)
	assert.True(t, t0.Add(time.Minute).Equal(env.OccurredAt), "the dead letter occurs when the remediation ends")

	assert.Contains(t, h.inFlightRow().Rationale, rem.Failure)
	assert.Contains(t, h.inFlightRow().Rationale, "dead-lettered as remediation "+id.String()+", redrive it once the cause is fixed",
		"the attempt row says how to resume")

	require.NoError(t, h.sweeper().Sweep(ctx))
	assert.Len(t, h.deadLetters(), 1, "a remediation that ended is not ended again")
}

// The drive's own check before a step ends a remediation through the same
// path: a try past its deadline, with no failure counted anywhere, is a dead
// letter delivered once.
func TestRunner_ASystemFailureTheDriveFindsWritesOneDeadLetter(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.clock.set(t0.Add(draftingTime + time.Second))

	require.NoError(t, h.drive(id))

	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	dls := h.deadLetters()
	require.Len(t, dls, 1)
	_, dl := deadLetterOf(t, dls[0])
	assert.Equal(t, int64(1), dl.DeliveryCount)
	assert.Contains(t, dl.Error, "try 1 was still drafting at its deadline")
	assert.Equal(t, "1-0", dl.OriginalMessageID)
}

// A redrive resumes the remediation where it stopped: the try it was on goes
// on from its first step with no done row, so no step that committed runs
// again and the model is not called again, and the remediation passes as if
// it had never stopped.
func TestRunner_RedriveResumesTheFailedRemediation(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.failGateSubmits(id, 5)
	require.NoError(t, h.drive(id), "the sixth drive ends the remediation")
	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	require.Equal(t, remediation.TryChecking, h.try(id, 1).Status, "the try is left where it stopped")
	require.Equal(t, proposal.StatusFailed, h.attempt(1).Proposal.Status)
	require.Equal(t, 1, h.llm.calls())
	dls := h.deadLetters()
	require.Len(t, dls, 1)

	h.clock.set(t0.Add(time.Hour))
	h.redrive(dls[0], "2-0")

	require.Len(t, h.store.Remediations(), 1, "a redrive adds no remediation")
	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusRunning, rem.Status)
	assert.Empty(t, rem.Failure)
	assert.Zero(t, rem.Failures)
	assert.Equal(t, "2-0", rem.TriggerMessageID, "the remediation answers to the redriven message")
	tr := h.try(id, 1)
	assert.Equal(t, remediation.TryChecking, tr.Status)
	assert.True(t, t0.Add(time.Hour+checkingTime).Equal(tr.DeadlineAt), "the try's deadline starts again: %s", tr.DeadlineAt)
	gate := h.flowRows(id, 1)["release_check"]
	require.Len(t, gate, 1)
	assert.Zero(t, gate[0].Failures, "the failed step's failures are reset")
	assert.Empty(t, gate[0].LastError)
	row := h.attempt(1)
	assert.Equal(t, proposal.StatusGenerating, row.Proposal.Status, "the attempt is in flight again")
	assert.Equal(t, proposal.StatusGenerating, row.Proposal.NodeOutcomes["svc-a"].Status)
	assert.Empty(t, row.Proposal.Rationale)
	msgs := h.store.ProcessedMessages()
	require.Len(t, msgs, 2)
	for _, m := range msgs {
		assert.Equal(t, messageprocessing.StateCompleted, m.State)
	}

	require.NoError(t, h.drive(id))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.checks.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Len(t, h.store.Tries(id), 1, "the remediation finished on the try it was on")
	assert.Equal(t, 1, h.llm.calls(), "the committed prompt step's model call is not repeated")
	prompts := h.stepRows(id, 1, 1)["dbt-compile"]
	require.Len(t, prompts, 1)
	assert.False(t, prompts[0].CacheHit, "the prompt step did not run again")
	assert.Len(t, h.deadLetters(), 1)
}

// A redrive of a message whose remediation still runs, or has finished, is a
// duplicate: the message is claimed and nothing else changes.
func TestRunner_ARedriveOfARunningRemediationIsADuplicate(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	require.NoError(t, h.runner.Accept(ctx, h.accepted(trig, "1-0")))
	before := h.store.Remediations()[0]
	outboxID := uuid.New()

	require.NoError(t, h.runner.Accept(ctx, h.acceptedAs(trig, payloadOf(t, trig), "2-0", &outboxID,
		map[string]string{"tenant_id": "default", "outbox_entry_id": outboxID.String()})))

	rems := h.store.Remediations()
	require.Len(t, rems, 1)
	assert.Equal(t, remediation.StatusRunning, rems[0].Status)
	assert.Equal(t, before.TriggerMessageID, rems[0].TriggerMessageID)
	assert.Equal(t, before.TriggerFields, rems[0].TriggerFields)
	msgs := h.store.ProcessedMessages()
	require.Len(t, msgs, 2)
	for _, m := range msgs {
		assert.Equal(t, messageprocessing.StateCompleted, m.State)
	}
}

// Once the release has a remediation of a later round, the operator already
// asked for a new round: a redrive of the earlier round's dead letter resumes
// nothing.
func TestRunner_ARedriveOfASupersededRoundResumesNothing(t *testing.T) {
	h := newHarness(t)
	id := h.sweptAsStuck()
	dls := h.deadLetters()
	require.Len(t, dls, 1)
	next := trigger("compile", compileNode("svc-a"))
	next.Round = 2
	require.NoError(t, h.runner.Accept(ctx, h.accepted(next, "3-0")))

	h.redrive(dls[0], "4-0")

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Equal(t, "1-0", rem.TriggerMessageID)
	assert.Equal(t, remediation.TryDrafting, h.try(id, 1).Status)
	assert.Equal(t, proposal.StatusFailed, h.attempt(1).Proposal.Status)
	assert.Len(t, h.store.Remediations(), 2)
	msgs := h.store.ProcessedMessages()
	require.Len(t, msgs, 3)
	for _, m := range msgs {
		assert.Equal(t, messageprocessing.StateCompleted, m.State, "the redriven message is claimed")
	}
}

// A system failure of a verifying try leaves the try verifying; a redrive puts
// its attempt row back to verifying, with its edits and runs and no verify
// error, and the gate's poller polls it again.
func TestRunner_ARedriveOfAVerifyingTryIsPolledAgain(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	verifying := h.attempt(1).Proposal
	require.Equal(t, proposal.StatusVerifying, verifying.Status)
	gate := h.flowRows(id, 1)["release_check"][0]
	gate.Failures = 5
	gate.LastError = "rebuilt run ids differ"
	h.saveStep(gate)
	require.NoError(t, h.sweeper().Sweep(ctx))
	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	require.Equal(t, proposal.StatusFailed, h.attempt(1).Proposal.Status)
	dls := h.deadLetters()
	require.Len(t, dls, 1)
	_, dl := deadLetterOf(t, dls[0])
	assert.Equal(t, int64(5), dl.DeliveryCount)

	h.redrive(dls[0], "2-0")

	row := h.attempt(1)
	assert.Equal(t, proposal.StatusVerifying, row.Proposal.Status)
	assert.Empty(t, row.VerifyError)
	assert.Equal(t, verifying.Edits, row.Proposal.Edits)
	assert.Equal(t, verifying.ReleaseChecks, row.Proposal.ReleaseChecks)
	assert.Equal(t, verifying.NodeOutcomes, row.Proposal.NodeOutcomes)
	assert.Equal(t, verifying.Rationale, row.Proposal.Rationale)
	gate = h.flowRows(id, 1)["release_check"][0]
	assert.Zero(t, gate.Failures)
	assert.Equal(t, remediation.StepWaiting, gate.Status)

	h.checks.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poller(func() {}).Poll(ctx))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Equal(t, 1, h.llm.calls())
}

// Each system failure is a dead letter of the message the remediation then
// answers to: one that ends again after a redrive is a second dead letter,
// with an event id of its own, which dead-letter-controller stores apart.
func TestRunner_ASecondSystemFailureAfterARedriveIsASecondDeadLetter(t *testing.T) {
	h := newHarness(t)
	id := h.sweptAsStuck()
	first := h.deadLetters()
	require.Len(t, first, 1)
	h.redrive(first[0], "2-0")
	require.Equal(t, remediation.StatusRunning, h.remediation(id).Status)

	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)
	require.NoError(t, h.sweeper().Sweep(ctx))

	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	dls := h.deadLetters()
	require.Len(t, dls, 2)
	assert.NotEqual(t, dls[0].ID, dls[1].ID)
	firstEnv, _ := deadLetterOf(t, dls[0])
	secondEnv, second := deadLetterOf(t, dls[1])
	assert.NotEqual(t, firstEnv.EventID, secondEnv.EventID)
	assert.Equal(t, "2-0", second.OriginalMessageID)
	assert.Equal(t, proposal.StatusFailed, h.attempt(1).Proposal.Status)
}

// A remediation that ended as a system failure keeps its try open for a
// redrive, but nothing acts on it before then: no worker claims it, the gate's
// poller does not poll it and the sweeper does not end it again, even past
// its deadline.
func TestRunner_APausedTryIsNotDrivenPolledOrSwept(t *testing.T) {
	t.Run("drafting", func(t *testing.T) {
		h := newHarness(t)
		id := h.sweptAsStuck()
		h.clock.set(t0.Add(draftingTime + time.Hour))
		repo := h.store.RemediationRepository()

		_, found, err := repo.ClaimNext(ctx, "worker-b", h.clock.Now(), lease)
		require.NoError(t, err)
		assert.False(t, found)
		late, err := repo.ListPastDeadline(ctx, h.clock.Now(), 10)
		require.NoError(t, err)
		assert.Empty(t, late)
		require.NoError(t, h.sweeper().Sweep(ctx))

		assert.Len(t, h.deadLetters(), 1)
		assert.Equal(t, remediation.TryDrafting, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	})
	t.Run("verifying", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))
		require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
		h.countRemediationFailures(id, 5)
		require.NoError(t, h.sweeper().Sweep(ctx))
		require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
		h.checks.answer(proposal.PhasePassed, nil)

		require.NoError(t, h.poller(func() {}).Poll(ctx))
		require.NoError(t, h.sweeper().Sweep(ctx))
		_, found, err := h.store.RemediationRepository().ClaimNext(ctx, "worker-b", h.clock.Now(), lease)
		require.NoError(t, err)

		assert.False(t, found)
		assert.Zero(t, h.checks.statusReads(), "the gate is not read")
		assert.Len(t, h.deadLetters(), 1)
		assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	})
}
