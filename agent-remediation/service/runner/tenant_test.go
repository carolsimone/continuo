package runner_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// TestRunner_RowsAndEventsCarryTheTriggersTenant verifies that the tenant a
// trigger arrives under is the one its remediation row is stored under and the
// one the remediation.proposed:v1 entry it ends in is stored under, so the
// event is published under that tenant and no other.
func TestRunner_RowsAndEventsCarryTheTriggersTenant(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	trig.TenantID = "acme"
	id := h.accept(trig)
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	assert.Equal(t, "acme", h.remediation(id).TenantID)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)

	events := h.proposedEvents()
	require.Len(t, events, 1)
	tenantID, body := proposedPayload(t, events[0])
	assert.Equal(t, "acme", tenantID)
	var payload struct {
		ReleaseID string `json:"release_id"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, releaseID, payload.ReleaseID)
}

// TestRunner_DrivesARemediationStoredFromAV2Payload verifies a remediation whose
// stored trigger payload carries an event_id of its own, as a
// remediation.requested:v2 payload does, is driven like any other: its trigger
// decodes from the stored bytes.
func TestRunner_DrivesARemediationStoredFromAV2Payload(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	var stored map[string]any
	require.NoError(t, json.Unmarshal(payloadOf(t, trig), &stored))
	stored["event_id"] = "evt-v2"
	stored["classified_at"] = "2026-08-28T00:00:00Z"
	a := h.accepted(trig, "1-0")
	var err error
	a.Payload, err = json.Marshal(stored)
	require.NoError(t, err)
	require.NoError(t, h.runner.Accept(ctx, a))
	rems := h.store.Remediations()
	require.Len(t, rems, 1)
	claimed, err := h.store.RemediationRepository().Claim(ctx, rems[0].ID, workerOwner, h.clock.Now(), lease)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, h.runner.Drive(ctx, rems[0].ID))

	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status, "the trigger's node was fixed and its edits sent to verification")
}
