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
	h.checks.answer(proposal.PhasePassed, nil)
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
