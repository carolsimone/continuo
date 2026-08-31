package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestTrigger_NodeIDsAreSortedAndIndependentOfInputOrder pins the order every
// batched write derives from: the resolved node set, the representative node,
// and the attempt's node outcomes all follow it, so it must not depend on the
// order the classifier happened to list the failures in.
func TestTrigger_NodeIDsAreSortedAndIndependentOfInputOrder(t *testing.T) {
	tr := Trigger{Nodes: []TriggerNode{{NodeID: "s.c"}, {NodeID: "s.a"}, {NodeID: "s.b"}}}
	assert.Equal(t, []string{"s.a", "s.b", "s.c"}, tr.NodeIDs())
	assert.Equal(t, "s.c", tr.Nodes[0].NodeID, "NodeIDs must sort a copy, leaving the trigger's own nodes as delivered")
}

// TestTrigger_IdempotencyKeyPrefersTheUpstreamOutboxRow pins the LLM cache
// identity: an upstream republish of the same logical trigger with a fresh
// Redis message id reuses the cached completions, while a genuinely new trigger
// does not.
func TestTrigger_IdempotencyKeyPrefersTheUpstreamOutboxRow(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	withOutbox := Trigger{MessageID: "1-0", OutboxEntryID: &id}
	republished := Trigger{MessageID: "2-0", OutboxEntryID: &id}
	assert.Equal(t, withOutbox.idempotencyKey(), republished.idempotencyKey())
	assert.Equal(t, "msg:1-0", Trigger{MessageID: "1-0"}.idempotencyKey())
}
