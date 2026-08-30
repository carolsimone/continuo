package handlers

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrigger_SubsetKeepsHeaderAndNamedNodes(t *testing.T) {
	tr := Trigger{ReleaseID: "r", Source: "validation", RemediationRound: 2, Repo: "o/r",
		Nodes: []TriggerNode{{NodeID: "s.a"}, {NodeID: "s.b"}, {NodeID: "s.c"}}}
	sub := tr.Subset([]string{"s.c", "s.a"})
	if got := sub.NodeIDs(); len(got) != 2 || got[0] != "s.a" || got[1] != "s.c" {
		t.Fatalf("want [s.a s.c], got %v", got)
	}
	if sub.ReleaseID != "r" || sub.RemediationRound != 2 || sub.Repo != "o/r" {
		t.Fatalf("header must be preserved: %+v", sub)
	}
	if len(sub.RawPayload) == 0 {
		t.Fatal("subset must carry a payload the reconciler can store and replay")
	}
}

// TestTrigger_SubsetPayloadRoundTripsThroughTheWire proves the bytes Subset
// writes are the same wire shape the stream carries: decoding them again must
// rebuild the narrowed trigger, since the reconciler stores them and replays
// them through the adapter's decoder.
func TestTrigger_SubsetPayloadRoundTripsThroughTheWire(t *testing.T) {
	tr := Trigger{
		Source: "validation", ReleaseID: "r", RemediationRound: 3,
		Repo: "o/r", CommitSHA: "sha", CodeBundleURI: "s3://b/bundle.json",
		Nodes: []TriggerNode{
			{NodeID: "s.a", RelationID: "analytics.a", ErrorSignature: "sig", Category: "logic",
				Reason: "logic:missing_object", ErrorExcerpt: "column x does not exist",
				DBTLogURI: "s3://l/a", CandidateArtifactURI: "s3://c/a", FilePath: "models/a.sql",
				Service: "svc", NodeType: "dbt-model", OtherService: "other", OtherFilePath: "models/z.sql",
				ChangedAncestorIDs: []string{"s.u"}},
			{NodeID: "s.b", ErrorSignature: "sig"},
		},
	}

	sub := tr.Subset([]string{"s.a"})

	var wire TriggerWire
	require.NoError(t, json.Unmarshal(sub.RawPayload, &wire))
	assert.Equal(t, "validation", wire.Source)
	assert.Equal(t, "r", wire.ReleaseID)
	assert.Equal(t, 3, wire.RemediationRound)
	assert.Equal(t, "s3://b/bundle.json", wire.CodeBundleURI)
	require.Len(t, wire.Nodes, 1)
	assert.Equal(t, TriggerNodeWire{
		NodeID: "s.a", RelationID: "analytics.a", Category: "logic", ErrorSignature: "sig",
		Reason: "logic:missing_object", ErrorExcerpt: "column x does not exist",
		DBTLogURI: "s3://l/a", CandidateArtifactURI: "s3://c/a", FilePath: "models/a.sql",
		Service: "svc", NodeType: "dbt-model", OtherService: "other", OtherFilePath: "models/z.sql",
		ChangedAncestorIDs: []string{"s.u"},
	}, wire.Nodes[0], "every field a fix reads must survive the narrowed payload")
}

// TestTrigger_SubsetDropsTheDeliveringMessagesIdentity keeps a replayed subset
// from inheriting the dedup identity of the message the first attempt consumed:
// the caller that replays it supplies an identity of its own.
func TestTrigger_SubsetDropsTheDeliveringMessagesIdentity(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	tr := Trigger{ReleaseID: "r", MessageID: "1-0", OutboxEntryID: &id,
		Nodes: []TriggerNode{{NodeID: "s.a"}, {NodeID: "s.b"}}}

	sub := tr.Subset([]string{"s.a"})

	assert.Empty(t, sub.MessageID)
	assert.Nil(t, sub.OutboxEntryID)
}

// TestTrigger_SubsetIgnoresUnknownNodes pins that a subset names nodes this
// trigger actually carries: an id the trigger never held cannot become an empty
// node the fixers would then be handed.
func TestTrigger_SubsetIgnoresUnknownNodes(t *testing.T) {
	tr := Trigger{ReleaseID: "r", Nodes: []TriggerNode{{NodeID: "s.a"}, {NodeID: "s.b"}}}
	sub := tr.Subset([]string{"s.a", "s.zzz"})
	assert.Equal(t, []string{"s.a"}, sub.NodeIDs())
}

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
