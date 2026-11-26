package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// streamTap records every entry appended to a stream from the moment it starts.
// dead-letter-controller trims consumed entries every few minutes, so a test
// that inspects what a stream received starts a tap before triggering the work
// rather than reading the stream's history afterwards.
type streamTap struct {
	mu      sync.Mutex
	entries []goredis.XMessage
}

// startStreamTap taps the prefixed key of the contract stream name stream.
func startStreamTap(t *testing.T, ctx context.Context, rc *goredis.Client, stream string) *streamTap {
	t.Helper()
	from := "0-0"
	if info, err := rc.XInfoStream(ctx, pkgredis.Key(stream)).Result(); err == nil {
		from = info.LastGeneratedID
	}
	tap := &streamTap{}
	tctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for tctx.Err() == nil {
			res, err := rc.XRead(tctx, &goredis.XReadArgs{Streams: []string{pkgredis.Key(stream), from}, Block: time.Second, Count: 500}).Result()
			if err != nil {
				if !errors.Is(err, goredis.Nil) && tctx.Err() == nil {
					time.Sleep(200 * time.Millisecond)
				}
				continue
			}
			for _, s := range res {
				tap.mu.Lock()
				tap.entries = append(tap.entries, s.Messages...)
				tap.mu.Unlock()
				if n := len(s.Messages); n > 0 {
					from = s.Messages[n-1].ID
				}
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	return tap
}

// Entries returns a copy of the entries recorded so far, in stream order.
func (s *streamTap) Entries() []goredis.XMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]goredis.XMessage(nil), s.entries...)
}

// tappedDispatch is one task.dispatch:v1 entry a tap recorded: its payload and
// the outbox row that carried it.
type tappedDispatch struct {
	pkgevents.TaskDispatch
	OutboxEntryID string
}

// entryFields returns a stream entry's string-valued fields, the form the
// pkg/events decoders read.
func entryFields(msg goredis.XMessage) map[string]string {
	out := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// Dispatches decodes the task.dispatch:v1 entries recorded so far, in stream
// order. An entry that does not decode fails the test.
func (s *streamTap) Dispatches(t *testing.T) []tappedDispatch {
	t.Helper()
	entries := s.Entries()
	out := make([]tappedDispatch, 0, len(entries))
	for _, msg := range entries {
		fields := entryFields(msg)
		_, d, err := pkgevents.DecodeTaskDispatch(fields)
		require.NoError(t, err, "entry %s is not a task.dispatch:v1 entry", msg.ID)
		out = append(out, tappedDispatch{TaskDispatch: d, OutboxEntryID: fields["outbox_entry_id"]})
	}
	return out
}

// tappedTrigger is one heal.triggered:v1 entry a tap recorded: its
// payload, the tenant its envelope names and the outbox row that carried it.
type tappedTrigger struct {
	pkgevents.HealTriggered
	TenantID      string
	OutboxEntryID string
}

// findNode returns the trigger's entry for a node id, and whether it is
// present. Callers assert on the entry rather than on the trigger as a whole,
// because one batched trigger can carry several failing nodes.
func (p tappedTrigger) findNode(nodeID string) (pkgevents.HealTriggeredNode, bool) {
	for _, n := range p.Nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}
	return pkgevents.HealTriggeredNode{}, false
}

// Triggers decodes the heal.triggered:v1 entries recorded so far, in
// stream order. An entry that does not decode fails the test.
func (s *streamTap) Triggers(t *testing.T) []tappedTrigger {
	t.Helper()
	entries := s.Entries()
	out := make([]tappedTrigger, 0, len(entries))
	for _, msg := range entries {
		fields := entryFields(msg)
		env, p, err := pkgevents.DecodeHealTriggered(fields)
		require.NoError(t, err, "entry %s is not a heal.triggered:v1 entry", msg.ID)
		out = append(out, tappedTrigger{HealTriggered: p, TenantID: env.TenantID, OutboxEntryID: fields["outbox_entry_id"]})
	}
	return out
}

// tappedHandOver is one remediation.handed_over:v1 entry a tap recorded: its
// payload and the tenant its envelope names.
type tappedHandOver struct {
	pkgevents.RemediationHandedOver
	TenantID string
}

// HandOvers decodes the remediation.handed_over:v1 entries recorded so far, in
// stream order: the envelope with pkgevents.ParseEnvelope, which must carry
// the event's schema version, and the payload as JSON. An entry that does not
// decode fails the test.
func (s *streamTap) HandOvers(t *testing.T) []tappedHandOver {
	t.Helper()
	entries := s.Entries()
	out := make([]tappedHandOver, 0, len(entries))
	for _, msg := range entries {
		env, payload, err := pkgevents.ParseEnvelope(entryFields(msg))
		require.NoError(t, err, "entry %s is not an enveloped remediation.handed_over:v1 entry", msg.ID)
		require.Equal(t, pkgevents.RemediationHandedOverSchemaVersion, env.SchemaVersion,
			"entry %s: remediation.handed_over:v1 schema_version", msg.ID)
		var p pkgevents.RemediationHandedOver
		require.NoError(t, json.Unmarshal(payload, &p), "entry %s: remediation.handed_over:v1 payload %s", msg.ID, payload)
		out = append(out, tappedHandOver{RemediationHandedOver: p, TenantID: env.TenantID})
	}
	return out
}
