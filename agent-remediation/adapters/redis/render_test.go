package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ pkgoutbox.Renderer = (*agentRemediationOutboxPublisher)(nil)

func TestRender_PayloadFieldWithoutOutboxEntryID(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{"release_id":"r1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if values["payload"] != `{"release_id":"r1"}` {
		t.Fatalf("values = %v", values)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

// TestRender_StampsTheDefaultTenant holds that every message the service
// publishes carries tenant_id beside its payload, and that a dead-letter row's
// fields are rendered as they are. The stamp is in Render, which feeds both the
// XADD and the original fields an outbox dead letter stores, so a redrive of a
// dead-lettered message keeps its tenant.
func TestRender_StampsTheDefaultTenant(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}

	t.Run("a published message holds its payload and the default tenant", func(t *testing.T) {
		values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{"release_id":"r1"}`)})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"payload": `{"release_id":"r1"}`, "tenant_id": pkgevents.DefaultTenantID}, values)
		assert.Equal(t, "default", values["tenant_id"])
	})

	t.Run("the fields an outbox dead letter stores carry the tenant", func(t *testing.T) {
		values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{}`)})
		require.NoError(t, err)
		stored, err := pkgoutbox.StringifyFields(values)
		require.NoError(t, err)
		assert.Equal(t, "default", stored["tenant_id"])
	})

	t.Run("a dead-letter row's fields are unchanged", func(t *testing.T) {
		entry := &pkgoutbox.Entry{
			ID: uuid.New(), EventType: pkgoutbox.DeadLetterEventType,
			Payload: []byte(`{"failure_kind":"permanent","original_event_type":"node_updated"}`),
		}
		want, err := pkgoutbox.DeadLetterValues(entry)
		require.NoError(t, err)
		delete(want, "outbox_entry_id")

		values, err := p.Render(entry)
		require.NoError(t, err)
		assert.Equal(t, want, values)
		assert.NotContains(t, values, "tenant_id")
	})
}

// captureXAdd answers every command a go-redis client sends without a server
// and records it.
type captureXAdd struct{ cmds []goredis.Cmder }

func (c *captureXAdd) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (c *captureXAdd) ProcessHook(goredis.ProcessHook) goredis.ProcessHook {
	return func(_ context.Context, cmd goredis.Cmder) error {
		c.cmds = append(c.cmds, cmd)
		return nil
	}
}

func (c *captureXAdd) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// TestPublish_XAddCarriesTenantAndOutboxEntryID holds that the XADD Publish
// sends names the entry's stream and carries payload, tenant_id and
// outbox_entry_id.
func TestPublish_XAddCarriesTenantAndOutboxEntryID(t *testing.T) {
	capture := &captureXAdd{}
	rc := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	rc.AddHook(capture)
	t.Cleanup(func() { _ = rc.Close() })

	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", StreamName: streams.RemediationPrOpenedV1, Payload: []byte(`{"release_id":"r1"}`)}
	require.NoError(t, (&agentRemediationOutboxPublisher{redis: rc}).Publish(context.Background(), entry))

	require.Len(t, capture.cmds, 1)
	args := capture.cmds[0].Args()
	require.GreaterOrEqual(t, len(args), 4)
	assert.Equal(t, "xadd", args[0])
	assert.Equal(t, pkgredis.Key(streams.RemediationPrOpenedV1), args[1])
	assert.Equal(t, "nomkstream", args[2], "the XADD never creates the stream")
	assert.Equal(t, "*", args[3], "the XADD sets no length cap")
	fields := map[string]any{}
	for i := 4; i+1 < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	assert.Equal(t, map[string]any{
		"payload":         `{"release_id":"r1"}`,
		"tenant_id":       "default",
		"outbox_entry_id": entry.ID.String(),
	}, fields)
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	values, err := p.Render(&pkgoutbox.Entry{
		ID: uuid.New(), EventType: pkgoutbox.DeadLetterEventType,
		Payload: []byte(`{"failure_kind":"permanent","original_event_type":"node_updated"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if values["failure_kind"] != "permanent" {
		t.Fatalf("values = %v", values)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{}`)}
	first, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	first["payload"] = "mutated"
	second, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	if second["payload"] != "{}" {
		t.Fatalf("second render = %v, want a fresh map", second)
	}
}

// TestXAddArgs_DoNotTrim asserts the publisher never trims a stream: the
// dead-letter-controller's trim loop is the only thing that bounds them.
func TestXAddArgs_DoNotTrim(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", StreamName: streams.RemediationPrOpenedV1, Payload: []byte(`{}`)}
	values, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	args := p.xaddArgs(entry, values)
	if args.Stream != pkgredis.Key(streams.RemediationPrOpenedV1) {
		t.Fatalf("stream = %q", args.Stream)
	}
	if !args.NoMkStream {
		t.Fatal("the publisher must not create a stream: NOMKSTREAM is unset")
	}
	if args.MaxLen != 0 || args.MinID != "" || args.Approx {
		t.Fatalf("publisher trims: MaxLen=%d MinID=%q Approx=%v", args.MaxLen, args.MinID, args.Approx)
	}
	if args.Values.(map[string]any)["outbox_entry_id"] != entry.ID.String() {
		t.Fatalf("outbox_entry_id missing from %v", args.Values)
	}
}

// TestRender_ProposedRowStoresItsTenant holds that a remediation.proposed:v1
// row, stored under its tenant, renders as the payload it wraps beside that
// tenant, and that a row stored under no tenant is refused as permanent:
// no retry makes it renderable.
func TestRender_ProposedRowStoresItsTenant(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	const body = `{"release_id":"r1","attempt":1}`

	t.Run("renders the wrapped payload beside the stored tenant", func(t *testing.T) {
		stored, err := pkgoutbox.WrapTenant("acme", []byte(body))
		require.NoError(t, err)
		values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "remediation_proposed", StreamName: streams.RemediationProposedV1, Payload: stored})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"payload": body, "tenant_id": "acme"}, values)
	})

	t.Run("the default tenant renders as stored", func(t *testing.T) {
		stored, err := pkgoutbox.WrapTenant(pkgevents.DefaultTenantID, []byte(body))
		require.NoError(t, err)
		values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), StreamName: streams.RemediationProposedV1, Payload: stored})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"payload": body, "tenant_id": pkgevents.DefaultTenantID}, values)
	})

	t.Run("a row stored under no tenant is permanent", func(t *testing.T) {
		for name, payload := range map[string]string{
			"an unwrapped payload": body,
			"an empty tenant":      `{"tenant_id":"","payload":{"release_id":"r1"}}`,
			"no payload":           `{"tenant_id":"acme"}`,
		} {
			_, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), StreamName: streams.RemediationProposedV1, Payload: []byte(payload)})
			require.ErrorIs(t, err, pkgevents.ErrPermanent, name)
			require.ErrorIs(t, err, pkgoutbox.ErrNoTenant, name)
		}
	})
}

// TestRender_PrOpenedAndPrClosedRowsKeepTheDefaultTenant holds that the rows
// agent-remediation announces for a pull request's lifecycle are stored as
// their payload and render beside the default tenant.
func TestRender_PrOpenedAndPrClosedRowsKeepTheDefaultTenant(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	for _, stream := range []string{streams.RemediationPrOpenedV1, streams.RemediationPrClosedV1} {
		values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), StreamName: stream, Payload: []byte(`{"release_id":"r1"}`)})
		require.NoError(t, err, stream)
		assert.Equal(t, map[string]any{"payload": `{"release_id":"r1"}`, "tenant_id": pkgevents.DefaultTenantID}, values, stream)
	}
}

// TestPublish_ProposedRowXAddCarriesItsStoredTenant holds that the XADD of a
// remediation.proposed:v1 row carries the payload it wraps, the tenant it is
// stored under and the entry's outbox_entry_id.
func TestPublish_ProposedRowXAddCarriesItsStoredTenant(t *testing.T) {
	capture := &captureXAdd{}
	rc := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	rc.AddHook(capture)
	t.Cleanup(func() { _ = rc.Close() })

	stored, err := pkgoutbox.WrapTenant("acme", []byte(`{"release_id":"r1"}`))
	require.NoError(t, err)
	entry := &pkgoutbox.Entry{ID: uuid.New(), StreamName: streams.RemediationProposedV1, Payload: stored}
	require.NoError(t, (&agentRemediationOutboxPublisher{redis: rc}).Publish(context.Background(), entry))

	require.Len(t, capture.cmds, 1)
	args := capture.cmds[0].Args()
	assert.Equal(t, pkgredis.Key(streams.RemediationProposedV1), args[1])
	fields := map[string]any{}
	for i := 4; i+1 < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	assert.Equal(t, map[string]any{
		"payload":         `{"release_id":"r1"}`,
		"tenant_id":       "acme",
		"outbox_entry_id": entry.ID.String(),
	}, fields)
}

// TestRender_HandedOverCarriesTheRowsTenant holds that a
// remediation.handed_over:v1 row, stored under its tenant, renders as the
// event's envelope: the row's id as event_id, the tenant the row is stored
// under, the row's creation time, agent-remediation as producer and the event
// it wraps as payload; and that a row stored under no tenant, whose stored
// payload is not the event, or whose event the event's decoder refuses, is
// refused as permanent: no retry makes it renderable.
func TestRender_HandedOverCarriesTheRowsTenant(t *testing.T) {
	p := &agentRemediationOutboxPublisher{}
	handed := pkgevents.RemediationHandedOver{
		RemediationID: "0b6f8c4e-3a1d-4d2b-9f7e-5c8a1b2d3e4f", ReleaseID: "rel-1", RemediationRound: 1, Flow: "rejected-release", Tries: 3,
		Nodes: []pkgevents.HandedOverNode{{NodeID: "model.a", Reason: string(model.HandoverReasonTriesSpent), Message: "tried 3 times"}},
	}
	body, err := json.Marshal(handed)
	require.NoError(t, err)
	stored, err := pkgoutbox.WrapTenant("acme", body)
	require.NoError(t, err)
	entry := &pkgoutbox.Entry{
		ID: uuid.New(), EventType: "remediation_handed_over", StreamName: streams.RemediationHandedOverV1,
		Payload: stored, CreatedAt: time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC),
	}

	values, err := p.Render(entry)
	require.NoError(t, err)

	want, err := pkgevents.RemediationHandedOverFields(entry.ID.String(), "acme", "agent-remediation", entry.CreatedAt, handed)
	require.NoError(t, err)
	assert.Equal(t, want, values)
	assert.Equal(t, "acme", values["tenant_id"])
	fields, err := pkgoutbox.StringifyFields(values)
	require.NoError(t, err)
	env, got, err := pkgevents.DecodeRemediationHandedOver(fields)
	require.NoError(t, err)
	assert.Equal(t, entry.ID.String(), env.EventID)
	assert.Equal(t, "acme", env.TenantID)
	assert.Equal(t, "agent-remediation", env.Producer)
	assert.True(t, entry.CreatedAt.Equal(env.OccurredAt))
	assert.Equal(t, handed, got)

	for name, payload := range map[string]string{
		"an unwrapped payload":         string(body),
		"an empty tenant":              `{"tenant_id":"","payload":{"release_id":"rel-1"}}`,
		"a payload that is not the event": `{"tenant_id":"acme","payload":["not","the","event"]}`,
		"a reason outside the vocabulary": `{"tenant_id":"acme","payload":{"remediation_id":"r","release_id":"rel-1","remediation_round":1,"flow":"rejected-release","tries":1,` +
			`"nodes":[{"node_id":"model.a","reason":"gave_up","message":"m"}]}}`,
		"no remediation round": `{"tenant_id":"acme","payload":{"remediation_id":"r","release_id":"rel-1","remediation_round":0,"flow":"rejected-release","tries":1,"nodes":[]}}`,
	} {
		_, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), StreamName: streams.RemediationHandedOverV1, Payload: []byte(payload)})
		require.ErrorIs(t, err, pkgevents.ErrPermanent, name)
	}
}
