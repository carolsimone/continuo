package redis

import (
	"context"
	"testing"

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
