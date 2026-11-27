package redis

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/google/uuid"
)

var _ pkgoutbox.Renderer = (*releaseOutboxPublisher)(nil)

func TestRender_PayloadFieldWithoutOutboxEntryID(t *testing.T) {
	p := &releaseOutboxPublisher{}
	values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "release_requested", Payload: []byte(`{"release_id":"r1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values["payload"] != `{"release_id":"r1"}` {
		t.Fatalf("values = %v", values)
	}
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := &releaseOutboxPublisher{}
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
	p := &releaseOutboxPublisher{}
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_requested", Payload: []byte(`{}`)}
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
	p := &releaseOutboxPublisher{}
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_requested", StreamName: "some.stream:v1", Payload: []byte(`{}`)}
	values, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	args := p.xaddArgs(entry, values)
	if args.Stream != pkgredis.Key("some.stream:v1") {
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

// A release.promoted:v1 row's payload is the typed event; Render wraps it in
// the envelope, deriving the event id from the seq and occurred_at from the
// row's creation, so every retry publishes identical fields.
func TestRender_ReleasePromotedRowCarriesTheEnvelope(t *testing.T) {
	created := time.Date(2026, 10, 8, 12, 0, 0, 123456000, time.UTC)
	want := pkgevents.ReleasePromoted{
		ReleaseID:       "r7",
		PromotedAt:      created,
		PromotionSeq:    7,
		TopologyURI:     "s3://b/tenants/default/topologies/r7/topology.json.gz",
		TopologySHA256:  "f00d",
		ChangedNodeIDs:  []string{"a"},
		CandidateSchema: "_candidate_r7",
		CodeBundleURI:   "s3://b/code-bundles/r7/bundle.json",
		Repo:            "acme/demo",
		CommitSHA:       "deadbeef",
	}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	p := &releaseOutboxPublisher{}
	values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: ports.ReleasePromotedEventType, Payload: payload, CreatedAt: created})
	if err != nil {
		t.Fatal(err)
	}
	fields, err := pkgoutbox.StringifyFields(values)
	if err != nil {
		t.Fatal(err)
	}
	env, got, err := pkgevents.DecodeReleasePromoted(fields)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReleaseID != want.ReleaseID || got.PromotionSeq != 7 || got.TopologySHA256 != "f00d" || len(got.ChangedNodeIDs) != 1 {
		t.Fatalf("payload = %+v", got)
	}
	if env.EventID != pkgevents.ReleasePromotedEventID(pkgevents.DefaultTenantID, 7) {
		t.Fatalf("event_id = %q", env.EventID)
	}
	if env.Producer != "release-controller" || env.TenantID != pkgevents.DefaultTenantID || !env.OccurredAt.Equal(created) {
		t.Fatalf("envelope = %+v", env)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

func TestRender_MalformedReleasePromotedRowIsPermanent(t *testing.T) {
	p := &releaseOutboxPublisher{}
	_, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: ports.ReleasePromotedEventType, Payload: []byte(`{`)})
	if !errors.Is(err, pkgevents.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}

// retryRow builds the outbox row RetryRemediation stores for the retry of a
// release: the typed payload wrapped under tenantID, on the v2 stream.
func retryRow(t *testing.T, tenantID string, created time.Time, p pkgevents.RoundRequested) *pkgoutbox.Entry {
	t.Helper()
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := pkgoutbox.WrapTenant(tenantID, payload)
	if err != nil {
		t.Fatal(err)
	}
	return &pkgoutbox.Entry{
		ID:         uuid.New(),
		EventType:  "round_requested",
		Payload:    stored,
		StreamName: streams.RoundRequestedV1,
		CreatedAt:  created,
	}
}

func TestRender_RetryRowCarriesTheStoredTenantAndDecodes(t *testing.T) {
	created := time.Date(2026, 11, 12, 14, 5, 33, 0, time.UTC)
	rejection := json.RawMessage(`{"release_id":"r1","stage":"compile","reason":"compile_failed"}`)
	entry := retryRow(t, "acme", created, pkgevents.RoundRequested{ReleaseID: "r1", Round: 2, Rejection: rejection})

	p := &releaseOutboxPublisher{}
	values, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
	fields, err := pkgoutbox.StringifyFields(values)
	if err != nil {
		t.Fatal(err)
	}
	env, got, err := pkgevents.DecodeRoundRequested(fields)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReleaseID != "r1" || got.Round != 2 || string(got.Rejection) != string(rejection) {
		t.Fatalf("payload = %+v", got)
	}
	if env.TenantID != "acme" {
		t.Fatalf("tenant = %q, want the stored tenant", env.TenantID)
	}
	if env.EventID != entry.ID.String() || env.Producer != "release-controller" || !env.OccurredAt.Equal(created) {
		t.Fatalf("envelope = %+v", env)
	}
}

func TestRender_RetryRowThatCannotBeRenderedIsPermanent(t *testing.T) {
	created := time.Date(2026, 11, 12, 14, 5, 33, 0, time.UTC)
	rejected := json.RawMessage(`{"release_id":"r1"}`)
	unwrapped := retryRow(t, "acme", created, pkgevents.RoundRequested{ReleaseID: "r1", Round: 2, Rejection: rejected})
	unwrapped.Payload = []byte(`{"release_id":"r1","round":2,"rejection":{}}`)
	notJSON := retryRow(t, "acme", created, pkgevents.RoundRequested{ReleaseID: "r1", Round: 2, Rejection: rejected})
	notJSON.Payload = []byte(`{"tenant_id":"acme","payload":"not an object"}`)

	cases := map[string]*pkgoutbox.Entry{
		"a payload with no tenant":         unwrapped,
		"a payload that is no retry":       notJSON,
		"a stored payload that is no JSON": {ID: uuid.New(), Payload: []byte(`{`), StreamName: streams.RoundRequestedV1, CreatedAt: created},
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			p := &releaseOutboxPublisher{}
			if _, err := p.Render(entry); !errors.Is(err, pkgevents.ErrPermanent) {
				t.Fatalf("err = %v, want ErrPermanent", err)
			}
		})
	}
}
