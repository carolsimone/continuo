package redis

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/google/uuid"
)

var _ pkgoutbox.Renderer = (*releaseOutboxPublisher)(nil)

func TestRender_PayloadFieldWithoutOutboxEntryID(t *testing.T) {
	p := &releaseOutboxPublisher{}
	values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", Payload: []byte(`{"release_id":"r1"}`)})
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
	p := &releaseOutboxPublisher{}
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: "release_promoted", StreamName: "some.stream:v1", Payload: []byte(`{}`)}
	values, err := p.Render(entry)
	if err != nil {
		t.Fatal(err)
	}
	args := p.xaddArgs(entry, values)
	if args.Stream != "some.stream:v1" {
		t.Fatalf("stream = %q", args.Stream)
	}
	if args.MaxLen != 0 || args.MinID != "" || args.Approx {
		t.Fatalf("publisher trims: MaxLen=%d MinID=%q Approx=%v", args.MaxLen, args.MinID, args.Approx)
	}
	if args.Values.(map[string]any)["outbox_entry_id"] != entry.ID.String() {
		t.Fatalf("outbox_entry_id missing from %v", args.Values)
	}
}

// A release.promoted:v2 row's payload is the typed event; Render wraps it in
// the envelope, deriving the event id from the seq and occurred_at from the
// row's creation, so every retry publishes identical fields.
func TestRender_ReleasePromotedV2RowCarriesTheEnvelope(t *testing.T) {
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
	values, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: ports.ReleasePromotedV2EventType, Payload: payload, CreatedAt: created})
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

func TestRender_MalformedReleasePromotedV2RowIsPermanent(t *testing.T) {
	p := &releaseOutboxPublisher{}
	_, err := p.Render(&pkgoutbox.Entry{ID: uuid.New(), EventType: ports.ReleasePromotedV2EventType, Payload: []byte(`{`)})
	if !errors.Is(err, pkgevents.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}
