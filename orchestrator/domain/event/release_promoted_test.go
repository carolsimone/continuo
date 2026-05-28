package event_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
)

func TestReleasePromoted_JSONRoundTrip(t *testing.T) {
	original := event.ReleasePromoted{
		ReleaseID: "rel-abc-123",
		Topology: []event.ReleasePromotedNode{
			{
				UniqueID:          "model.svc_a.table_x",
				SchemaName:        "public",
				TableName:         "table_x",
				ServiceName:       "svc_a",
				ImageTag:          "sha256:aabbcc",
				Schedule:          "0 * * * *",
				UpstreamUniqueIDs: []string{"model.svc_b.table_y"},
			},
			{
				UniqueID:          "model.svc_b.table_y",
				SchemaName:        "analytics",
				TableName:         "table_y",
				ServiceName:       "svc_b",
				ImageTag:          "sha256:ddeeff",
				Schedule:          "",
				UpstreamUniqueIDs: nil,
			},
		},
		ImageTags: map[string]string{
			"svc_a": "sha256:aabbcc",
			"svc_b": "sha256:ddeeff",
		},
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got event.ReleasePromoted
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.ReleaseID != original.ReleaseID {
		t.Errorf("ReleaseID: got %q, want %q", got.ReleaseID, original.ReleaseID)
	}
	if len(got.Topology) != len(original.Topology) {
		t.Fatalf("Topology length: got %d, want %d", len(got.Topology), len(original.Topology))
	}
	for i, n := range got.Topology {
		want := original.Topology[i]
		if n.UniqueID != want.UniqueID {
			t.Errorf("Topology[%d].UniqueID: got %q, want %q", i, n.UniqueID, want.UniqueID)
		}
		if n.SchemaName != want.SchemaName {
			t.Errorf("Topology[%d].SchemaName: got %q, want %q", i, n.SchemaName, want.SchemaName)
		}
		if n.TableName != want.TableName {
			t.Errorf("Topology[%d].TableName: got %q, want %q", i, n.TableName, want.TableName)
		}
		if n.ServiceName != want.ServiceName {
			t.Errorf("Topology[%d].ServiceName: got %q, want %q", i, n.ServiceName, want.ServiceName)
		}
		if n.ImageTag != want.ImageTag {
			t.Errorf("Topology[%d].ImageTag: got %q, want %q", i, n.ImageTag, want.ImageTag)
		}
		if n.Schedule != want.Schedule {
			t.Errorf("Topology[%d].Schedule: got %q, want %q", i, n.Schedule, want.Schedule)
		}
		if len(n.UpstreamUniqueIDs) != len(want.UpstreamUniqueIDs) {
			t.Errorf("Topology[%d].UpstreamUniqueIDs length: got %d, want %d", i, len(n.UpstreamUniqueIDs), len(want.UpstreamUniqueIDs))
		} else {
			for j, uid := range n.UpstreamUniqueIDs {
				if uid != want.UpstreamUniqueIDs[j] {
					t.Errorf("Topology[%d].UpstreamUniqueIDs[%d]: got %q, want %q", i, j, uid, want.UpstreamUniqueIDs[j])
				}
			}
		}
	}
	if len(got.ImageTags) != len(original.ImageTags) {
		t.Errorf("ImageTags length: got %d, want %d", len(got.ImageTags), len(original.ImageTags))
	}
	for k, v := range original.ImageTags {
		if got.ImageTags[k] != v {
			t.Errorf("ImageTags[%q]: got %q, want %q", k, got.ImageTags[k], v)
		}
	}
}

func TestReleasePromoted_JSONFieldNames(t *testing.T) {
	// Verify exact JSON key names match the wire format emitted by release-controller.
	payload := `{
		"release_id": "rel-xyz",
		"topology": [
			{
				"unique_id": "model.a.tbl",
				"schema_name": "pub",
				"table_name": "tbl",
				"service_name": "svc",
				"image_tag": "sha:ff",
				"schedule": "daily",
				"upstream_unique_ids": ["model.b.other"]
			}
		],
		"image_tags": {"svc": "sha:ff"}
	}`

	var got event.ReleasePromoted
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("unmarshal wire payload: %v", err)
	}
	if got.ReleaseID != "rel-xyz" {
		t.Errorf("release_id: got %q", got.ReleaseID)
	}
	if len(got.Topology) != 1 {
		t.Fatalf("topology length: got %d", len(got.Topology))
	}
	n := got.Topology[0]
	if n.UniqueID != "model.a.tbl" {
		t.Errorf("unique_id: got %q", n.UniqueID)
	}
	if len(n.UpstreamUniqueIDs) != 1 || n.UpstreamUniqueIDs[0] != "model.b.other" {
		t.Errorf("upstream_unique_ids: got %v", n.UpstreamUniqueIDs)
	}
	if got.ImageTags["svc"] != "sha:ff" {
		t.Errorf("image_tags[svc]: got %q", got.ImageTags["svc"])
	}
}
