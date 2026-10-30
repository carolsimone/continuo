package graphschema_test

import (
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/graphschema"
)

func TestGraphSchema_EveryLabelHasAnIdentity(t *testing.T) {
	labels := graphschema.Labels()
	if len(labels) != 10 {
		t.Fatalf("Labels() returned %d labels, want 10: %v", len(labels), labels)
	}
	seen := map[graphschema.Label]bool{}
	for _, l := range labels {
		if seen[l] {
			t.Errorf("label %q is listed twice", l)
		}
		seen[l] = true
		identity := graphschema.Identity(l)
		if len(identity) == 0 {
			t.Errorf("label %q has no identity", l)
		}
		for _, p := range identity {
			if p == "" || p == "tenant_id" {
				t.Errorf("label %q: identity property %q must be a non-empty property beside tenant_id", l, p)
			}
		}
	}
	if got := graphschema.Identity(graphschema.Label("Table")); got != nil {
		t.Errorf("Identity of a label outside the schema = %v, want nil", got)
	}
}

func TestGraphSchema_IdentitiesMatchTheDesign(t *testing.T) {
	want := map[graphschema.Label][]string{
		graphschema.Node:             {"unique_id"},
		graphschema.Meta:             {"key"},
		graphschema.NodeVersion:      {"unique_id", "content_hash"},
		graphschema.CodeUnit:         {"unit_id"},
		graphschema.CodeUnitVersion:  {"unit_id", "checksum"},
		graphschema.Rejection:        {"release_id", "node_id"},
		graphschema.ErrorSignature:   {"signature"},
		graphschema.Proposal:         {"proposal_id"},
		graphschema.PullRequest:      {"proposal_id", "service"},
		graphschema.ProcessedMessage: {"group", "message_id"},
	}
	for l, props := range want {
		got := graphschema.Identity(l)
		if len(got) != len(props) {
			t.Errorf("Identity(%q) = %v, want %v", l, got, props)
			continue
		}
		for i := range props {
			if got[i] != props[i] {
				t.Errorf("Identity(%q) = %v, want %v", l, got, props)
				break
			}
		}
	}
}

func TestGraphSchema_IdentityIsACopy(t *testing.T) {
	got := graphschema.Identity(graphschema.Node)
	got[0] = "changed"
	if again := graphschema.Identity(graphschema.Node); again[0] != "unique_id" {
		t.Fatalf("a caller changed the schema table: Identity(Node) = %v", again)
	}
}

func TestGraphSchema_RelTypes(t *testing.T) {
	want := []string{"CURRENT", "DEPENDS_ON", "USES_CODE", "RESOLVED_BY", "HAS_SIGNATURE", "FAILED", "PROPOSED", "HAS_PR", "EDITED"}
	got := graphschema.RelTypes()
	if len(got) != len(want) {
		t.Fatalf("RelTypes() = %v, want %v", got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("RelTypes() = %v, want %v", got, want)
		}
	}
}
