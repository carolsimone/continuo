package proposal

import (
	"reflect"
	"testing"
)

func TestGroupEditsByService_SplitsByOwningPrefix(t *testing.T) {
	prefixes := map[string]string{"core": "services/core", "finance": "services/finance"}
	edits := []FileEdit{
		{Path: "services/core/models/users.sql"},
		{Path: "services/finance/models/fix_orders.sql"},
		{Path: "services/core/models/kpis.sql"},
	}
	got := GroupEditsByService(prefixes, edits)
	if len(got) != 2 || len(got["core"]) != 2 || len(got["finance"]) != 1 {
		t.Fatalf("got %#v", got)
	}
}

func TestGroupEditsByService_UnmappedPathFallsToLegacyKey(t *testing.T) {
	got := GroupEditsByService(map[string]string{"core": "services/core"}, []FileEdit{{Path: "elsewhere/x.sql"}})
	if len(got[""]) != 1 {
		t.Fatalf("expected legacy '' group, got %#v", got)
	}
}

func TestGroupEditsByService_EmptyMapKeepsSingleGroup(t *testing.T) {
	got := GroupEditsByService(nil, []FileEdit{{Path: "a.sql"}, {Path: "b.sql"}})
	if len(got) != 1 || len(got[""]) != 2 {
		t.Fatalf("expected one '' group of 2, got %#v", got)
	}
}

func TestMembersOfEdits_UnionSortedAndFallback(t *testing.T) {
	edits := []FileEdit{
		{MemberNodeIDs: []string{"m.b", "m.a"}},
		{MemberNodeIDs: []string{"m.b", "m.c"}},
	}
	if got := MembersOfEdits(edits, nil); !reflect.DeepEqual(got, []string{"m.a", "m.b", "m.c"}) {
		t.Fatalf("got %v", got)
	}
	if got := MembersOfEdits([]FileEdit{{}}, []string{"all.x"}); !reflect.DeepEqual(got, []string{"all.x"}) {
		t.Fatalf("fallback not applied: %v", got)
	}
}
