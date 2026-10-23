package event

import "testing"

func TestEventTypesAreDistinctAndComplete(t *testing.T) {
	all := []string{
		EventTypeTaskStatusReport,
	}
	seen := map[string]bool{}
	for _, v := range all {
		if v == "" || seen[v] {
			t.Fatalf("event type %q is empty or duplicated", v)
		}
		seen[v] = true
	}
}
