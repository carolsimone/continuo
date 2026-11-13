package release

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitValidationFailures(t *testing.T) {
	results := []NodeOutcome{
		{NodeID: "analytics.b", Status: "failed"},
		{NodeID: "analytics.s", Status: "failed"},
		{NodeID: "analytics.c", Status: "skipped"},
		{NodeID: "analytics.d", Status: "ok"},
	}
	rebuilt := map[string]bool{"analytics.s": true, "analytics.c": true, "analytics.d": true}

	built, cloned := SplitValidationFailures(results, rebuilt)

	assert.Equal(t, []string{"analytics.s"}, built, "a failed node built from its own code")
	assert.Equal(t, []string{"analytics.b"}, cloned, "a failed node cloned from its production table")
}

// A skipped node never ran, so it is neither a code failure nor a copy that
// failed, wherever it would have been built from.
func TestSplitValidationFailures_SkippedAndOKNodesAreInNeitherList(t *testing.T) {
	results := []NodeOutcome{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "skipped"},
		{NodeID: "c", Status: "ok"},
		{NodeID: "d", Status: "skipped"},
	}

	built, cloned := SplitValidationFailures(results, map[string]bool{"a": true, "b": true})

	assert.Empty(t, built)
	assert.Empty(t, cloned)
}

func TestSplitValidationFailures_NoResults(t *testing.T) {
	built, cloned := SplitValidationFailures(nil, nil)

	assert.Empty(t, built)
	assert.Empty(t, cloned)
}

// Every status other than ok and skipped is a failure of the node itself.
func TestSplitValidationFailures_AnyOtherStatusIsAFailure(t *testing.T) {
	results := []NodeOutcome{
		{NodeID: "a", Status: "error"},
		{NodeID: "b", Status: ""},
	}

	built, cloned := SplitValidationFailures(results, map[string]bool{"a": true})

	assert.Equal(t, []string{"a"}, built)
	assert.Equal(t, []string{"b"}, cloned)
}

// Both lists come out sorted by id whatever order the results arrive in.
func TestSplitValidationFailures_SortsBothListsWhateverTheInputOrder(t *testing.T) {
	rebuilt := map[string]bool{"m.y": true, "m.x": true}
	forward := []NodeOutcome{
		{NodeID: "c.b", Status: "failed"},
		{NodeID: "m.y", Status: "failed"},
		{NodeID: "c.a", Status: "failed"},
		{NodeID: "m.x", Status: "failed"},
	}
	backward := make([]NodeOutcome, len(forward))
	for i, r := range forward {
		backward[len(forward)-1-i] = r
	}

	for name, results := range map[string][]NodeOutcome{"forward": forward, "backward": backward} {
		t.Run(name, func(t *testing.T) {
			built, cloned := SplitValidationFailures(results, rebuilt)

			assert.Equal(t, []string{"m.x", "m.y"}, built)
			assert.Equal(t, []string{"c.a", "c.b"}, cloned)
		})
	}
}
