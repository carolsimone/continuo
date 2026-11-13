package release

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

func TestSplitValidationFailures(t *testing.T) {
	results := []NodeOutcome{
		{NodeID: "analytics.b", Status: "failed"},
		{NodeID: "analytics.s", Status: "failed"},
		{NodeID: "analytics.c", Status: "skipped"},
		{NodeID: "analytics.d", Status: "ok"},
	}
	rebuilt := map[string]bool{"analytics.s": true, "analytics.c": true, "analytics.d": true}

	built, cloned := splitValidationFailures(results, rebuilt)

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

	built, cloned := splitValidationFailures(results, map[string]bool{"a": true, "b": true})

	assert.Empty(t, built)
	assert.Empty(t, cloned)
}

func TestSplitValidationFailures_NoResults(t *testing.T) {
	built, cloned := splitValidationFailures(nil, nil)

	assert.Empty(t, built)
	assert.Empty(t, cloned)
}

// Every status other than ok and skipped is a failure of the node itself.
func TestSplitValidationFailures_AnyOtherStatusIsAFailure(t *testing.T) {
	results := []NodeOutcome{
		{NodeID: "a", Status: "error"},
		{NodeID: "b", Status: ""},
	}

	built, cloned := splitValidationFailures(results, map[string]bool{"a": true})

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
			built, cloned := splitValidationFailures(results, rebuilt)

			assert.Equal(t, []string{"m.x", "m.y"}, built)
			assert.Equal(t, []string{"c.a", "c.b"}, cloned)
		})
	}
}

// DecideValidationRejection names a failed copy of production apart from a
// failure of the release's code: only copies failed is upstream_clone_failed,
// a code failure keeps validation_failed and drops the copies from the
// failing nodes.
func TestDecideValidationRejection(t *testing.T) {
	const summary = "2 of 4 nodes failed validation: m.s and m.t"
	// m.* nodes are built from their own code, c.* nodes copied from production.
	rebuilt := map[string]bool{"m.s": true, "m.t": true, "m.u": true, "m.v": true}
	cases := []struct {
		name    string
		results []NodeOutcome
		failing []string
		want    ValidationRejection
	}{
		{
			name: "only copies failed",
			results: []NodeOutcome{
				{NodeID: "c.b", Status: "failed"},
				{NodeID: "c.a", Status: "failed"},
				{NodeID: "m.s", Status: NodeStatusSkipped},
				{NodeID: "m.u", Status: NodeStatusOK},
			},
			failing: []string{"c.b", "c.a", "m.s"},
			want: ValidationRejection{
				Reason:       pkg_model.RejectReasonUpstreamCloneFailed,
				Detail:       "Could not copy the production table of c.a, c.b. Run them, then release again.",
				FailingNodes: []string{"c.a", "c.b"},
			},
		},
		{
			name: "only built nodes failed",
			results: []NodeOutcome{
				{NodeID: "m.t", Status: "failed"},
				{NodeID: "m.s", Status: "failed"},
				{NodeID: "c.a", Status: NodeStatusOK},
			},
			failing: []string{"m.t", "m.s"},
			want: ValidationRejection{
				Reason:       pkg_model.RejectReasonValidationFailed,
				Detail:       summary,
				FailingNodes: []string{"m.s", "m.t"},
			},
		},
		{
			name: "copies and built nodes failed, with skipped dependants",
			results: []NodeOutcome{
				{NodeID: "c.a", Status: "failed"},
				{NodeID: "m.v", Status: NodeStatusSkipped},
				{NodeID: "m.s", Status: "failed"},
				{NodeID: "m.t", Status: NodeStatusSkipped},
				{NodeID: "m.u", Status: NodeStatusOK},
			},
			failing: []string{"m.v", "c.a", "m.t", "m.s"},
			want: ValidationRejection{
				Reason:       pkg_model.RejectReasonValidationFailed,
				Detail:       summary + ". Could not copy the production table of c.a. Run it, then release again.",
				FailingNodes: []string{"m.s", "m.t", "m.v"},
			},
		},
		{
			name: "only skipped and passed nodes",
			results: []NodeOutcome{
				{NodeID: "c.a", Status: NodeStatusOK},
				{NodeID: "m.s", Status: NodeStatusSkipped},
			},
			failing: []string{"m.s"},
			want: ValidationRejection{
				Reason:       pkg_model.RejectReasonValidationFailed,
				Detail:       summary,
				FailingNodes: []string{"m.s"},
			},
		},
		{
			name:    "no stored result",
			results: nil,
			failing: nil,
			want: ValidationRejection{
				Reason: pkg_model.RejectReasonValidationFailed,
				Detail: summary,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DecideValidationRejection(tc.results, rebuilt, tc.failing, summary))
		})
	}
}

// The failing nodes and the copies the detail names come out in id order,
// whatever order the results and the failing nodes arrive in.
func TestDecideValidationRejection_SortsWhateverTheInputOrder(t *testing.T) {
	rebuilt := map[string]bool{"m.x": true, "m.y": true}
	results := []NodeOutcome{
		{NodeID: "c.b", Status: "failed"},
		{NodeID: "m.y", Status: "failed"},
		{NodeID: "c.a", Status: "failed"},
		{NodeID: "m.x", Status: "failed"},
	}
	failing := []string{"m.y", "c.b", "m.x", "c.a"}
	reversed := func(ids []string) []string {
		out := slices.Clone(ids)
		slices.Reverse(out)
		return out
	}

	forward := DecideValidationRejection(results, rebuilt, failing, "summary")
	backward := DecideValidationRejection(
		[]NodeOutcome{results[3], results[2], results[1], results[0]}, rebuilt, reversed(failing), "summary")

	want := ValidationRejection{
		Reason:       pkg_model.RejectReasonValidationFailed,
		Detail:       "summary. Could not copy the production table of c.a, c.b. Run them, then release again.",
		FailingNodes: []string{"m.x", "m.y"},
	}
	assert.Equal(t, want, forward)
	assert.Equal(t, want, backward)
	assert.Equal(t, []string{"m.y", "c.b", "m.x", "c.a"}, failing, "the caller's slice is left as it was")
}
