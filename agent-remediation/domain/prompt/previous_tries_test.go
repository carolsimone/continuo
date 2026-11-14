package prompt

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func previousTriesBase() ProposeRequest {
	req := reviseBaseRequest()
	req.User = "Failed node: analytics.table_e\n\nPropose a corrected version of the failed model's SQL."
	return req
}

func TestPreviousTriesSection_NoTriesAddsNothing(t *testing.T) {
	base := previousTriesBase()
	assert.Equal(t, base, WithPreviousTries(base, nil))
	assert.Equal(t, base, WithPreviousTries(base, []PreviousTry{}))
}

func TestPreviousTriesSection_RendersEachTry(t *testing.T) {
	base := previousTriesBase()
	got := WithPreviousTries(base, []PreviousTry{
		{
			Label:     "Try 1 (round 1)",
			Diffs:     []AttemptDiff{{Path: "models/e.sql", Diff: "-select a\n+select b\n"}},
			Findings:  []string{"round 1: content_unchanged: the answer repeats the original"},
			GateError: "analytics.table_e: column b does not exist",
		},
		{Label: "Attempt 2 (round 1)"},
	})

	want := base.User + "\n\n## Earlier tries at this failure\n" +
		"These tries at the same failure did not pass. Do not repeat a change that did not pass; correct what each try was refused for.\n" +
		"\n### Try 1 (round 1)\n" +
		"Changed models/e.sql:\n```diff\n-select a\n+select b\n```\n" +
		"What pre-flight found:\n- round 1: content_unchanged: the answer repeats the original\n" +
		"Why the gate failed: analytics.table_e: column b does not exist\n" +
		"\n### Attempt 2 (round 1)\n" +
		"Nothing was recorded for this try."
	assert.Equal(t, want, got.User)
	got.User = base.User
	assert.Equal(t, base, got, "only the user message changes")
}

func TestPreviousTriesSection_KeepsTheLastThreeTriesAndCapsEachDiff(t *testing.T) {
	long := strings.Repeat("x", 8192) + "tail-that-is-cut"
	tries := make([]PreviousTry, 0, 4)
	for i := 1; i <= 4; i++ {
		tries = append(tries, PreviousTry{
			Label: fmt.Sprintf("Try %d (round 1)", i),
			Diffs: []AttemptDiff{{Path: "models/e.sql", Diff: long}},
		})
	}

	user := WithPreviousTries(previousTriesBase(), tries).User

	assert.NotContains(t, user, "### Try 1 (round 1)", "the oldest try beyond the last three is left out")
	for i := 2; i <= 4; i++ {
		assert.Contains(t, user, fmt.Sprintf("### Try %d (round 1)", i))
	}
	assert.Less(t, strings.Index(user, "### Try 2"), strings.Index(user, "### Try 4"), "the tries stay oldest first")
	assert.NotContains(t, user, "tail-that-is-cut", "each diff is cut at 8192 bytes")
	assert.Equal(t, 3, strings.Count(user, strings.Repeat("x", 8192)+"\n… (diff truncated)"), "each cut diff carries the truncation marker")
}

func TestTruncateDiff_CutsAtARuneBoundaryAndMarksTheCut(t *testing.T) {
	assert.Equal(t, "short", TruncateDiff("short", 100), "a diff under the cap is left whole")
	assert.Equal(t, "ab\n… (diff truncated)", TruncateDiff("ab€cd", 4), "the cut backs up to the start of the multibyte rune")
}
