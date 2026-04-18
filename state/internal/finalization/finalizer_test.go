package finalization_test

import (
	"testing"

	"github.com/carolsimone/continuo/state/internal/finalization"
	"github.com/stretchr/testify/assert"
)

func TestFinalizer_Decide(t *testing.T) {
	tt := []struct {
		name          string
		terminalCount int32
		totalCount    int32
		anyFailed     bool
		initCompleted bool
		schedStatus   string
		wantOutcome   string
	}{
		{"not all done", 3, 5, false, true, "running", ""},
		{"all succeeded", 5, 5, false, true, "running", "succeeded"},
		{"any failed", 5, 5, true, true, "running", "failed"},
		{"init not complete", 5, 5, false, false, "running", ""},
		{"already terminal", 5, 5, false, true, "succeeded", ""},
		{"already failed terminal", 5, 5, true, true, "failed", ""},
		{"zero total count", 0, 0, false, true, "running", ""},
		{"terminal exceeds total but zero total", 1, 0, false, true, "running", ""},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := finalization.Decide(tc.terminalCount, tc.totalCount, tc.anyFailed, tc.initCompleted, tc.schedStatus)
			assert.Equal(t, tc.wantOutcome, got)
		})
	}
}
