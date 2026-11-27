package fix

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailedCheckReason(t *testing.T) {
	cases := []struct {
		name   string
		checks []ReleaseCheck
		want   string
	}{
		{
			name: "the failed run's own error is the reason",
			checks: []ReleaseCheck{
				{CheckID: "verify-a1", Phase: PhasePassed},
				{CheckID: "verify-b1", Phase: PhaseFailed, Error: "duplicate_table — rename the model"},
			},
			want: "release check verify-b1 failed: duplicate_table — rename the model",
		},
		{
			name:   "a failed run that reported no error says so",
			checks: []ReleaseCheck{{CheckID: "verify-a1", Phase: PhaseFailed}},
			want:   "release check verify-a1 failed without a per-node error",
		},
		{
			name:   "with no failed run the first run is named",
			checks: []ReleaseCheck{{CheckID: "verify-a1"}, {CheckID: "verify-b1"}},
			want:   "release check verify-a1 failed without a per-node error",
		},
		{
			name: "no run at all still gives a reason",
			want: "the release check failed without a per-node error",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, FailedCheckReason(tc.checks))
		})
	}
}
