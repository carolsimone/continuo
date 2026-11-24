package proposal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailedRunReason(t *testing.T) {
	cases := []struct {
		name string
		runs []Verification
		want string
	}{
		{
			name: "the failed run's own error is the reason",
			runs: []Verification{
				{RunID: "verify-a1", Phase: PhasePassed},
				{RunID: "verify-b1", Phase: PhaseFailed, Error: "duplicate_table — rename the model"},
			},
			want: "verification run verify-b1 failed: duplicate_table — rename the model",
		},
		{
			name: "a failed run that reported no error says so",
			runs: []Verification{{RunID: "verify-a1", Phase: PhaseFailed}},
			want: "verification run verify-a1 failed without a per-node error",
		},
		{
			name: "with no failed run the first run is named",
			runs: []Verification{{RunID: "verify-a1"}, {RunID: "verify-b1"}},
			want: "verification run verify-a1 failed without a per-node error",
		},
		{
			name: "no run at all still gives a reason",
			want: "verification failed without a per-node error",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, FailedRunReason(tc.runs))
		})
	}
}
