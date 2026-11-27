package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

func TestLaterTry_OrdersByRoundThenTryWithTheHandOverLast(t *testing.T) {
	try := func(round, number int) ports.FixSummary {
		return ports.FixSummary{NodeID: "finance", Round: round, Try: number}
	}
	cases := map[string]struct {
		a, b ports.FixSummary
		want bool
	}{
		"a later try of the same round":                 {try(1, 2), try(1, 1), true},
		"an earlier try of the same round":              {try(1, 1), try(1, 2), false},
		"the same try":                                  {try(1, 1), try(1, 1), false},
		"round 2 try 1 after round 1 try 3":             {try(2, 1), try(1, 3), true},
		"round 1 try 3 before round 2 try 1":            {try(1, 3), try(2, 1), false},
		"the round's hand-over after its last try":      {try(1, 0), try(1, 3), true},
		"a try before its round's hand-over":            {try(1, 3), try(1, 0), false},
		"round 2 try 1 after round 1's hand-over":       {try(2, 1), try(1, 0), true},
		"round 1's hand-over before round 2 try 1":      {try(1, 0), try(2, 1), false},
		"a try that names no round reads as round 1":    {try(0, 2), try(1, 1), true},
		"round 2 try 1 after a try that names no round": {try(2, 1), try(0, 3), true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, laterTry(tc.a, tc.b))
		})
	}
}
