package fix

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryBefore_OrdersByRoundThenTryWithTheHandOverRowLast(t *testing.T) {
	cases := []struct {
		name          string
		aRound, aTry  int
		bRound, bTry  int
		before, after bool
	}{
		{"an earlier try of the round", 1, 1, 1, 2, true, false},
		{"an earlier round's last try", 1, 3, 2, 1, true, false},
		{"a try before its round's hand-over row", 1, 3, 1, HandOverTry, true, false},
		{"a round's hand-over row before the next round", 1, HandOverTry, 2, 1, true, false},
		{"the same try", 2, 1, 2, 1, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.before, TryBefore(c.aRound, c.aTry, c.bRound, c.bTry))
			assert.Equal(t, c.after, TryBefore(c.bRound, c.bTry, c.aRound, c.aTry))
		})
	}
}
