package proposal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

func TestRedactDataValues(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "double-quoted data value is redacted",
			in:   `invalid input syntax for type integer: "customer@example.test"`,
			want: `invalid input syntax for type integer: "?"`,
		},
		{
			name: "double-quoted column identifier is preserved",
			in:   `column "amount" does not exist`,
			want: `column "amount" does not exist`,
		},
		{
			name: "double-quoted qualified identifier is preserved",
			in:   `relation "public.wrong_name" does not exist`,
			want: `relation "public.wrong_name" does not exist`,
		},
		{
			name: "key equals value parenthetical redacts both parenthetical groups",
			in:   `Key (email)=(john@doe.com) already exists.`,
			want: `Key (?)=(?) already exists.`,
		},
		{
			name: "single-quoted data value is redacted",
			in:   `value '5000000' is out of range`,
			want: `value '?' is out of range`,
		},
		{
			name: "postgres DETAIL failing-row parenthetical is redacted",
			in:   `DETAIL:  Failing row contains (5000, 2026-01-01, 384923, john@doe.com).`,
			want: `DETAIL:  Failing row contains (?).`,
		},
		{
			name: "empty string is unchanged",
			in:   "",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, proposal.RedactDataValues(tc.in))
		})
	}
}

// TestRedactDataValues_DetailRowLeaksNoData is a belt-and-suspenders check on
// the Postgres constraint-violation DETAIL shape: the row's individual data
// values (an email, a date, numbers) must not survive redaction anywhere in
// the output, not just fail to match a single expected string.
func TestRedactDataValues_DetailRowLeaksNoData(t *testing.T) {
	got := proposal.RedactDataValues(`DETAIL:  Failing row contains (5000, 2026-01-01, 384923, john@doe.com).`)
	assert.NotContains(t, got, "john@doe.com")
	assert.NotContains(t, got, "2026-01-01")
	assert.NotContains(t, got, "5000")
	assert.NotContains(t, got, "384923")
}
