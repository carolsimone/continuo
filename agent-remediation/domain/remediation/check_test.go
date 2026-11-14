package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

func TestCheckResult_PassedNeedsNoFindingsAndNoWait(t *testing.T) {
	assert.True(t, CheckResult{Check: "c"}.Passed())
	assert.False(t, CheckResult{Check: "c", Waiting: true}.Passed())
	assert.False(t, CheckResult{Check: "c", Findings: []Finding{{Code: "x"}}}.Passed())
}

func TestNewChecked_RefusesAFailedCheck(t *testing.T) {
	_, err := NewChecked(Draft{}, []CheckResult{{Check: "content_changed", Findings: []Finding{{Code: "content_unchanged"}}}})
	require.Error(t, err)
}

func TestNewChecked_RefusesAWaitingCheck(t *testing.T) {
	_, err := NewChecked(Draft{}, []CheckResult{{Check: "verification_run", Waiting: true}})
	require.Error(t, err)
}

func TestNewChecked_KeepsTheDraft(t *testing.T) {
	d := Draft{NamedTarget: "model.a", Edits: []Edit{{Path: "a.sql"}}}
	c, err := NewChecked(d, []CheckResult{{Check: "content_changed"}})
	require.NoError(t, err)
	assert.False(t, c.IsZero())
	assert.Equal(t, d, c.Draft())
	assert.True(t, Checked{}.IsZero())
}

func TestNewVerified_RefusesAZeroChecked(t *testing.T) {
	_, err := NewVerified("p1", []Checked{{}}, []CheckResult{{Check: "verification_run"}}, nil)
	require.Error(t, err)
}

func TestNewVerified_RefusesAWaitingGate(t *testing.T) {
	c, err := NewChecked(Draft{}, nil)
	require.NoError(t, err)
	_, err = NewVerified("p1", []Checked{c}, []CheckResult{{Check: "verification_run", Waiting: true}}, nil)
	require.Error(t, err)
}

func TestNewVerified_RefusesAFailedGate(t *testing.T) {
	c, err := NewChecked(Draft{}, nil)
	require.NoError(t, err)
	_, err = NewVerified("p1", []Checked{c}, []CheckResult{{Check: "verification_run", Findings: []Finding{{Code: "run_failed"}}}}, nil)
	require.Error(t, err)
}

func TestNewVerified_RefusesAnEmptyIDAndNoChecked(t *testing.T) {
	c, err := NewChecked(Draft{}, nil)
	require.NoError(t, err)
	_, err = NewVerified("", []Checked{c}, nil, nil)
	require.Error(t, err)
	_, err = NewVerified("p1", nil, nil, nil)
	require.Error(t, err)
}

func TestNewVerified_KeepsWhatItWasGiven(t *testing.T) {
	c, err := NewChecked(Draft{NamedTarget: "model.a"}, nil)
	require.NoError(t, err)
	vs := []proposal.Verification{{Service: "sales", RunID: "r1"}}
	v, err := NewVerified("p1", []Checked{c}, []CheckResult{{Check: "verification_run"}}, vs)
	require.NoError(t, err)
	assert.False(t, v.IsZero())
	assert.Equal(t, "p1", v.ProposalID())
	assert.Equal(t, vs, v.Verifications())
	assert.True(t, Verified{}.IsZero())
}
