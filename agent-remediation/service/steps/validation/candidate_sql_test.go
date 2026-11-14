package validation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
)

func TestCandidateSQL_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := validation.NewCandidateSQL(nil, nil).Meta()
	assert.Equal(t, "candidate_sql", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyCandidateSQL}, m.Provides)
}

func TestCandidateSQL_ProvidesTheArtifactRaw(t *testing.T) {
	ev := &fakeEvidence{bodies: map[string]string{"s3://candidate/a.sql": "select  1 -- raw"}}

	out := gather(t, validation.NewCandidateSQL(ev, discardLogger()), failure(nil), remediation.NewEvidence())

	assert.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{remediation.KeyCandidateSQL: remediation.Text("select  1 -- raw")}, out.Evidence)
	assert.Equal(t, []string{"s3://candidate/a.sql"}, ev.calls)
}

func TestCandidateSQL_DeclinesWithNoMessage(t *testing.T) {
	cases := []struct {
		name      string
		mod       func(*remediation.Failure)
		evidence  *fakeEvidence
		wantReads int
	}{
		{"a python node", func(f *remediation.Failure) { f.Target.NodeType = "python-node" }, &fakeEvidence{}, 0},
		{"a python-csv node", func(f *remediation.Failure) { f.Target.NodeType = "python-csv" }, &fakeEvidence{}, 0},
		{"no candidate artifact", func(f *remediation.Failure) { f.Target.CandidateArtifactURI = "" }, &fakeEvidence{}, 0},
		{"an unreadable artifact", nil, &fakeEvidence{bodies: map[string]string{}}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gather(t, validation.NewCandidateSQL(c.evidence, discardLogger()), failure(c.mod), remediation.NewEvidence())

			assert.True(t, out.Declined)
			assert.Empty(t, out.Decline)
			assert.Empty(t, out.Evidence)
			assert.Len(t, c.evidence.calls, c.wantReads)
		})
	}
}

func TestCandidateSQL_ATransientErrorIsReturned(t *testing.T) {
	ev := &fakeEvidence{err: errBoom}

	_, err := validation.NewCandidateSQL(ev, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failure(nil)})

	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "fetch candidate sql")
}
