package shared_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

func TestUpstreamChanges_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := shared.NewUpstreamChanges(nil, nil, nil).Meta()
	assert.Equal(t, "upstream_changes", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyUpstreamChanges}, m.Provides)
}

func TestUpstreamChanges_SanitizesEachDiffWithoutTouchingTheReadersSlice(t *testing.T) {
	reader := &fakeUpstream{changes: []prompt.UpstreamChange{
		{NodeID: "model.up", Depth: 1, CodeDiff: "code", ConfigDiff: "cfg", Truncated: true},
	}}
	out, err := shared.NewUpstreamChanges(reader, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("")})
	require.NoError(t, err)
	assert.Equal(t, []string{"model.a"}, reader.nodes)
	assert.Equal(t, []prompt.UpstreamChange{
		{NodeID: "model.up", Depth: 1, CodeDiff: "<code>", ConfigDiff: "<cfg>", Truncated: true},
	}, out.Evidence[remediation.KeyUpstreamChanges])
	assert.Equal(t, "code", reader.changes[0].CodeDiff)
}

func TestUpstreamChanges_AnErrorProvidesNoChanges(t *testing.T) {
	reader := &fakeUpstream{err: errors.New("catalog down")}
	out, err := shared.NewUpstreamChanges(reader, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("")})
	require.NoError(t, err)
	assert.Empty(t, out.Decline)
	got, ok := out.Evidence[remediation.KeyUpstreamChanges].([]prompt.UpstreamChange)
	assert.True(t, ok)
	assert.Empty(t, got)
}
