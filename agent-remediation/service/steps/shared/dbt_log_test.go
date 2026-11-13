package shared_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

func failureWithLog(uri string) remediation.Failure {
	return remediation.Failure{Target: remediation.TriggerNode{NodeID: "model.a", DBTLogURI: uri}}
}

func TestDBTLog_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := shared.NewDBTLog(nil, nil, nil).Meta()
	assert.Equal(t, "dbt_log", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyDBTLog}, m.Provides)
}

func TestDBTLog_ProvidesTheSanitizedLog(t *testing.T) {
	ev := &fakeEvidence{body: "boom"}
	out, err := shared.NewDBTLog(ev, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("s3://b/log")})
	require.NoError(t, err)
	assert.Equal(t, []string{"s3://b/log"}, ev.calls)
	assert.Equal(t, map[remediation.EvidenceKey]any{remediation.KeyDBTLog: remediation.Text("<boom>")}, out.Evidence)
	assert.Empty(t, out.Decline)
}

func TestDBTLog_EmptyURIProvidesEmptyTextWithoutReading(t *testing.T) {
	ev := &fakeEvidence{body: "boom"}
	out, err := shared.NewDBTLog(ev, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("")})
	require.NoError(t, err)
	assert.Empty(t, ev.calls)
	assert.Equal(t, remediation.Text(""), out.Evidence[remediation.KeyDBTLog])
}

func TestDBTLog_NotFoundProvidesEmptyText(t *testing.T) {
	ev := &fakeEvidence{err: ports.ErrNotFound}
	out, err := shared.NewDBTLog(ev, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("s3://b/gone")})
	require.NoError(t, err)
	assert.Equal(t, remediation.Text(""), out.Evidence[remediation.KeyDBTLog])
}

func TestDBTLog_ATransientErrorIsReturned(t *testing.T) {
	boom := errors.New("s3 down")
	ev := &fakeEvidence{err: boom}
	_, err := shared.NewDBTLog(ev, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failureWithLog("s3://b/log")})
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "fetch dbt log")
}
