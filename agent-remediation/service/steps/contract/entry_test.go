package contract_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

func gatherEntry(t *testing.T, b *fakeBundle, f remediation.Failure) (step.ContextOutput, error) {
	t.Helper()
	return contract.NewContractEntry(b, markSanitizer{}, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: f, Evidence: remediation.NewEvidence()})
}

func entryOf(t *testing.T, out step.ContextOutput) remediation.Text {
	t.Helper()
	v, ok := out.Evidence[remediation.KeyContractEntry].(remediation.Text)
	require.True(t, ok, "contract_entry must hold a remediation.Text")
	return v
}

func TestContractEntry_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := contract.NewContractEntry(nil, nil, nil).Meta()
	assert.Equal(t, "contract_entry", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyContractEntry}, m.Provides)
}

func TestContractEntry_ReadsTheBundlesNormalizedContractSanitized(t *testing.T) {
	b := &fakeBundle{nodes: map[string]ports.CandidateSource{
		nodeID: {Runtime: ports.RuntimePython, RawCode: `{"table":"rates"}`},
	}}

	out, err := gatherEntry(t, b, failure(nil))

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Equal(t, remediation.Text(`<{"table":"rates"}>`), entryOf(t, out))
	assert.Equal(t, []bundleCall{{"s3://bundle/rel-1", nodeID, "rel-1"}}, b.calls)
}

func TestContractEntry_AnEmptyBundleURIIsNeverReadAndGivesNoEntry(t *testing.T) {
	b := &fakeBundle{}

	out, err := gatherEntry(t, b, failure(func(f *remediation.Failure) { f.Trigger.CodeBundleURI = "" }))

	require.NoError(t, err)
	assert.Equal(t, remediation.Text(""), entryOf(t, out))
	assert.Empty(t, b.calls)
}

func TestContractEntry_ANonPythonRuntimeIsLeftOut(t *testing.T) {
	b := &fakeBundle{nodes: map[string]ports.CandidateSource{
		nodeID: {Runtime: ports.RuntimeDbt, RawCode: "select 1"},
	}}

	out, err := gatherEntry(t, b, failure(nil))

	require.NoError(t, err)
	assert.Equal(t, remediation.Text(""), entryOf(t, out))
}

func TestContractEntry_APermanentMissGivesNoEntry(t *testing.T) {
	out, err := gatherEntry(t, &fakeBundle{err: fmt.Errorf("bundle: %w", ports.ErrNotFound)}, failure(nil))

	require.NoError(t, err)
	assert.Equal(t, remediation.Text(""), entryOf(t, out))
}

func TestContractEntry_ReturnsATransientFetchError(t *testing.T) {
	_, err := gatherEntry(t, &fakeBundle{err: errBoom}, failure(nil))

	require.ErrorIs(t, err, errBoom)
	assert.EqualError(t, err, "fetch code bundle: boom")
}
