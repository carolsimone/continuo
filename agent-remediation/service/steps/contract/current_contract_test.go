package contract_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

// fakeContracts answers the current contract of each node, "" for a node it
// does not hold, and records the nodes it was asked for.
type fakeContracts struct {
	byNode map[string]string
	err    error
	asked  []string
}

func (f *fakeContracts) CurrentContract(_ context.Context, uniqueID string) (string, error) {
	f.asked = append(f.asked, uniqueID)
	return f.byNode[uniqueID], f.err
}

func gatherCurrent(t *testing.T, c *fakeContracts) (step.ContextOutput, error) {
	t.Helper()
	return contract.NewCurrentContract(c, nil).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})
}

func TestCurrentContractStep_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := contract.NewCurrentContract(nil, nil).Meta()
	assert.Equal(t, "current_contract", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyCurrentContract}, m.Provides)
}

func TestCurrentContractStep_ProvidesTheContract(t *testing.T) {
	c := &fakeContracts{byNode: map[string]string{nodeID: "id: fx.rates\nversion: \"3\"\n"}}

	out, err := gatherCurrent(t, c)

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{
		remediation.KeyCurrentContract: remediation.Text("id: fx.rates\nversion: \"3\"\n"),
	}, out.Evidence)
	assert.Equal(t, []string{nodeID}, c.asked)
}

func TestCurrentContractStep_EmptyWithoutOne(t *testing.T) {
	out, err := gatherCurrent(t, &fakeContracts{})

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{
		remediation.KeyCurrentContract: remediation.Text(""),
	}, out.Evidence)
}

// The current contract is reference for the model, never needed to fix the
// node: a catalog that fails to answer (an outage, or a catalog that serves no
// GetNodeContract yet during a rolling upgrade) is logged as a warning and
// the step provides an empty text, so the remediation goes on.
func TestCurrentContractStep_AFailedReadIsAWarningAndAnEmptyText(t *testing.T) {
	var logs bytes.Buffer
	c := &fakeContracts{err: errBoom}

	out, err := contract.NewCurrentContract(c, slog.New(slog.NewTextHandler(&logs, nil))).Gather(
		context.Background(), step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{
		remediation.KeyCurrentContract: remediation.Text(""),
	}, out.Evidence)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), nodeID)
	assert.Contains(t, logs.String(), errBoom.Error())
}
