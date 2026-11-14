package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_RegistersTheDBTValidationFamily(t *testing.T) {
	r := NewRegistry(Collaborators{})
	want := map[string]step.Kind{
		"candidate_sql":          step.KindContext,
		"code_bundle_source":     step.KindContext,
		"own_change_diff":        step.KindContext,
		"changed_ancestor_diffs": step.KindContext,
		"dbt-validation":         step.KindPrompt,
	}
	for name, kind := range want {
		_, m, ok := r.Lookup(workflow.StepName(name))
		require.True(t, ok, name)
		assert.Equal(t, kind, m.Kind, name)
	}
}

func TestRegistry_TypesTheDBTValidationEvidence(t *testing.T) {
	r := NewRegistry(Collaborators{})
	want := map[remediation.EvidenceKey]any{
		remediation.KeyCandidateSQL:         remediation.Text(""),
		remediation.KeyCodeBundleSource:     remediation.CandidateSource{},
		remediation.KeyOwnChangeDiff:        remediation.Text(""),
		remediation.KeyChangedAncestorDiffs: []prompt.ReleaseUpstreamChange{},
	}
	for key, zero := range want {
		f, ok := r.EvidenceType(key)
		require.True(t, ok, key)
		assert.IsType(t, zero, f(), key)
	}
}
