package prompt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pythonEvidence builds a fully-populated PythonEvidence: every evidence
// section a python contract fix can carry is present, so a renderer that drops
// one is caught rather than passing on the sections it happens to render.
func pythonEvidence() PythonEvidence {
	return PythonEvidence{
		NodeID:        "analytics.py_daily_kpis",
		ErrorExcerpt:  "column revenue_total is declared but the script produced revenue",
		RunnerLog:     "continuo-runtime: validating analytics.py_daily_kpis\nFAILED",
		ContractEntry: `{"output_columns":[{"name":"revenue_total"}],"reads":{"orders":"select * from analytics.orders"}}`,
		YAMLPath:      "services/service-py/contracts/py_daily_kpis.yml",
		YAMLText:      "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n",
		UpstreamChanges: []UpstreamChange{
			{NodeID: "analytics.orders", Depth: 1, CodeDiff: "-  revenue\n+  revenue_amount", ConfigDiff: "+materialized: table"},
		},
		Precedents: []Precedent{
			{NodeID: "analytics.other", Category: "validation", Reason: "missing_column",
				ErrorExcerpt: "column missing", RejectedAt: "2026-08-01T00:00:00Z",
				Resolved: true, ResolutionDiff: "-old\n+new", PRURL: "https://example.test/pr/1"},
		},
		PriorAttempts: []PriorAttempt{
			{
				Attempt:     1,
				VerifyError: "shadow rejected: revenue_total still missing",
				Diffs: []AttemptDiff{
					{Path: "services/service-py/contracts/py_daily_kpis.yml", Diff: "-    - name: revenue\n+    - name: revenue_total"},
				},
			},
		},
	}
}

// TestAssemblePythonContractFix_RendersEverySection pins that every evidence
// section reaches the model, each in the fence its content type calls for. The
// per-attempt evidence (the prior attempt's diff and the shadow release's
// error) is what makes a second attempt better-informed than the first, so a
// renderer that silently drops a section would degrade retries without any
// visible failure.
func TestAssemblePythonContractFix_RendersEverySection(t *testing.T) {
	req := AssemblePythonContractFix(pythonEvidence())
	u := req.User

	require.Contains(t, u, "analytics.py_daily_kpis")
	require.Contains(t, u, "column revenue_total is declared but the script produced revenue")
	require.Contains(t, u, "continuo-runtime: validating analytics.py_daily_kpis")

	// The contract entry is canonical JSON and the declaring file is yaml; each
	// is fenced as what it is so the model does not have to guess.
	require.Contains(t, u, "```json\n"+`{"output_columns":[{"name":"revenue_total"}],"reads":{"orders":"select * from analytics.orders"}}`)
	require.Contains(t, u, "```yaml\nnodes:\n  - schema: analytics\n    table: py_daily_kpis\n")
	require.Contains(t, u, "services/service-py/contracts/py_daily_kpis.yml")

	// Upstream changes and precedent.
	require.Contains(t, u, "analytics.orders")
	require.Contains(t, u, "+  revenue_amount")
	require.Contains(t, u, "+materialized: table")
	require.Contains(t, u, "How similar failures were fixed before")
	require.Contains(t, u, "https://example.test/pr/1")

	// Prior attempts: the shadow release's error and the diff that attempt made.
	require.Contains(t, u, "shadow rejected: revenue_total still missing")
	require.Contains(t, u, "+    - name: revenue_total")
}

// TestAssemblePythonContractFix_SectionsOmittedWhenAbsent verifies that an
// evidence value with nothing to say renders no heading at all, so a first
// attempt's prompt is not padded with empty scaffolding the model has to read
// past.
func TestAssemblePythonContractFix_SectionsOmittedWhenAbsent(t *testing.T) {
	req := AssemblePythonContractFix(PythonEvidence{
		NodeID:       "analytics.py_daily_kpis",
		ErrorExcerpt: "boom",
		YAMLPath:     "contracts/py.yml",
		YAMLText:     "nodes: []\n",
	})
	u := req.User

	require.NotContains(t, u, "Previous fix attempts")
	require.NotContains(t, u, "How similar failures were fixed before")
	require.NotContains(t, u, "Recent upstream changes")
	require.NotContains(t, u, "```json")
	require.NotContains(t, u, "Full runner log")
}

// TestAssemblePythonContractFix_ToolSchema pins the forced tool: the fix is
// returned as a list of complete files, so a model that can only name one file
// cannot express a multi-node contract change.
func TestAssemblePythonContractFix_ToolSchema(t *testing.T) {
	req := AssemblePythonContractFix(pythonEvidence())

	require.Equal(t, "propose_python_fix", req.ToolName)

	byName := map[string]ToolParam{}
	for _, p := range req.ToolParams {
		byName[p.Name] = p
	}
	files, ok := byName["updated_files"]
	require.True(t, ok, "the tool must expose updated_files")
	require.True(t, files.Required, "updated_files must be required")
	require.Equal(t, "array", files.Type)

	itemNames := make([]string, 0, len(files.Items))
	for _, it := range files.Items {
		itemNames = append(itemNames, it.Name)
	}
	require.ElementsMatch(t, []string{"path", "content"}, itemNames,
		"each element must carry the file's path and its complete new content")

	require.True(t, byName["rationale"].Required)
	require.True(t, byName["confidence"].Required)
}

// TestAssemblePythonContractFix_SystemPromptScopesTheEdit pins the three rules
// that keep a contract fix from breaking the release it is meant to repair:
// only validation-relevant fields change, unrelated nodes in a shared file are
// left alone, and every returned file is complete rather than a fragment.
func TestAssemblePythonContractFix_SystemPromptScopesTheEdit(t *testing.T) {
	sys := AssemblePythonContractFix(pythonEvidence()).System
	lower := strings.ToLower(sys)

	require.Contains(t, lower, "reads")
	require.Contains(t, lower, "output_columns")
	require.Contains(t, lower, "config")
	require.Contains(t, lower, "unchanged", "the prompt must tell the model to leave other nodes untouched")
	require.Contains(t, lower, "complete", "the prompt must demand each file's full new content")
}
