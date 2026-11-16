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
		YAMLPath:      "services/service-py/contracts/py_daily_kpis.odcs.yaml",
		YAMLText:      "apiVersion: v3.2.0\nkind: DataContract\nid: analytics.py_daily_kpis\n",
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
				VerifyError: "verification failed: revenue_total still missing",
				Diffs: []AttemptDiff{
					{Path: "services/service-py/contracts/py_daily_kpis.odcs.yaml", Diff: "-    - name: revenue\n+    - name: revenue_total"},
				},
			},
		},
	}
}

// TestAssemblePythonContractFix_RendersEverySection pins that every evidence
// section reaches the model, each in the fence its content type calls for. The
// per-attempt evidence (the prior attempt's diff and the verification run's
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
	require.Contains(t, u, "```yaml\napiVersion: v3.2.0\nkind: DataContract\nid: analytics.py_daily_kpis\n")
	require.Contains(t, u, "services/service-py/contracts/py_daily_kpis.odcs.yaml")

	// Upstream changes and precedent.
	require.Contains(t, u, "analytics.orders")
	require.Contains(t, u, "+  revenue_amount")
	require.Contains(t, u, "+materialized: table")
	require.Contains(t, u, "How similar failures were fixed before")
	require.Contains(t, u, "https://example.test/pr/1")

	// Prior attempts: the verification run's error and the diff that attempt made.
	require.Contains(t, u, "verification failed: revenue_total still missing")
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
		YAMLPath:     "contracts/py_daily_kpis.odcs.yaml",
		YAMLText:     "id: analytics.py_daily_kpis\n",
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

// TestAssemblePythonContractFix_SystemPromptScopesTheEdit pins the rules that
// keep a contract fix from breaking the release it is meant to repair: only
// validation-relevant fields change, the contracts of other nodes are left
// alone, and every returned file is complete rather than a fragment.
func TestAssemblePythonContractFix_SystemPromptScopesTheEdit(t *testing.T) {
	sys := AssemblePythonContractFix(pythonEvidence()).System
	lower := strings.ToLower(sys)

	require.Contains(t, lower, "reads")
	require.Contains(t, lower, "config")
	require.Contains(t, lower, "unchanged", "the prompt must tell the model to leave other nodes untouched")
	require.Contains(t, lower, "complete", "the prompt must demand each file's full new content")
}

// TestPythonPrompts_DescribeTheODCSContract pins that every python prompt
// describes the file the model edits as it is: one ODCS contract per node at
// contracts/<table>.odcs.yaml, its id, its script, its readQueries (each one
// SELECT naming every table's schema) and the columns of schema[0].properties,
// and that none still describes a "nodes:" list.
func TestPythonPrompts_DescribeTheODCSContract(t *testing.T) {
	noReads := pythonEvidence()
	noReads.NoReads = true
	for name, sys := range map[string]string{
		"validation":           AssemblePythonContractFix(pythonEvidence()).System,
		"validation, no reads": AssemblePythonContractFix(noReads).System,
		"parse":                AssemblePythonParseFix(pythonEvidence()).System,
	} {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, sys, "contracts/<table>.odcs.yaml")
			require.Contains(t, sys, "one contract per node")
			require.Contains(t, sys, `id is "<schema>.<table>"`)
			require.Contains(t, sys, "script")
			require.Contains(t, sys, "schema[0].properties")
			require.Contains(t, sys, "secretRef")
			require.NotContains(t, sys, "nodes:")
			require.NotContains(t, sys, "output_columns")
			require.NotContains(t, sys, "secret_ref")
		})
	}
	for name, sys := range map[string]string{
		"validation": AssemblePythonContractFix(pythonEvidence()).System,
		"parse":      AssemblePythonParseFix(pythonEvidence()).System,
	} {
		require.Contains(t, sys, "readQueries", name)
		require.Contains(t, sys, "each query is one SELECT that names every table it reads with its schema", name)
	}
}

// TestAssemblePythonContractFix_NoReadsNodeSaysItFetchesItsOwnData pins that a
// node declaring no reads is not described as reading upstream relations, and
// that every python prompt lists secretRef among the fields a fix never
// touches.
func TestAssemblePythonContractFix_NoReadsNodeSaysItFetchesItsOwnData(t *testing.T) {
	ev := pythonEvidence()
	ev.NoReads = true
	req := AssemblePythonContractFix(ev)
	all := req.System + req.User

	require.Contains(t, all, "declares no reads")
	require.NotContains(t, all, "the upstream relations it reads")
	require.NotContains(t, req.System, "declared reads")
	require.NotContains(t, req.System, "adding a read is allowed")
	require.NotContains(t, req.System, "Keep every read")
	require.Contains(t, req.System, "Never add a read")
	require.Contains(t, req.System, "the script that produces it. This node declares no reads")
	require.NotEqual(t, pythonContractFixSystemPrompt, noReadsSystemPrompt(pythonContractFixSystemPrompt),
		"the rewrite must change the base prompt")

	withReads := AssemblePythonContractFix(pythonEvidence())
	require.Contains(t, withReads.System, "the upstream relations it reads")
	require.NotContains(t, withReads.System, "declares no reads")
	require.Contains(t, withReads.System, "declared reads")
	require.Contains(t, withReads.System, "adding a read is allowed")
	require.Contains(t, withReads.System, "Keep every read")

	for name, sys := range map[string]string{
		"validation":           withReads.System,
		"validation, no reads": req.System,
		"parse":                AssemblePythonParseFix(pythonEvidence()).System,
	} {
		require.Contains(t, sys, "secretRef", "%s prompt must forbid touching secretRef", name)
	}
}

// TestAssemblePythonParseFix_NoReadsLeavesTheParsePromptUnchanged pins that the
// no-reads rewrite only applies to the validation prompt: the parse prompt does
// not contain the reads wording, so it must be sent as is and never panic.
func TestAssemblePythonParseFix_NoReadsLeavesTheParsePromptUnchanged(t *testing.T) {
	ev := pythonEvidence()
	ev.NoReads = true
	var req ProposeRequest
	require.NotPanics(t, func() { req = AssemblePythonParseFix(ev) })
	require.Equal(t, pythonParseFixSystemPrompt, req.System)
}

// TestAssemblePythonParseFix_ShowsTheParserError verifies the parse-stage
// request: the parser's error stands where the validation error would, no
// runner-log section can appear (no Job ran), the declaring yaml is shown, and
// the system prompt frames the task as making a read's SQL parse rather than
// making validation pass.
func TestAssemblePythonParseFix_ShowsTheParserError(t *testing.T) {
	ev := pythonEvidence()
	ev.ErrorExcerpt = "unqualified table reference `orders` in read `orders`. Line 1, Col: 16."
	ev.RunnerLog = ""
	ev.ContractEntry = ""

	req := AssemblePythonParseFix(ev)
	u := req.User

	require.Contains(t, u, "analytics.py_daily_kpis")
	require.Contains(t, u, "SQL parse error:")
	require.Contains(t, u, "unqualified table reference `orders`")
	require.NotContains(t, u, "Validation error:")
	require.NotContains(t, u, "Full runner log")
	require.Contains(t, u, "```yaml\napiVersion: v3.2.0\nkind: DataContract\nid: analytics.py_daily_kpis\n")
	require.Contains(t, u, "services/service-py/contracts/py_daily_kpis.odcs.yaml")

	require.Contains(t, req.System, "parser rejected")
	require.Contains(t, req.System, "schema-qualified")
	require.Contains(t, req.System, "Keep every read")

	// Same answer shape as the validation fix, so one adapter parses both.
	require.Equal(t, "propose_python_fix", req.ToolName)
	require.Equal(t, AssemblePythonContractFix(ev).ToolParams, req.ToolParams)
}

// TestPythonPrompts_ShowTheCurrentContract pins that every python contract
// prompt shows the node's current contract under its own heading when there is
// one, and leaves the heading out when there is none.
func TestPythonPrompts_ShowTheCurrentContract(t *testing.T) {
	const current = "apiVersion: v3.2.0\nkind: DataContract\nid: analytics.kpis\nversion: \"4\"\n"
	builders := map[string]func(PythonEvidence) ProposeRequest{
		"python validation": AssemblePythonContractFix,
		"python parse":      AssemblePythonParseFix,
		"csv validation":    func(ev PythonEvidence) ProposeRequest { return AssembleCsvContractFix(CsvEvidence(ev)) },
	}
	for name, build := range builders {
		t.Run(name, func(t *testing.T) {
			ev := PythonEvidence{NodeID: "analytics.kpis", YAMLPath: "contracts/kpis.odcs.yaml", YAMLText: "id: analytics.kpis\n"}

			without := build(ev).User
			require.NotContains(t, without, "Current contract")

			ev.CurrentContract = current
			with := build(ev).User
			require.Contains(t, with, "Current contract (the node's contract as continuo last promoted it, for reference only: do not copy its version, or its provenance and reads properties, into the file):\n```yaml\n"+current+"\n```\n\n")
			require.Less(t, strings.Index(with, "Contract file contracts/kpis.odcs.yaml"), strings.Index(with, "Current contract"),
				"the current contract follows the file the fix edits")
		})
	}
}
