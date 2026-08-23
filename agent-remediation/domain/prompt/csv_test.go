package prompt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// csvEvidence builds a fully-populated CsvEvidence: every evidence section a
// csv contract fix can carry is present, so a renderer that drops one is
// caught rather than passing on the sections it happens to render.
func csvEvidence() CsvEvidence {
	return CsvEvidence{
		NodeID:        "analytics.py_csv_orders",
		ErrorExcerpt:  "csv header missing declared column(s): ['customer_id']",
		RunnerLog:     "continuo-runtime: validating analytics.py_csv_orders\nFAILED",
		ContractEntry: `{"output_columns":[{"name":"customer_id"}],"reads":{"csv":"s3://exports/orders/orders.csv"}}`,
		YAMLPath:      "services/service-csv/contracts/py_csv_orders.yml",
		YAMLText:      "nodes:\n  - schema: analytics\n    table: py_csv_orders\n    kind: python-csv\n",
		UpstreamChanges: []UpstreamChange{
			{NodeID: "analytics.orders", Depth: 1, CodeDiff: "-  revenue\n+  revenue_amount", ConfigDiff: "+materialized: table"},
		},
		Precedents: []Precedent{
			{NodeID: "analytics.other_csv", Category: "validation", Reason: "missing_column",
				ErrorExcerpt: "column missing", RejectedAt: "2026-08-01T00:00:00Z",
				Resolved: true, ResolutionDiff: "-old\n+new", PRURL: "https://example.test/pr/1"},
		},
		PriorAttempts: []PriorAttempt{
			{
				Attempt:     1,
				VerifyError: "shadow rejected: customer_id still missing",
				Diffs: []AttemptDiff{
					{Path: "services/service-csv/contracts/py_csv_orders.yml", Diff: "-    - name: cust_id\n+    - name: customer_id"},
				},
			},
		},
	}
}

// TestAssembleCsvContractFix_RendersEverySection pins that every evidence
// section reaches the model, each in the fence its content type calls for.
// The per-attempt evidence (the prior attempt's diff and the shadow release's
// error) is what makes a second attempt better-informed than the first, so a
// renderer that silently drops a section would degrade retries without any
// visible failure.
func TestAssembleCsvContractFix_RendersEverySection(t *testing.T) {
	req := AssembleCsvContractFix(csvEvidence())
	u := req.User

	require.Contains(t, u, "analytics.py_csv_orders")
	require.Contains(t, u, "csv header missing declared column(s): ['customer_id']")
	require.Contains(t, u, "continuo-runtime: validating analytics.py_csv_orders")

	// The contract entry is canonical JSON and the declaring file is yaml; each
	// is fenced as what it is so the model does not have to guess.
	require.Contains(t, u, "```json\n"+`{"output_columns":[{"name":"customer_id"}],"reads":{"csv":"s3://exports/orders/orders.csv"}}`)
	require.Contains(t, u, "```yaml\nnodes:\n  - schema: analytics\n    table: py_csv_orders\n    kind: python-csv\n")
	require.Contains(t, u, "services/service-csv/contracts/py_csv_orders.yml")

	// Upstream changes and precedent.
	require.Contains(t, u, "analytics.orders")
	require.Contains(t, u, "+  revenue_amount")
	require.Contains(t, u, "+materialized: table")
	require.Contains(t, u, "How similar failures were fixed before")
	require.Contains(t, u, "https://example.test/pr/1")

	// Prior attempts: the shadow release's error and the diff that attempt made.
	require.Contains(t, u, "shadow rejected: customer_id still missing")
	require.Contains(t, u, "+    - name: customer_id")
}

// TestAssembleCsvContractFix_SectionsOmittedWhenAbsent verifies that an
// evidence value with nothing to say renders no heading at all, so a first
// attempt's prompt is not padded with empty scaffolding the model has to read
// past.
func TestAssembleCsvContractFix_SectionsOmittedWhenAbsent(t *testing.T) {
	req := AssembleCsvContractFix(CsvEvidence{
		NodeID:       "analytics.py_csv_orders",
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

// TestAssembleCsvContractFix_ToolSchema pins the forced tool: the fix is
// returned as a list of complete files, so a model that can only name one
// file cannot express a multi-node contract change.
func TestAssembleCsvContractFix_ToolSchema(t *testing.T) {
	req := AssembleCsvContractFix(csvEvidence())

	require.Equal(t, "propose_csv_fix", req.ToolName)

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

// TestAssembleCsvContractFix_SystemPromptScopesTheEdit pins the rules that
// keep a csv contract fix from breaking the release it is meant to repair,
// and that keep it a genuinely different prompt from the python-model lane's:
// the model must be told what a fix may touch (output_columns, the csv uri),
// that the "csv" read may never be deleted or renamed, and that the node's
// kind is identity it may never change.
func TestAssembleCsvContractFix_SystemPromptScopesTheEdit(t *testing.T) {
	sys := AssembleCsvContractFix(csvEvidence()).System
	lower := strings.ToLower(sys)

	require.Contains(t, lower, "output_columns")
	require.Contains(t, lower, "csv:")
	require.Contains(t, lower, "never")
	require.Contains(t, lower, "deleted or renamed")
	require.Contains(t, lower, "kind")
	require.Contains(t, lower, "complete", "the prompt must demand each file's full new content")
}
