package serialization_test

import (
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// journalRow is one done journal row of try 1 holding rec as its output.
func journalRow[R serialization.JournalRecord](t *testing.T, cluster int, step, kind string, draft int, rec R, findings ...remediation.Finding) remediation.StepRun {
	t.Helper()
	out, err := serialization.EncodeRecord(rec)
	require.NoError(t, err)
	return remediation.StepRun{
		Try: 1, Cluster: cluster, Step: step, Kind: kind, Draft: draft,
		Status: remediation.StepDone, Output: out, Findings: findings,
	}
}

func TestTryRecordOf_ReadsEachClustersEditsAndFindingsAndTheTrysOwn(t *testing.T) {
	unchanged := remediation.Finding{Code: "content_unchanged", Message: "the answer repeats the original"}
	twice := remediation.Finding{Code: "path_edited_twice", Message: "models/a.sql is edited twice"}
	kinds := remediation.Finding{Code: "two_manifest_kinds", Message: "dbt and python edits in one try"}
	broken := remediation.Finding{Code: "release_check_failed", Message: "model.a: still broken"}
	first := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: "select a\n", Content: "select a\n"}}}
	second := remediation.Draft{Edits: []remediation.Edit{
		{Path: "models/a.sql", Original: "select a\n", Content: "select b\n"},
		{Path: "models/unchanged.sql", Original: "select 1\n", Content: "select 1\n"},
	}}
	failedRow := journalRow(t, 2, remediation.StepPreflight, remediation.KindRunner, 1, serialization.PreflightRecord{Draft: second})
	failedRow.Status = remediation.StepFailed

	snap := remediation.Snapshot{
		Remediation: remediation.Remediation{ID: uuid.New(), Round: 2, CurrentTry: 1},
		Try:         &remediation.Try{Try: 1, Status: remediation.TryFailed},
		Clusters: []remediation.Cluster{
			{Cluster: 1, Kind: flow.ClusterSingle, TargetNodeID: "model.a", Members: []string{"model.a"}, TypeName: "dbt-validation", Status: remediation.ClusterChecked, Draft: 2},
			{Cluster: 2, Kind: flow.ClusterSingle, TargetNodeID: "model.b", Members: []string{"model.b"}, TypeName: "dbt-validation",
				Status: remediation.ClusterHandedOver, Draft: 1, Outcome: &fix.NodeOutcome{Status: fix.StatusSkipped, Reason: "the model declined"}},
			{Cluster: 3, Kind: flow.ClusterSingle, TargetNodeID: "model.c", Members: []string{"model.c"}, TypeName: "dbt-validation",
				Status: remediation.ClusterHandedOver, Draft: 1, Outcome: &fix.NodeOutcome{
					Status: fix.StatusSkipped, Reason: "the node's source is not in the release's code bundle", Code: model.HandoverReasonDeclined,
				}},
		},
		Steps: []remediation.StepRun{
			journalRow(t, 1, "dbt-validation", "prompt", 1, serialization.PromptRecord{Draft: first}),
			journalRow(t, 1, "content_changed", remediation.KindCheck, 1, serialization.CheckRecord{Result: remediation.CheckResult{Check: "content_changed", Findings: []remediation.Finding{unchanged}}}, unchanged),
			journalRow(t, 1, remediation.StepPreflight, remediation.KindRunner, 1, serialization.PreflightRecord{Draft: first}, unchanged),
			journalRow(t, 1, "dbt-validation", "prompt", 2, serialization.PromptRecord{Draft: second}),
			journalRow(t, 1, remediation.StepPreflight, remediation.KindRunner, 2, serialization.PreflightRecord{Draft: second}),
			failedRow,
			journalRow(t, 0, "no_path_edited_twice", remediation.KindCheck, 1, serialization.CheckRecord{Result: remediation.CheckResult{Check: "no_path_edited_twice", Findings: []remediation.Finding{twice}}}, twice),
			journalRow(t, 0, "one_manifest_kind", remediation.KindCheck, 1, serialization.CheckRecord{Result: remediation.CheckResult{Check: "one_manifest_kind", Findings: []remediation.Finding{kinds}}}, kinds),
			journalRow(t, 0, "release_check", remediation.KindCheck, 1, serialization.CheckRecord{Result: remediation.CheckResult{Check: "release_check", Findings: []remediation.Finding{broken}}, Gate: true}, broken),
			{Try: 1, Step: "hand_over", Kind: "output", Draft: 1, Status: remediation.StepDone},
		},
	}

	for i := range snap.Steps {
		snap.Steps[i].ID = int64(i + 1)
	}

	got, err := serialization.TryRecordOf(snap)
	require.NoError(t, err)

	reversed := snap
	reversed.Steps = slices.Clone(snap.Steps)
	slices.Reverse(reversed.Steps)
	reversed.Clusters = slices.Clone(snap.Clusters)
	slices.Reverse(reversed.Clusters)
	again, err := serialization.TryRecordOf(reversed)
	require.NoError(t, err)
	assert.Equal(t, got, again, "the record does not depend on the order the snapshot lists its rows and clusters in")

	assert.Equal(t, remediation.TryRecord{
		Round: 2, Try: 1,
		Clusters: []remediation.ClusterRecord{
			{
				Type: "dbt-validation", TargetNodeID: "model.a", Members: []string{"model.a"},
				Edits:  []remediation.EditRecord{{Path: "models/a.sql", Diff: fix.ComputeUnifiedDiff("select a\n", "select b\n", "models/a.sql")}},
				Drafts: [][]remediation.Finding{{unchanged}, nil},
			},
			{
				Type: "dbt-validation", TargetNodeID: "model.b", Members: []string{"model.b"},
				Outcome: "skipped: the model declined",
			},
			{
				Type: "dbt-validation", TargetNodeID: "model.c", Members: []string{"model.c"},
				Outcome: "skipped (declined): the node's source is not in the release's code bundle",
			},
		},
		CrossCluster: []remediation.Finding{twice, kinds},
		Gate:         []remediation.Finding{broken},
	}, got)
}

func TestTryRecordOf_RefusesASnapshotWithNoTryOrAnUnreadableRow(t *testing.T) {
	_, err := serialization.TryRecordOf(remediation.Snapshot{Remediation: remediation.Remediation{ID: uuid.New(), Round: 1}})
	require.Error(t, err)

	snap := remediation.Snapshot{
		Remediation: remediation.Remediation{ID: uuid.New(), Round: 1, CurrentTry: 1},
		Try:         &remediation.Try{Try: 1},
		Clusters:    []remediation.Cluster{{Cluster: 1, Members: []string{"model.a"}}},
		Steps: []remediation.StepRun{{
			Try: 1, Cluster: 1, Step: remediation.StepPreflight, Kind: remediation.KindRunner, Draft: 1,
			Status: remediation.StepDone, Output: []byte("not json"),
		}},
	}
	_, err = serialization.TryRecordOf(snap)
	require.Error(t, err)
}

// TestTryRecordOf_AMovedFileIsShownAsARename pins that an earlier try which
// moved a file without changing its content is shown with the move, not left
// out as an edit with no diff.
func TestTryRecordOf_AMovedFileIsShownAsARename(t *testing.T) {
	moved := remediation.Draft{Edits: []remediation.Edit{{
		Path: "models/table_b_v2.sql", RenamedFrom: "models/table_b.sql", Original: "select 1\n", Content: "select 1\n",
	}}}
	snap := remediation.Snapshot{
		Remediation: remediation.Remediation{ID: uuid.New(), Round: 1, CurrentTry: 1},
		Try:         &remediation.Try{Try: 1, Status: remediation.TryFailed},
		Clusters: []remediation.Cluster{{
			Cluster: 1, Kind: flow.ClusterSingle, TargetNodeID: "analytics.table_b", Members: []string{"analytics.table_b"},
			TypeName: "duplicate-table", Status: remediation.ClusterChecked, Draft: 1,
		}},
		Steps: []remediation.StepRun{
			journalRow(t, 1, remediation.StepPreflight, remediation.KindRunner, 1, serialization.PreflightRecord{Draft: moved}),
		},
	}
	snap.Steps[0].ID = 1

	got, err := serialization.TryRecordOf(snap)
	require.NoError(t, err)
	require.Len(t, got.Clusters, 1)
	assert.Equal(t, []remediation.EditRecord{{
		Path: "models/table_b_v2.sql",
		Diff: "rename from models/table_b.sql\nrename to models/table_b_v2.sql\n",
	}}, got.Clusters[0].Edits)
}
