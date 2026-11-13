package serialization_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

// The golden files under testdata/journal hold the JSON the runner's journal
// stores in remediation_step.output: one file per record shape and one per
// evidence key. A change to a domain type that changes what a row stores, or
// what a stored row decodes into, fails these tests. Run them with -update to
// rewrite the files, and only when rows already stored still decode.
var updateGolden = flag.Bool("update", false, "rewrite the golden files under testdata/journal")

// golden compares got with the golden file testdata/journal/<name>.json,
// ignoring whitespace and key order, and returns the file's content. With
// -update it writes got, indented, to the file first.
func golden(t *testing.T, name string, got []byte) []byte {
	t.Helper()
	path := filepath.Join("testdata", "journal", filepath.FromSlash(name)+".json")
	if *updateGolden {
		var out bytes.Buffer
		require.NoError(t, json.Indent(&out, got, "", "  "))
		out.WriteByte('\n')
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, out.Bytes(), 0o600))
	}
	want, err := os.ReadFile(path) //nolint:gosec // G304: a golden file of this package's testdata.
	require.NoError(t, err, "missing golden file; run the test with -update to write it")
	assert.JSONEq(t, string(want), string(got), "the stored JSON of %s changed", name)
	return want
}

// requireEveryFieldSet fails unless every exported field of v, at every depth,
// holds a value that is not its zero value, and every slice and map holds at
// least one element. A golden file made from such a value names every field, so
// a field added to or renamed in the type changes it.
func requireEveryFieldSet(t *testing.T, path string, v reflect.Value) {
	t.Helper()
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		require.False(t, v.IsNil(), "%s is nil", path)
		requireEveryFieldSet(t, path, v.Elem())
	case reflect.Struct:
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if !f.IsExported() {
				continue
			}
			requireEveryFieldSet(t, path+"."+f.Name, v.Field(i))
		}
	case reflect.Slice, reflect.Map:
		require.Positive(t, v.Len(), "%s is empty", path)
		if v.Kind() == reflect.Slice {
			for i := range v.Len() {
				requireEveryFieldSet(t, path+"[]", v.Index(i))
			}
			return
		}
		for _, k := range v.MapKeys() {
			requireEveryFieldSet(t, path+"[]", v.MapIndex(k))
		}
	default:
		require.False(t, v.IsZero(), "%s is its zero value", path)
	}
}

func sampleNode(id string) remediation.TriggerNode {
	return remediation.TriggerNode{
		NodeID:               id,
		RelationID:           "analytics.orders_daily",
		ErrorSignature:       "sig-1",
		Category:             "compile",
		Reason:               "compile_error",
		ErrorExcerpt:         "column x does not exist",
		DBTLogURI:            "s3://logs/dbt.log",
		CandidateArtifactURI: "s3://candidate/orders_daily.sql",
		FilePath:             "models/orders_daily.sql",
		Service:              "orders",
		NodeType:             "model",
		OtherService:         "billing",
		OtherFilePath:        "models/other.sql",
		ChangedAncestors: []remediation.ChangedAncestor{
			{NodeID: "orders.model.stg_orders", FilePath: "models/stg_orders.sql", Service: "orders", Depth: 1},
		},
	}
}

func sampleFailure() remediation.Failure {
	target := sampleNode("orders.model.orders_daily")
	return remediation.Failure{
		Trigger: remediation.Trigger{
			TenantID:      "acme",
			Source:        "release-validation",
			ReleaseID:     "rel-1",
			Round:         2,
			Repo:          "org/repo",
			CommitSHA:     "abc123",
			CodeBundleURI: "s3://bundles/rel-1.tar.gz",
			Nodes:         []remediation.TriggerNode{target},
		},
		Kind:         workflow.ClusterSharedUpstream,
		TargetNodeID: target.NodeID,
		Target:       target,
		Members:      []remediation.TriggerNode{target},
		Attempt:      3,
	}
}

func sampleDraft() remediation.Draft {
	return remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         "services/orders/models/orders_daily.sql",
			Original:     "select 1",
			Content:      "select 2",
			TargetNodeID: "orders.model.orders_daily",
			ContentKey:   "proposed-fix/rel-1/orders/attempt-3.source.sql",
			DiffKey:      "proposed-fix/rel-1/orders/attempt-3.source.diff",
			ContentURI:   "s3://artifacts/proposed-fix/rel-1/orders/attempt-3.source.sql",
			DiffURI:      "s3://artifacts/proposed-fix/rel-1/orders/attempt-3.source.diff",
		}},
		NamedTarget:         "models/orders_daily.sql",
		TargetShown:         true,
		Rationale:           "the column was renamed upstream",
		Confidence:          "high",
		Model:               "model-1",
		SuspectedRoot:       "orders.model.stg_orders",
		CandidateFixSQLURI:  "s3://artifacts/fix.sql",
		CandidateFixDiffURI: "s3://artifacts/fix.diff",
		SourceResolved:      true,
	}
}

func sampleCheckResult() remediation.CheckResult {
	return remediation.CheckResult{
		Check:    "dbt-compile",
		Findings: []remediation.Finding{{Code: "compile_error", Message: "column x does not exist", File: "models/orders_daily.sql", Line: 4}},
		Waiting:  true,
		RunIDs:   []string{"run-1"},
	}
}

// evidenceSamples is one value of the Go type every evidence key holds.
func evidenceSamples() map[remediation.EvidenceKey]any {
	return map[remediation.EvidenceKey]any{
		remediation.KeyFailure: sampleFailure(),
		remediation.KeyDraft:   sampleDraft(),
		remediation.KeyDBTLog:  remediation.Text("Compilation Error in model orders_daily"),
		remediation.KeyFailurePrecedents: []prompt.Precedent{{
			ReleaseID:      "rel-0",
			NodeID:         "orders.model.orders_daily",
			Stage:          "validation",
			Category:       "compile",
			Reason:         "compile_error",
			ErrorExcerpt:   "column y does not exist",
			RejectedAt:     "2026-11-01T09:00:00Z",
			Resolved:       true,
			ResolutionDiff: "-y\n+z",
			DiffTruncated:  true,
			PRURL:          "https://github.com/org/repo/pull/7",
			Edited: []prompt.EditedPrecedent{{
				NodeID: "orders.model.orders_daily", Path: "models/orders_daily.sql", Amended: true, Diff: "-y\n+z", DiffIsShipped: true,
			}},
		}},
		remediation.KeyUpstreamChanges: []prompt.UpstreamChange{{
			NodeID: "orders.model.stg_orders", Depth: 1, CodeDiff: "-a\n+b", ConfigDiff: "-c\n+d", Truncated: true,
		}},
		remediation.KeySourceFiles: remediation.SourceFiles{
			Files:   map[string]string{"models/orders_daily.sql": "select 1"},
			Order:   []string{"models/orders_daily.sql"},
			Primary: "models/orders_daily.sql",
			Service: "orders",
		},
		remediation.KeyCandidateSQL: remediation.Text("select 1 from analytics.orders"),
		remediation.KeyCodeBundleSource: remediation.CandidateSource{
			Source: "select 1", Origin: "code_bundle", FilePath: "models/orders_daily.sql", Service: "orders",
		},
		remediation.KeyOwnChangeDiff: remediation.Text("-select 0\n+select 1"),
		remediation.KeyChangedAncestorDiffs: []prompt.ReleaseUpstreamChange{{
			NodeID: "orders.model.stg_orders", Service: "orders", Depth: 1, Diff: "-a\n+b",
		}},
		remediation.KeyContractFile: remediation.ContractFile{
			Schema: "analytics", Table: "fx", RepoRoot: "svc", ContractDir: "svc/contracts",
			YAMLPath: "svc/contracts/fx.yaml", YAMLText: "nodes: []",
		},
		remediation.KeyContractEntry: remediation.Text("schema: analytics\ntable: fx"),
		remediation.KeyPriorAttempts: []prompt.PriorAttempt{{
			Attempt: 1, VerifyError: "column x does not exist", Diffs: []prompt.AttemptDiff{{Path: "svc/contracts/fx.yaml", Diff: "-a\n+b"}},
		}},
		remediation.KeyPackagedContract: remediation.Bytes("packaged contract"),
		remediation.KeyUpstreamTarget: remediation.UpstreamTarget{
			NodeID: "orders.model.stg_orders", Source: "select a", FilePath: "models/stg_orders.sql",
			Service: "orders", OwnChangeDiff: "-a\n+b",
		},
	}
}

// registeredKeys is every evidence key the runner puts in a cluster's evidence
// itself or a registered step reads or writes.
func registeredKeys(reg *runner.Registry) []remediation.EvidenceKey {
	keys := []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyDraft}
	for _, m := range reg.All() {
		keys = append(keys, m.Needs...)
		keys = append(keys, m.Provides...)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// Every evidence key the runner or a registered step uses has a sample here of
// the Go type the registry decodes it into, so it has a golden file.
func TestJournal_EveryEvidenceKeyHasASampleOfItsRegisteredType(t *testing.T) {
	reg := runner.NewRegistry(runner.Collaborators{})
	samples := evidenceSamples()
	for _, k := range registeredKeys(reg) {
		zero, ok := reg.EvidenceType(k)
		require.True(t, ok, "evidence %q has no registered type", k)
		sample, ok := samples[k]
		if assert.True(t, ok, "evidence %q has no sample; add one and run with -update", k) {
			assert.Equal(t, reflect.TypeOf(zero()), reflect.TypeOf(sample), "the sample of %q is not of its registered type", k)
		}
	}
	for k := range samples {
		_, ok := reg.EvidenceType(k)
		assert.True(t, ok, "the sample of %q names a key the registry does not know", k)
	}
}

// Each evidence value is stored as its golden file holds it, and the golden
// file decodes back into the same value.
func TestJournal_EvidenceValuesKeepTheirStoredJSON(t *testing.T) {
	reg := runner.NewRegistry(runner.Collaborators{})
	for k, sample := range evidenceSamples() {
		t.Run(string(k), func(t *testing.T) {
			requireEveryFieldSet(t, string(k), reflect.ValueOf(sample))
			encoded, err := serialization.EncodeEvidence(map[remediation.EvidenceKey]any{k: sample}, reg.EvidenceType)
			require.NoError(t, err)
			stored := golden(t, "evidence/"+string(k), encoded[k])

			ev, err := serialization.EncodedEvidence{k: stored}.AddTo(remediation.NewEvidence(), reg.EvidenceType)
			require.NoError(t, err)
			got, ok := remediation.Get[any](ev, k)
			require.True(t, ok)
			assert.Equal(t, sample, got, "a stored value decodes back into the same value")
		})
	}
}

func TestJournal_EncodeEvidenceRefusesAKeyWithNoRegisteredType(t *testing.T) {
	reg := runner.NewRegistry(runner.Collaborators{})
	_, err := serialization.EncodeEvidence(map[remediation.EvidenceKey]any{"no_such_key": remediation.Text("x")}, reg.EvidenceType)
	require.EqualError(t, err, `evidence "no_such_key" has no registered type`)

	_, err = serialization.EncodedEvidence{"no_such_key": json.RawMessage(`"x"`)}.AddTo(remediation.NewEvidence(), reg.EvidenceType)
	require.EqualError(t, err, `evidence "no_such_key" has no registered type`)
}

func TestJournal_NoEvidenceEncodesAsNil(t *testing.T) {
	encoded, err := serialization.EncodeEvidence(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, encoded)
}

// record is one record shape of the journal, encoded and decoded through
// EncodeRecord and DecodeRecord.
type record struct {
	name   string
	encode func() ([]byte, error)
	// decoded checks what the golden file decodes into.
	decoded func(t *testing.T, stored []byte)
}

func recordOf[R serialization.JournalRecord](name string, rec R, check func(t *testing.T, got R)) record {
	return record{
		name:   name,
		encode: func() ([]byte, error) { return serialization.EncodeRecord(rec) },
		decoded: func(t *testing.T, stored []byte) {
			got, err := serialization.DecodeRecord[R](stored)
			require.NoError(t, err)
			check(t, got)
		},
	}
}

// Each record a journal row stores keeps its golden JSON, and its golden file
// decodes back into the same record.
func TestJournal_RecordsKeepTheirStoredJSON(t *testing.T) {
	reg := runner.NewRegistry(runner.Collaborators{})
	evidence, err := serialization.EncodeEvidence(map[remediation.EvidenceKey]any{
		remediation.KeyDBTLog:      remediation.Text("Compilation Error"),
		remediation.KeySourceFiles: evidenceSamples()[remediation.KeySourceFiles],
	}, reg.EvidenceType)
	require.NoError(t, err)
	sameEvidence := func(t *testing.T, got serialization.EncodedEvidence) {
		t.Helper()
		want, err := evidence.AddTo(remediation.NewEvidence(), reg.EvidenceType)
		require.NoError(t, err)
		have, err := got.AddTo(remediation.NewEvidence(), reg.EvidenceType)
		require.NoError(t, err)
		assert.Equal(t, want, have)
	}

	records := []record{
		recordOf("context", serialization.ContextRecord{Evidence: evidence}, func(t *testing.T, got serialization.ContextRecord) {
			sameEvidence(t, got.Evidence)
			assert.False(t, got.Declined)
		}),
		recordOf("context_declined", serialization.ContextRecord{Declined: true, Decline: "no source for an ingested node"}, func(t *testing.T, got serialization.ContextRecord) {
			assert.Equal(t, serialization.ContextRecord{Declined: true, Decline: "no source for an ingested node"}, got)
		}),
		recordOf("prompt", serialization.PromptRecord{Draft: sampleDraft()}, func(t *testing.T, got serialization.PromptRecord) {
			assert.Equal(t, serialization.PromptRecord{Draft: sampleDraft()}, got)
		}),
		recordOf("prompt_declined", serialization.PromptRecord{Declined: true, Decline: "the model declined"}, func(t *testing.T, got serialization.PromptRecord) {
			assert.Equal(t, serialization.PromptRecord{Declined: true, Decline: "the model declined"}, got)
		}),
		recordOf("check", serialization.CheckRecord{Result: sampleCheckResult(), Evidence: evidence}, func(t *testing.T, got serialization.CheckRecord) {
			assert.Equal(t, sampleCheckResult(), got.Result)
			sameEvidence(t, got.Evidence)
		}),
		recordOf("check_result_only", serialization.CheckRecord{Result: sampleCheckResult()}, func(t *testing.T, got serialization.CheckRecord) {
			assert.Equal(t, serialization.CheckRecord{Result: sampleCheckResult()}, got)
		}),
		recordOf("preflight", serialization.PreflightRecord{Draft: sampleDraft()}, func(t *testing.T, got serialization.PreflightRecord) {
			assert.Equal(t, serialization.PreflightRecord{Draft: sampleDraft()}, got)
		}),
	}
	requireEveryFieldSet(t, "draft", reflect.ValueOf(sampleDraft()))
	requireEveryFieldSet(t, "check result", reflect.ValueOf(sampleCheckResult()))
	for _, r := range records {
		t.Run(r.name, func(t *testing.T) {
			out, err := r.encode()
			require.NoError(t, err)
			r.decoded(t, golden(t, "records/"+r.name, out))
		})
	}
}

func TestJournal_DecodeRecordReturnsTheZeroRecordForOutputThatDoesNotDecode(t *testing.T) {
	got, err := serialization.DecodeRecord[serialization.PromptRecord]([]byte(`{"draft":`))
	require.Error(t, err)
	assert.Equal(t, serialization.PromptRecord{}, got)
}

// A digest changes with anything the step ran on, and only with that.
func TestJournal_DigestsIdentifyTheInput(t *testing.T) {
	ev := remediation.NewEvidence().With(remediation.KeyDBTLog, remediation.Text("log"))
	a, err := serialization.InputDigest(ev, 1)
	require.NoError(t, err)
	again, err := serialization.InputDigest(remediation.NewEvidence().With(remediation.KeyDBTLog, remediation.Text("log")), 1)
	require.NoError(t, err)
	otherRound, err := serialization.InputDigest(ev, 2)
	require.NoError(t, err)
	otherEvidence, err := serialization.InputDigest(remediation.NewEvidence().With(remediation.KeyDBTLog, remediation.Text("other")), 1)
	require.NoError(t, err)
	assert.Len(t, a, 64)
	assert.Equal(t, a, again)
	assert.NotEqual(t, a, otherRound)
	assert.NotEqual(t, a, otherEvidence)

	drafts := []serialization.DigestedDraft{{Cluster: 1, Draft: sampleDraft(), Evidence: ev}}
	d, err := serialization.DraftsDigest(drafts)
	require.NoError(t, err)
	dAgain, err := serialization.DraftsDigest([]serialization.DigestedDraft{{Cluster: 1, Draft: sampleDraft(), Evidence: ev}})
	require.NoError(t, err)
	otherCluster, err := serialization.DraftsDigest([]serialization.DigestedDraft{{Cluster: 2, Draft: sampleDraft(), Evidence: ev}})
	require.NoError(t, err)
	assert.Equal(t, d, dAgain)
	assert.NotEqual(t, d, otherCluster)
}
