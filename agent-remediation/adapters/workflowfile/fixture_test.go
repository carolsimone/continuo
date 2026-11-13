package workflowfile

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

const typeFixtureYAML = `kind: fixture
version: 1
type: dbt-validation
trigger:
  source: validation
  release_id: rel-1
  remediation_round: 2
  repo: o/repo
  commit_sha: sha
  code_bundle_uri: s3://bundle
  nodes:
    - node_id: model.sales.orders
      category: validation
      error_signature: sig-1
      reason: missing_column
      error_excerpt: column amount does not exist
      dbt_log_uri: s3://log
      candidate_artifact_uri: s3://cand
      file_path: models/orders.sql
      service: sales
      node_type: dbt-model
      changed_ancestors:
        - { node_id: model.sales.base, file_path: models/base.sql, service: sales, depth: 1 }
service_repo_paths: { sales: services/sales }
repo_files: { services/sales/models/orders.sql: "select 1" }
objects: { "s3://cand": "select 0" }
bundle: { model.sales.orders: { runtime: dbt, raw_code: "select 0" } }
catalog:
  locations: { model.sales.base: { file_path: models/base.sql, service: sales } }
  current_versions: { model.sales.orders: { raw_code: "select 9", content_hash: h1, promoted_at: "2026-11-01T00:00:00Z" } }
  upstream_changes: { model.sales.orders: [{ node_id: model.sales.base, depth: 1, code_diff: "-a\n+b" }] }
  precedents: [{ node_id: model.sales.other, category: validation, reason: missing_column, resolved: true }]
release: { image_tag: sha-abc, failing_nodes: { model.sales.orders: boom } }
packager: { merged: "merged: contract" }
answers:
  pass:
    - { proposed_sql: "select 1", rationale: diagnosis, confidence: high, model: m }
    - { proposed_sql: "select 2", confidence: high }
  fail: [{ proposed_sql: "" }]
  decline: [{ files: [{ path: a.yml, content: x }], confidence: low }]
member_answers: { model.sales.base: { pass: [{ target_file: t.sql, proposed_content: c, suspected_root_cause_node: n }] } }
decline_before_model: { node: { candidate_artifact_uri: "", service: other } }
gate_errors: { model.sales.orders: still failing }
`

func TestLoadFixtures_ReadsEveryField(t *testing.T) {
	fsys := fstest.MapFS{"fixtures/types/dbt-validation.yaml": {Data: []byte(typeFixtureYAML)}}

	fixtures, err := LoadFixtures(fsys)
	require.NoError(t, err)
	require.Len(t, fixtures, 1)
	f := fixtures[0]

	assert.Equal(t, "fixtures/types/dbt-validation.yaml", f.File)
	assert.Equal(t, 1, f.Version)
	assert.Equal(t, "dbt-validation", f.Type)
	assert.Empty(t, f.Flow)
	assert.Empty(t, f.Scenario)
	node := remediation.TriggerNode{
		NodeID: "model.sales.orders", Category: "validation", ErrorSignature: "sig-1", Reason: "missing_column",
		ErrorExcerpt: "column amount does not exist", DBTLogURI: "s3://log", CandidateArtifactURI: "s3://cand",
		FilePath: "models/orders.sql", Service: "sales", NodeType: "dbt-model",
		ChangedAncestors: []remediation.ChangedAncestor{{NodeID: "model.sales.base", FilePath: "models/base.sql", Service: "sales", Depth: 1}},
	}
	assert.Equal(t, remediation.Trigger{
		Source: "validation", ReleaseID: "rel-1", Round: 2, Repo: "o/repo", CommitSHA: "sha", CodeBundleURI: "s3://bundle",
		Nodes: []remediation.TriggerNode{node},
	}, f.Trigger)
	assert.Equal(t, map[string]string{"sales": "services/sales"}, f.ServiceRepoPaths)
	assert.Equal(t, map[string]string{"services/sales/models/orders.sql": "select 1"}, f.RepoFiles)
	assert.Equal(t, map[string]string{"s3://cand": "select 0"}, f.Objects)
	assert.Equal(t, map[string]ports.CandidateSource{"model.sales.orders": {Runtime: "dbt", RawCode: "select 0"}}, f.Bundle)
	assert.Equal(t, conformance.Catalog{
		Locations:       map[string]conformance.Location{"model.sales.base": {FilePath: "models/base.sql", Service: "sales"}},
		CurrentVersions: map[string]ports.CurrentVersion{"model.sales.orders": {RawCode: "select 9", ContentHash: "h1", PromotedAt: "2026-11-01T00:00:00Z"}},
		UpstreamChanges: map[string][]prompt.UpstreamChange{"model.sales.orders": {{NodeID: "model.sales.base", Depth: 1, CodeDiff: "-a\n+b"}}},
		Precedents:      []prompt.Precedent{{NodeID: "model.sales.other", Category: "validation", Reason: "missing_column", Resolved: true}},
	}, f.Catalog)
	assert.Equal(t, conformance.Release{ImageTag: "sha-abc", FailingNodes: map[string]string{"model.sales.orders": "boom"}}, f.Release)
	assert.Equal(t, conformance.Packager{Merged: "merged: contract"}, f.Packager)
	assert.Equal(t, conformance.Answers{
		Pass: []ports.ProposeResult{
			{ProposedSQL: "select 1", Rationale: "diagnosis", Confidence: "high", Model: "m"},
			{ProposedSQL: "select 2", Confidence: "high"},
		},
		Fail:    []ports.ProposeResult{{}},
		Decline: []ports.ProposeResult{{Files: []ports.ProposedFile{{Path: "a.yml", Content: "x"}}, Confidence: "low"}},
	}, f.Answers)
	assert.Equal(t, map[string]conformance.Answers{
		"model.sales.base": {Pass: []ports.ProposeResult{{TargetFile: "t.sql", ProposedContent: "c", SuspectedRootCauseNode: "n"}}},
	}, f.MemberAnswers)
	assert.Equal(t, map[string]string{"model.sales.orders": "still failing"}, f.GateErrors)

	require.NotNil(t, f.DeclineBeforeModel)
	patched := node
	patched.CandidateArtifactURI = ""
	patched.Service = "other"
	assert.Equal(t, []remediation.TriggerNode{patched}, f.DeclineBeforeModel.Nodes)
	assert.Equal(t, "rel-1", f.DeclineBeforeModel.ReleaseID)
	assert.Equal(t, "s3://cand", f.Trigger.Nodes[0].CandidateArtifactURI, "the patch leaves the trigger itself alone")
}

func TestLoadFixtures_ReadsAFlowFixtureAndDefaultsTheRound(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte(`kind: fixture
version: 1
flow: rejected-release
scenario: maintenance
trigger: { source: compile, release_id: rel-1, nodes: [{ node_id: svc }] }
`)}}

	fixtures, err := LoadFixtures(fsys)
	require.NoError(t, err)
	require.Len(t, fixtures, 1)
	assert.Equal(t, "rejected-release", fixtures[0].Flow)
	assert.Equal(t, conformance.FlowMaintenance, fixtures[0].Scenario)
	assert.Equal(t, 1, fixtures[0].Trigger.Round)
	assert.Nil(t, fixtures[0].DeclineBeforeModel)
}

func TestLoadFixtures_SkipsFlowsAndTypesAndLoadSkipsFixtures(t *testing.T) {
	fsys := fstest.MapFS{
		"flows/rejected-release.yaml":   {Data: []byte(flowYAML)},
		"types/dbt-compile.yaml":        {Data: []byte(typeYAML)},
		"fixtures/types/dbt-valid.yaml": {Data: []byte(typeFixtureYAML)},
		"fixtures/several.yaml": {Data: []byte(typeFixtureYAML + "---\n" + `kind: fixture
version: 1
type: dbt-compile
trigger: { nodes: [{ node_id: svc }] }
`)},
	}

	fixtures, err := LoadFixtures(fsys)
	require.NoError(t, err)
	require.Len(t, fixtures, 3)
	assert.Equal(t, "fixtures/several.yaml", fixtures[0].File)
	assert.Equal(t, "dbt-validation", fixtures[0].Type)
	assert.Equal(t, "dbt-compile", fixtures[1].Type)
	assert.Equal(t, "fixtures/types/dbt-valid.yaml", fixtures[2].File)

	set, err := Load(fsys)
	require.NoError(t, err)
	assert.Len(t, set.Flows, 1)
	assert.Len(t, set.Types, 1)
}

func TestLoadFixtures_NamesTheFileAndLineOfEachProblem(t *testing.T) {
	cases := map[string]struct {
		yaml string
		want []string
	}{
		"an unknown field": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\nanswer: {}\n",
			want: []string{`f.yaml:5: field "answer" is not known here`},
		},
		"an unknown field deep inside": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n      nodetype: dbt-model\n",
			want: []string{`f.yaml:7: field "nodetype" is not known here`},
		},
		"an unknown field in an answer": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\nanswers:\n  pass:\n    - { sql: x }\n",
			want: []string{`f.yaml:7: field "sql" is not known here`},
		},
		"an unknown field in the patch": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\ndecline_before_model:\n  node: { candidate_uri: \"\" }\n",
			want: []string{`f.yaml:6: field "candidate_uri" is not known here`},
		},
		"an alias": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: &t { nodes: [{ node_id: a }] }\nrepo_files: *t\n",
			want: []string{`f.yaml:5: YAML aliases are not supported`},
		},
		"an unsupported version": {
			yaml: "kind: fixture\nversion: 2\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\n",
			want: []string{`f.yaml:2: unsupported version 2`},
		},
		"a type and a flow": {
			yaml: "kind: fixture\nversion: 1\ntype: x\nflow: f\nscenario: s\ntrigger: { nodes: [{ node_id: a }] }\n",
			want: []string{`f.yaml:1: a fixture names either the type it tests or a flow and a scenario, not both`},
		},
		"neither a type nor a flow": {
			yaml: "kind: fixture\nversion: 1\ntrigger: { nodes: [{ node_id: a }] }\n",
			want: []string{`f.yaml:1: a fixture names the type it tests, or a flow and a scenario`},
		},
		"a flow with no scenario": {
			yaml: "kind: fixture\nversion: 1\nflow: f\ntrigger: { nodes: [{ node_id: a }] }\n",
			want: []string{`f.yaml:3: a flow fixture names the scenario it plays`},
		},
		"no failing node": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { source: compile }\n",
			want: []string{`f.yaml:4: the fixture's trigger has no failing node`},
		},
		"a node with no id": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n    - category: compile\n",
			want: []string{`f.yaml:7: a failing node of the trigger has no node_id`},
		},
		"a value of the wrong type": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\nrepo_files: [a, b]\n",
			want: []string{`f.yaml:5:`},
		},
		"a patch that is not a mapping": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\ndecline_before_model: {}\n",
			want: []string{`f.yaml:5: decline_before_model.node must be a mapping`},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fixtures, err := LoadFixtures(fstest.MapFS{"f.yaml": {Data: []byte(tc.yaml)}})
			require.Error(t, err)
			assert.Nil(t, fixtures)
			for _, w := range tc.want {
				assert.Contains(t, err.Error(), w)
			}
		})
	}
}

func TestLoadFixtures_ReportsEveryBadFixture(t *testing.T) {
	fsys := fstest.MapFS{
		"a.yaml": {Data: []byte("kind: fixture\nversion: 2\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\n")},
		"b.yaml": {Data: []byte("kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\nbogus: 1\n")},
	}

	_, err := LoadFixtures(fsys)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "a.yaml:2:")
	assert.Contains(t, err.Error(), "b.yaml:5:")
}
