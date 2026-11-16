package workflowfile

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
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
  revision:
    pass: [{ proposed_sql: "select 3", confidence: high }]
    fail: [{ proposed_sql: "selec 3" }]
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
		Revision: conformance.RoundAnswers{
			Pass: []ports.ProposeResult{{ProposedSQL: "select 3", Confidence: "high"}},
			Fail: []ports.ProposeResult{{ProposedSQL: "selec 3"}},
		},
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

// A failing node the classifier marked unfixable carries its reason and detail
// into the trigger, whichever of the two classifier reasons it names, and a
// node with no mark stays fixable.
func TestLoadFixtures_ReadsAnUnfixableNode(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte(`kind: fixture
version: 1
flow: rejected-release
scenario: unfixable
trigger:
  source: validation
  release_id: rel-1
  nodes:
    - node_id: infra
      unfixable: { reason: classifier_infra, detail: "could not connect to server: connection refused" }
    - node_id: parse
      unfixable:
        reason: classifier_unhealable_parse
        detail: the manifest cannot be read
    - node_id: fixable
`)}}

	fixtures, err := LoadFixtures(fsys)

	require.NoError(t, err)
	require.Len(t, fixtures, 1)
	assert.Equal(t, conformance.FlowUnfixable, fixtures[0].Scenario)
	assert.Equal(t, []remediation.TriggerNode{
		{NodeID: "infra", Unfixable: &remediation.Unfixable{Reason: model.HandoverReasonClassifierInfra, Detail: "could not connect to server: connection refused"}},
		{NodeID: "parse", Unfixable: &remediation.Unfixable{Reason: model.HandoverReasonClassifierUnhealableParse, Detail: "the manifest cannot be read"}},
		{NodeID: "fixable"},
	}, fixtures[0].Trigger.Nodes)
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
		"a revision inside a revision": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\nanswers:\n  revision:\n    revision: {}\n",
			want: []string{`f.yaml:7: field "revision" is not known here`},
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
		"an unknown unfixable reason": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n      unfixable:\n        reason: cosmic_rays\n        detail: d\n",
			want: []string{`f.yaml:8: unfixable reason "cosmic_rays" is not one a classifier gives (classifier_infra, classifier_unhealable_parse)`},
		},
		"an unfixable reason that is not a classifier's": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n      unfixable: { reason: tries_spent, detail: d }\n",
			want: []string{`f.yaml:7: unfixable reason "tries_spent" is not one a classifier gives`},
		},
		"an unfixable mark with no reason": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n      unfixable: { detail: d }\n",
			want: []string{`f.yaml:7: unfixable reason "" is not one a classifier gives`},
		},
		"an unknown field of the unfixable mark": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger:\n  nodes:\n    - node_id: a\n      unfixable:\n        reason: classifier_infra\n        why: d\n",
			want: []string{`f.yaml:9: field "why" is not known here (allowed: reason, detail)`},
		},
		"an unknown unfixable reason in the patch": {
			yaml: "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: a }] }\ndecline_before_model:\n  node:\n    unfixable: { reason: cosmic_rays, detail: d }\n",
			want: []string{`f.yaml:7: unfixable reason "cosmic_rays" is not one a classifier gives`},
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

// filled sets every field of v to a value no zero value equals: a string to
// its field's JSON name, an int to 7, a slice to one filled element, a pointer
// to a filled value.
func filled(v reflect.Value, name string) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(name)
	case reflect.Int:
		v.SetInt(7)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		filled(v.Index(0), name)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		filled(v.Elem(), name)
	case reflect.Struct:
		for i := range v.NumField() {
			tag, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("json"), ",")
			filled(v.Field(i), tag)
		}
	default:
		panic("filled: unhandled kind " + v.Kind().String())
	}
}

// A fixture's trigger takes every field of the remediation.requested payload a
// trigger carries, under its payload name, and keeps each value: a field the
// payload gains must be added to the fixture format too, or this fails. A
// trigger carries no classification time, so that payload field is left out. A
// node's unfixable mark takes a reason a classifier gives, the one value the
// fixture format accepts.
func TestLoadFixtures_TheTriggerTakesEveryFieldOfThePayload(t *testing.T) {
	var p events.RemediationRequested
	filled(reflect.ValueOf(&p).Elem(), "")
	require.NotEmpty(t, p.Nodes)
	for i := range p.Nodes {
		require.NotNil(t, p.Nodes[i].Unfixable, "the filled payload marks its nodes unfixable")
		p.Nodes[i].Unfixable.Reason = string(model.HandoverReasonClassifierInfra)
	}
	var want map[string]any
	filledJSON, err := json.Marshal(p)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(filledJSON, &want))
	delete(want, "classified_at")
	payload, err := json.Marshal(want)
	require.NoError(t, err)
	doc := "kind: fixture\nversion: 1\ntype: x\ntrigger: " + string(payload) + "\n"

	fixtures, err := LoadFixtures(fstest.MapFS{"f.yaml": {Data: []byte(doc)}})
	require.NoError(t, err)
	require.Len(t, fixtures, 1)
	encoded, err := serialization.EncodeTrigger(fixtures[0].Trigger)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(encoded, &got))
	assert.Equal(t, "", got["classified_at"], "a trigger has no classification time")
	delete(got, "classified_at")
	assert.Equal(t, want, got)
}

// Problems are reported in the order the file holds them, never in the order
// a decoded map would hand them over in.
func TestLoadFixtures_ReportsProblemsInFileOrder(t *testing.T) {
	for _, nodes := range [][2]string{{"z.node", "a.node"}, {"a.node", "z.node"}} {
		doc := "kind: fixture\nversion: 1\ntype: x\ntrigger: { nodes: [{ node_id: n }] }\nbundle:\n" +
			"  " + nodes[0] + ": { runtime: dbt, code: x }\n" +
			"  " + nodes[1] + ": { runtime: dbt, source: y }\n" +
			"member_answers:\n" +
			"  " + nodes[0] + ": { pass: [{ sql: x }] }\n" +
			"  " + nodes[1] + ": { passes: [] }\n"
		for range 20 {
			_, err := LoadFixtures(fstest.MapFS{"f.yaml": {Data: []byte(doc)}})

			require.Error(t, err)
			lines := strings.Split(err.Error(), "\n")
			require.Len(t, lines, 4)
			assert.True(t, strings.HasPrefix(lines[0], `f.yaml:6: field "code"`), lines[0])
			assert.True(t, strings.HasPrefix(lines[1], `f.yaml:7: field "source"`), lines[1])
			assert.True(t, strings.HasPrefix(lines[2], `f.yaml:9: field "sql"`), lines[2])
			assert.True(t, strings.HasPrefix(lines[3], `f.yaml:10: field "passes"`), lines[3])
		}
	}
}
