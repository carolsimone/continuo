package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// A copy of a fixture shares no map, slice or pointer with it: a change made
// anywhere in the copy leaves the fixture as it was.
func TestFixture_CloneSharesNothingWithTheOriginal(t *testing.T) {
	decline := remediation.Trigger{Nodes: []remediation.TriggerNode{{NodeID: "a", FilePath: "models/a.sql"}}}
	f := Fixture{
		Trigger: remediation.Trigger{Nodes: []remediation.TriggerNode{{
			NodeID:           "a",
			ChangedAncestors: []remediation.ChangedAncestor{{NodeID: "b"}},
			Unfixable:        &remediation.Unfixable{Reason: model.HandoverReasonClassifierInfra, Detail: "down"},
		}}},
		DeclineBeforeModel: &decline,
		ServiceRepoPaths:   map[string]string{"svc": "services/svc"},
		RepoFiles:          map[string]string{"services/svc/a.sql": "select 1"},
		Objects:            map[string]string{"s3://logs/a.log": "log"},
		Bundle:             map[string]ports.CandidateSource{"a": {RawCode: "select 1", Runtime: "dbt"}},
		Catalog: Catalog{
			UpstreamChanges: map[string][]prompt.UpstreamChange{"a": {{NodeID: "b", CodeDiff: "-x"}}},
			Precedents:      []prompt.Precedent{{NodeID: "c"}},
		},
		Release: Release{FailingNodes: map[string]string{"a": "boom"}},
		Answers: Answers{
			Pass:     []ports.ProposeResult{{ProposedSQL: "select 2", Files: []ports.ProposedFile{{Path: "p", Content: "c"}}}},
			Revision: RoundAnswers{Fail: []ports.ProposeResult{{ProposedSQL: "select 3"}}},
		},
		MemberAnswers: map[string]Answers{"a": {Pass: []ports.ProposeResult{{ProposedSQL: "select 4"}}}},
		GateErrors:    map[string]string{"a": "gate"},
	}

	c := f.clone()
	c.Trigger.Nodes[0].NodeID = "changed"
	c.Trigger.Nodes[0].ChangedAncestors[0].NodeID = "changed"
	c.Trigger.Nodes[0].Unfixable.Detail = "changed"
	c.DeclineBeforeModel.Nodes[0].FilePath = "changed"
	c.ServiceRepoPaths["svc"] = "changed"
	c.RepoFiles["services/svc/a.sql"] = "changed"
	c.Objects["s3://logs/a.log"] = "changed"
	c.Bundle["a"] = ports.CandidateSource{RawCode: "changed"}
	c.Catalog.UpstreamChanges["a"][0].CodeDiff = "changed"
	c.Catalog.Precedents[0].NodeID = "changed"
	c.Release.FailingNodes["a"] = "changed"
	c.Answers.Pass[0].ProposedSQL = "changed"
	c.Answers.Pass[0].Files[0].Content = "changed"
	c.Answers.Revision.Fail[0].ProposedSQL = "changed"
	c.MemberAnswers["a"].Pass[0].ProposedSQL = "changed"
	c.GateErrors["a"] = "changed"

	assert.Equal(t, "a", f.Trigger.Nodes[0].NodeID)
	assert.Equal(t, "b", f.Trigger.Nodes[0].ChangedAncestors[0].NodeID)
	assert.Equal(t, "down", f.Trigger.Nodes[0].Unfixable.Detail)
	assert.Equal(t, "models/a.sql", f.DeclineBeforeModel.Nodes[0].FilePath)
	assert.Equal(t, "services/svc", f.ServiceRepoPaths["svc"])
	assert.Equal(t, "select 1", f.RepoFiles["services/svc/a.sql"])
	assert.Equal(t, "log", f.Objects["s3://logs/a.log"])
	assert.Equal(t, "select 1", f.Bundle["a"].RawCode)
	assert.Equal(t, "-x", f.Catalog.UpstreamChanges["a"][0].CodeDiff)
	assert.Equal(t, "c", f.Catalog.Precedents[0].NodeID)
	assert.Equal(t, "boom", f.Release.FailingNodes["a"])
	assert.Equal(t, "select 2", f.Answers.Pass[0].ProposedSQL)
	assert.Equal(t, "c", f.Answers.Pass[0].Files[0].Content)
	assert.Equal(t, "select 3", f.Answers.Revision.Fail[0].ProposedSQL)
	assert.Equal(t, "select 4", f.MemberAnswers["a"].Pass[0].ProposedSQL)
	assert.Equal(t, "gate", f.GateErrors["a"])
}

// A nil map, slice or pointer stays nil in the copy, so a fixture that has no
// decline patch has none after it.
func TestFixture_CloneKeepsWhatIsAbsentAbsent(t *testing.T) {
	c := Fixture{File: "f.yaml"}.clone()

	assert.Equal(t, "f.yaml", c.File)
	assert.Nil(t, c.DeclineBeforeModel)
	assert.Nil(t, c.RepoFiles)
	assert.Nil(t, c.Answers.Pass)
	assert.Nil(t, c.Trigger.Nodes)
}
