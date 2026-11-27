package step_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestWithPreviousTries_ShowsEachRecordedTry(t *testing.T) {
	req := prompt.ProposeRequest{System: "s", User: "Failed node: model.a"}
	ev := remediation.NewEvidence().With(remediation.KeyPreviousTries, []remediation.TryRecord{
		{
			Round: 1, Try: 0, Attempt: 1,
			Clusters: []remediation.ClusterRecord{{Members: []string{"model.a"}, Edits: []remediation.EditRecord{{Path: "models/a.sql", Diff: "-a\n+b"}}}},
			Gate:     []remediation.Finding{{Message: "model.a: column b does not exist"}},
		},
		{
			Round: 2, Try: 1, Attempt: 2,
			Clusters: []remediation.ClusterRecord{{
				Members: []string{"model.a"},
				Edits:   []remediation.EditRecord{{Path: "models/a.sql", Diff: "-a\n+c"}},
				Rounds: [][]remediation.Finding{
					{{Code: "content_unchanged", Message: "the answer repeats the original"}},
					{},
				},
				Outcome: "failed (preflight_rounds_spent): the answer repeats the original",
			}},
			CrossCluster: []remediation.Finding{{Code: "path_edited_twice", Message: "models/a.sql is edited by two clusters", File: "models/a.sql"}},
			Gate: []remediation.Finding{
				{Code: "release_check_failed", Message: "model.a: still broken"},
				{Code: "release_check_failed", Message: "model.b: broken too"},
			},
		},
	})

	got := step.WithPreviousTries(req, ev)

	assert.Equal(t, prompt.WithPreviousTries(req, []prompt.PreviousTry{
		{
			Label:     "Attempt 1 (round 1)",
			Diffs:     []prompt.AttemptDiff{{Path: "models/a.sql", Diff: "-a\n+b"}},
			GateError: "model.a: column b does not exist",
		},
		{
			Label: "Try 1 (round 2)",
			Diffs: []prompt.AttemptDiff{{Path: "models/a.sql", Diff: "-a\n+c"}},
			Findings: []string{
				"round 1: content_unchanged: the answer repeats the original",
				"outcome: failed (preflight_rounds_spent): the answer repeats the original",
				"across clusters: path_edited_twice: models/a.sql is edited by two clusters (models/a.sql)",
			},
			GateError: "model.a: still broken; model.b: broken too",
		},
	}), got)
}

func TestWithPreviousTries_NoRecordsLeavesTheRequestAsItIs(t *testing.T) {
	req := prompt.ProposeRequest{System: "s", User: "Failed node: model.a"}
	assert.Equal(t, req, step.WithPreviousTries(req, remediation.NewEvidence()))
	assert.Equal(t, req, step.WithPreviousTries(req, remediation.NewEvidence().With(remediation.KeyPreviousTries, []remediation.TryRecord{})))
}
