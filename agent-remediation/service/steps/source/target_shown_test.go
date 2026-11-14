package source_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/source"
)

func TestTargetFileWasShown_MetaIsAPreflightCheckOnTheDraftAndTheSourceFiles(t *testing.T) {
	m := source.NewTargetFileWasShown().Meta()
	assert.Equal(t, "target_file_was_shown", string(m.Name))
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeySourceFiles}, m.Needs)
	assert.Empty(t, m.Provides)
	assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages)
}

func TestTargetFileWasShown(t *testing.T) {
	shown := remediation.SourceFiles{
		Files: map[string]string{
			"services/svc/models/x.sql":    "select 1",
			"services/svc/models/y.sql":    "select 2",
			"services/svc/dbt_project.yml": "name: svc",
			"services/svc/staging/y.sql":   "select 3",
		},
		Order: []string{
			"services/svc/models/x.sql",
			"services/svc/models/y.sql",
			"services/svc/dbt_project.yml",
			"services/svc/staging/y.sql",
		},
		Primary: "services/svc/models/x.sql",
	}
	const msgFmt = "the model named %q as the file to change, which is not one of the files it was shown"
	cases := []struct {
		name   string
		target string
		files  remediation.SourceFiles
		fails  bool
	}{
		{"an exact path passes", "services/svc/models/x.sql", shown, false},
		{"a suffix spelling passes", "models/x.sql", shown, false},
		{"a bare file name that is unique passes", "x.sql", shown, false},
		{"an ambiguous suffix fails", "y.sql", shown, true},
		{"an unknown path fails", "models/z.sql", shown, true},
		{"an empty target fails when several files were shown", "", shown, true},
		{"an empty target passes when one file was shown", "",
			remediation.SourceFiles{Files: map[string]string{"a.sql": "x"}, Order: []string{"a.sql"}, Primary: "a.sql"}, false},
		{"a suffix that is not on a path boundary fails", "ls/x.sql", shown, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := remediation.NewEvidence().With(remediation.KeySourceFiles, c.files)
			out, err := source.NewTargetFileWasShown().Check(context.Background(), step.CheckInput{
				Evidence: ev,
				Draft:    remediation.Draft{NamedTarget: c.target},
			})
			require.NoError(t, err)
			assert.Equal(t, "target_file_was_shown", out.Result.Check)
			if !c.fails {
				assert.True(t, out.Result.Passed())
				return
			}
			require.Len(t, out.Result.Findings, 1)
			assert.Equal(t, "target_not_shown", out.Result.Findings[0].Code)
			assert.Equal(t, fmt.Sprintf(msgFmt, c.target), out.Result.Findings[0].Message)
		})
	}
}

func TestTargetFileWasShown_JudgesTheNamedTargetNotTheDraftsOwnFlag(t *testing.T) {
	files := remediation.SourceFiles{Files: map[string]string{"a/x.sql": "x", "b/x.sql": "x"}, Order: []string{"a/x.sql", "b/x.sql"}, Primary: "a/x.sql"}
	out, err := source.NewTargetFileWasShown().Check(context.Background(), step.CheckInput{
		Evidence: remediation.NewEvidence().With(remediation.KeySourceFiles, files),
		Draft:    remediation.Draft{NamedTarget: "x.sql", TargetShown: true},
	})
	require.NoError(t, err)
	assert.False(t, out.Result.Passed())
}

func TestTargetFileWasShown_WithoutSourceFilesIsAnError(t *testing.T) {
	_, err := source.NewTargetFileWasShown().Check(context.Background(), step.CheckInput{
		Evidence: remediation.NewEvidence(),
		Draft:    remediation.Draft{NamedTarget: "a.sql"},
	})
	assert.Error(t, err)
}
