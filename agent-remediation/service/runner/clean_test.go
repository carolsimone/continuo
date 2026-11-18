package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/sanitize"
)

// rawEvidence holds a value a rule redacts at every depth: in a struct, a
// slice, a map key and value, a pointer, a named text type and valid UTF-8
// bytes.
func rawEvidence() remediation.Evidence {
	return remediation.NewEvidence().
		With(remediation.KeyFailure, remediation.Failure{
			Target: remediation.TriggerNode{
				NodeID:       "model.a",
				ErrorExcerpt: "connect failed for ops@acme.com",
				Unfixable:    &remediation.Unfixable{Reason: pkg_model.HandoverReasonClassifierInfra, Detail: "password=hunter2"},
			},
		}).
		With(remediation.KeySourceFiles, remediation.SourceFiles{
			Files:   map[string]string{"models/dev@acme.com/a.sql": "select 'dev@acme.com'", "a.sql": "select 'ops@acme.com'"},
			Order:   []string{"a.sql", "models/dev@acme.com/a.sql"},
			Primary: "a.sql",
		}).
		With(remediation.KeyFailurePrecedents, []prompt.Precedent{{
			ErrorExcerpt: "token: plain-value-1",
			Edited:       []prompt.EditedPrecedent{{Path: "b.sql", Diff: "+ select 'ops@acme.com'"}},
		}}).
		With(remediation.KeyDBTLog, remediation.Text("login as root@db.internal")).
		With(remediation.KeyPackagedContract, remediation.Bytes(`{"owner":"ops@acme.com"}`))
}

func TestCleanEvidence_CleansEveryNestedString(t *testing.T) {
	raw := rawEvidence()

	got := cleanEvidence(raw, sanitize.NewRedactor())

	f, ok := remediation.Get[remediation.Failure](got, remediation.KeyFailure)
	require.True(t, ok, "a cleaned value keeps its type")
	assert.Equal(t, "connect failed for [redacted:email:2]", f.Target.ErrorExcerpt)
	require.NotNil(t, f.Target.Unfixable)
	assert.Equal(t, "password=[redacted:secret:1]", f.Target.Unfixable.Detail)
	assert.Equal(t, "model.a", f.Target.NodeID, "text no rule matches stays as it is")

	files, ok := remediation.Get[remediation.SourceFiles](got, remediation.KeySourceFiles)
	require.True(t, ok)
	assert.Equal(t, map[string]string{
		"a.sql":                           "select '[redacted:email:2]'",
		"models/[redacted:email:3]/a.sql": "select '[redacted:email:3]'",
	}, files.Files, "map keys and values are cleaned")
	assert.Equal(t, []string{"a.sql", "models/[redacted:email:3]/a.sql"}, files.Order)

	precedents, ok := remediation.Get[[]prompt.Precedent](got, remediation.KeyFailurePrecedents)
	require.True(t, ok)
	require.Len(t, precedents, 1)
	assert.Equal(t, "token: [redacted:secret:2]", precedents[0].ErrorExcerpt)
	assert.Equal(t, "+ select '[redacted:email:2]'", precedents[0].Edited[0].Diff, "the same value keeps its placeholder")

	log, ok := remediation.Get[remediation.Text](got, remediation.KeyDBTLog)
	require.True(t, ok)
	assert.Equal(t, remediation.Text("login as [redacted:email:1]"), log)

	contract, ok := remediation.Get[remediation.Bytes](got, remediation.KeyPackagedContract)
	require.True(t, ok)
	assert.Equal(t, `{"owner":"[redacted:email:2]"}`, string(contract))

	assert.Equal(t, rawEvidence(), raw, "the input is never changed")
}

func TestCleanEvidence_NumbersTheSameOnEveryRun(t *testing.T) {
	first := cleanEvidence(rawEvidence(), sanitize.NewRedactor())
	for range 50 {
		assert.Equal(t, first, cleanEvidence(rawEvidence(), sanitize.NewRedactor()),
			"keys and map entries are cleaned in sorted order, so the numbers never change")
	}
}

func TestCleanEvidence_LeavesBytesThatAreNotTextAsTheyAre(t *testing.T) {
	binary := remediation.Bytes{0xff, 0xfe, 'o', 'p', 's', '@', 'a', '.', 'c', 'o'}
	got := cleanEvidence(remediation.NewEvidence().With(remediation.KeyPackagedContract, binary), sanitize.NewRedactor())

	b, ok := remediation.Get[remediation.Bytes](got, remediation.KeyPackagedContract)
	require.True(t, ok)
	assert.Equal(t, binary, b)
}

func TestRestoreDraft_PutsBackEveryValueAndFindsWhatItNeverIssued(t *testing.T) {
	r := sanitize.NewRedactor()
	shown := r.Clean("select 'ops@acme.com'")
	require.Equal(t, "select '[redacted:email:1]'", shown)

	d, findings := restoreDraft(remediation.Draft{
		Rationale: "keep [redacted:email:1] and drop [redacted:email:7]",
		Edits: []remediation.Edit{{
			Path:     "models/a.sql",
			Original: shown,
			Content:  "select '[redacted:email:1]', '[REDACTED:email:1]', '[redacted:email:1",
		}},
	}, r)

	assert.Equal(t, "select 'ops@acme.com'", d.Edits[0].Original)
	assert.Equal(t, "select 'ops@acme.com', '[REDACTED:email:1]', '[redacted:email:1", d.Edits[0].Content)
	assert.Equal(t, "keep ops@acme.com and drop [redacted:email:7]", d.Rationale)
	assert.Equal(t, []remediation.Finding{
		{Code: findingUnknownPlaceholder, Message: "the answer holds [REDACTED:email:1], which was never in what you were shown; keep the text you were shown", File: "models/a.sql"},
		{Code: findingUnknownPlaceholder, Message: "the answer holds [redacted:email:1, which was never in what you were shown; keep the text you were shown", File: "models/a.sql"},
		{Code: findingUnknownPlaceholder, Message: "the answer holds [redacted:email:7], which was never in what you were shown; keep the text you were shown"},
	}, findings)
}
