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

func TestRestoreDraft_PutsBackTheValuesOfTheCodeAndRefusesTheRest(t *testing.T) {
	r := sanitize.NewRedactor()
	shown := r.Clean("select 'ops@acme.com'")
	require.Equal(t, "select '[redacted:email:1]'", shown)
	logged := r.Clean("login failed: password=hunter2")
	require.Equal(t, "login failed: password=[redacted:secret:1]", logged)

	d, findings := restoreDraft(remediation.Draft{
		Rationale: "keep [redacted:email:1], drop [redacted:email:7] and [REDACTED:email:3], the log said [redacted:secret:1]",
		Edits: []remediation.Edit{{
			Path:     "models/a.sql",
			Original: shown,
			Content:  "select '[redacted:email:1]', '[redacted:secret:1]', '[redacted:email:7]'",
		}},
	}, r, []string{"select 'ops@acme.com'"})

	assert.Equal(t, "select 'ops@acme.com'", d.Edits[0].Original)
	assert.Equal(t, "select 'ops@acme.com', '[redacted:secret:1]', '[redacted:email:7]'", d.Edits[0].Content,
		"a value seen only in a log stays a placeholder, and one never issued is left for markerFindings")
	assert.Equal(t, "keep ops@acme.com, drop [redacted:email:7] and [REDACTED:email:3], the log said [redacted:secret:1]", d.Rationale,
		"the rationale gets back only values of the code, and a log's value keeps its placeholder with no finding")
	assert.Equal(t, []remediation.Finding{
		{Code: findingUnknownPlaceholder, Message: "the answer holds a placeholder (redacted secret 1) for a value from a log or a check, not from the code; leave that value out of the file", File: "models/a.sql"},
		{Code: findingUnknownPlaceholder, Message: "the answer holds a placeholder (redacted email 7) that was never in what you were shown; keep the text you were shown"},
	}, findings, "in the rationale only a numbered placeholder never issued is a finding")
}

func TestRestoreDraft_AValueInTheEditsOriginalIsRestored(t *testing.T) {
	r := sanitize.NewRedactor()
	original := r.Clean("select 'ops@acme.com'")

	d, findings := restoreDraft(remediation.Draft{Edits: []remediation.Edit{{Path: "a.sql", Original: original, Content: original + ", 2"}}}, r, nil)

	assert.Equal(t, "select 'ops@acme.com', 2", d.Edits[0].Content)
	assert.Empty(t, findings)
}

func TestMarkerFindings_FindsOnlyTheMarkersAnEditAdds(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("password=hunter2")
	cases := []struct {
		name, original, content string
		want                    []string
	}{
		{"a literal marker the file holds", "select '[REDACTED]'", "select '[REDACTED]', 2", nil},
		{"a fixed marker the file holds", "pw: '[redacted:secret]'", "pw: '[redacted:secret]'\nx: 1", nil},
		{"one more literal marker", "select '[REDACTED]'", "select '[REDACTED]', '[REDACTED]'", []string{"REDACTED"}},
		{"one more fixed marker", "pw: '[redacted:secret]'", "pw: '[redacted:secret]'\nx: '[redacted:secret]'", []string{"redacted secret"}},
		{"a number never issued", "select 1", "select '[redacted:email:9]'", []string{"redacted email 9"}},
		{"a marker in a new file", "", "x: '[redacted:email]'", []string{"redacted email"}},
		{"a marker without its bracket", "select 1", "select '[redacted:email:1 as x", []string{"redacted email 1"}},
		{"an issued placeholder left by restoreDraft", "select 1", "select '[redacted:secret:1]'", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := markerFindings(remediation.Draft{Edits: []remediation.Edit{{Path: "a.sql", Original: tc.original, Content: tc.content}}}, r)

			var names []string
			for _, f := range got {
				assert.Equal(t, findingUnknownPlaceholder, f.Code)
				assert.Equal(t, "a.sql", f.File)
				names = append(names, f.Message)
			}
			var want []string
			for _, n := range tc.want {
				want = append(want, "the answer adds a redaction marker ("+n+") the file did not hold; keep the text you were shown")
			}
			assert.Equal(t, want, names)
		})
	}
}

func TestUnknownFinding_IsNeverChangedWhenCleanedForTheNextRound(t *testing.T) {
	for _, token := range []string{"[redacted:secret:1]", "[redacted:email:9]", "[redacted:key:2]", "[redacted:secret]", "[REDACTED]", "[REDACTED:email:1]", "[redacted:email:1"} {
		for _, message := range []string{placeholderAdded, placeholderFromALog, placeholderNeverShown} {
			f := unknownFinding(message, token, "a.sql")
			assert.Equal(t, f.Message, sanitize.NewRedactor().Clean(f.Message), "%s in %q", token, message)
			assert.Equal(t, f.Message, sanitize.Text(f.Message))
		}
	}
}

func TestCodeTexts_ReadsOnlyTheCodeClass(t *testing.T) {
	ev := remediation.NewEvidence().
		With(remediation.KeySourceFiles, remediation.SourceFiles{Files: map[string]string{"a.sql": "select 1"}, Order: []string{"a.sql"}}).
		With(remediation.KeyDBTLog, remediation.Text("the log"))

	got := codeTexts(ev, NewRegistry(Collaborators{}).EvidenceClass)

	assert.Contains(t, got, "select 1")
	assert.NotContains(t, got, "the log")
}

func TestRestoreDraft_AValueIsShownOnlyAsAWholeToken(t *testing.T) {
	cases := []struct {
		name, code, want string
		refused          bool
	}{
		{"a code text that only contains the value", "select administrator from users", "select '[redacted:secret:1]'", true},
		{"a code text that holds the value as a token", "where user = 'admin'", "select 'admin'", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := sanitize.NewRedactor()
			require.Equal(t, "login failed: password=[redacted:secret:1]", r.Clean("login failed: password=admin"))

			d, findings := restoreDraft(remediation.Draft{
				Edits: []remediation.Edit{{Path: "a.sql", Content: "select '[redacted:secret:1]'"}},
			}, r, []string{tc.code})

			assert.Equal(t, tc.want, d.Edits[0].Content)
			assert.Equal(t, tc.refused, len(findings) == 1)
		})
	}
}

// A value that occurs as a whole token in the code is restored even when the
// model saw it in a log: a short value such as 1 occurs in most code.
func TestRestoreDraft_AValueSeenInALogThatIsAlsoATokenOfTheCodeIsRestored(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "password=[redacted:secret:1]", r.Clean("password=1"))

	d, findings := restoreDraft(remediation.Draft{
		Rationale: "the log said [redacted:secret:1]",
		Edits:     []remediation.Edit{{Path: "a.sql", Content: "select [redacted:secret:1]"}},
	}, r, []string{"select 1"})

	assert.Equal(t, "select 1", d.Edits[0].Content)
	assert.Equal(t, "the log said 1", d.Rationale)
	assert.Empty(t, findings)
}

func TestRestoreDraft_APathGetsBackOnlyValuesOfTheCode(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("login failed: password=hunter2")
	shown := r.Clean("owner: ops@acme.com")

	d, findings := restoreDraft(remediation.Draft{Edits: []remediation.Edit{
		{Path: "contracts/[redacted:secret:1].odcs.yaml", Content: "x: 1"},
		{Path: "contracts/[redacted:email:1].odcs.yaml", Content: "y: 2"},
	}}, r, []string{"owner: ops@acme.com"})
	require.Equal(t, "owner: [redacted:email:1]", shown)

	assert.Equal(t, "contracts/[redacted:secret:1].odcs.yaml", d.Edits[0].Path, "a log's value never names a file")
	assert.Equal(t, "contracts/ops@acme.com.odcs.yaml", d.Edits[1].Path)
	assert.Equal(t, []remediation.Finding{{
		Code:    findingUnknownPlaceholder,
		Message: "the answer names a file with a placeholder (redacted secret 1) for a value from a log or a check, not from the code; leave that value out of the path",
	}}, findings)
}

func TestShownIn_MatchesAWholeToken(t *testing.T) {
	cases := []struct {
		text, value string
		want        bool
	}{
		{"user = 'admin'", "admin", true},
		{"administrator", "admin", false},
		{"sysadmin", "admin", false},
		{"admin_user", "admin", false},
		{"admin", "admin", true},
		{"administrator, then admin", "admin", true},
		{"owner ops@acme.com.", "ops@acme.com", true},
		{"xops@acme.com", "ops@acme.com", false},
		{"a[redacted:secret:1]b", "[redacted:secret:1]", true},
		{"anything", "", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, shownIn(tc.text, tc.value), "%q in %q", tc.value, tc.text)
	}
}
