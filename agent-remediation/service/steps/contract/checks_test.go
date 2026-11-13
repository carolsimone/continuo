package contract_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

// seedArchive hands out a real directory per fetch holding the seeded files.
type seedArchive struct {
	t       *testing.T
	seed    map[string]string
	fetches int
	dirs    []string
}

func (a *seedArchive) Fetch(_ context.Context, _, _ string) (string, func(), error) {
	a.fetches++
	dir := a.t.TempDir()
	for p, c := range a.seed {
		full := filepath.Join(dir, filepath.FromSlash(p))
		require.NoError(a.t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(a.t, os.WriteFile(full, []byte(c), 0o600))
	}
	a.dirs = append(a.dirs, dir)
	return dir, func() { _ = os.RemoveAll(dir) }, nil
}

var _ ports.RepoArchive = (*seedArchive)(nil)

// fakeInspector reads declarations by the text of the yaml: a text it has no
// entry for declares nothing, and one in bad does not parse.
type fakeInspector struct {
	decls map[string][]ports.NodeDeclaration
	bad   map[string]error
}

func (f *fakeInspector) Declarations(text string) ([]ports.NodeDeclaration, error) {
	if err := f.bad[text]; err != nil {
		return nil, err
	}
	return f.decls[text], nil
}

var _ ports.ContractInspector = (*fakeInspector)(nil)

type mergeCall struct{ contractDir, repoRoot, service, dialect string }

type fakePackager struct {
	out   []byte
	err   error
	calls []mergeCall
}

func (f *fakePackager) Merge(_ context.Context, contractDir, repoRoot, service, dialect string) ([]byte, error) {
	f.calls = append(f.calls, mergeCall{contractDir, repoRoot, service, dialect})
	return f.out, f.err
}

var _ ports.ContractPackager = (*fakePackager)(nil)

const contractDir = "svc/contracts"

func decl(schema, table string, reads ...string) ports.NodeDeclaration {
	return ports.NodeDeclaration{
		Identity: ports.NodeIdentity{Schema: schema, Table: table, Script: "run.py", Kind: "python-node"},
		ReadKeys: reads,
	}
}

func withIdentity(d ports.NodeDeclaration, mod func(*ports.NodeIdentity)) ports.NodeDeclaration {
	mod(&d.Identity)
	return d
}

func edit(path, content string) remediation.Edit {
	return remediation.Edit{Path: path, Content: content, TargetNodeID: nodeID}
}

// pass is one pre-flight pass over a repository holding seed.
type pass struct {
	archive *seedArchive
	scratch *step.Scratch
}

func newPass(t *testing.T, seed map[string]string) pass {
	t.Helper()
	a := &seedArchive{t: t, seed: seed}
	s := step.NewScratch(a, repoName, commit)
	t.Cleanup(s.Close)
	return pass{archive: a, scratch: s}
}

func (p pass) input(edits ...remediation.Edit) step.CheckInput {
	return step.CheckInput{
		Failure:  failure(nil),
		Evidence: contractFileEvidence(),
		Draft:    remediation.Draft{Edits: edits},
		Scratch:  p.scratch,
	}
}

func runCheck(t *testing.T, c step.Check, in step.CheckInput) step.CheckOutput {
	t.Helper()
	out, err := c.Check(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, string(c.Meta().Name), out.Result.Check)
	return out
}

func findings(t *testing.T, c step.Check, in step.CheckInput) []remediation.Finding {
	t.Helper()
	return runCheck(t, c, in).Result.Findings
}

func TestChecks_MetaIsAPreflightCheckOnTheDraft(t *testing.T) {
	draft := []remediation.EvidenceKey{remediation.KeyDraft}
	withFile := []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeyContractFile}
	cases := []struct {
		name     string
		check    step.Check
		needs    []remediation.EvidenceKey
		provides []remediation.EvidenceKey
	}{
		{"contract_paths_inside_folder", contract.NewContractPathsInsideFolder(), withFile, nil},
		{"yaml_reads", contract.NewYAMLReads(nil), draft, nil},
		{"node_identity_unchanged", contract.NewNodeIdentityUnchanged(nil), draft, nil},
		{"reads_kept", contract.NewReadsKept(nil), draft, nil},
		{"csv_read_kept", contract.NewCSVReadKept(nil), draft, nil},
		{"no_new_secret_ref", contract.NewNoNewSecretRef(nil), draft, nil},
		{"declared_once", contract.NewDeclaredOnce(nil), withFile, nil},
		{"contract_packages", contract.NewContractPackages(nil, ""), withFile, []remediation.EvidenceKey{remediation.KeyPackagedContract}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := c.check.Meta()
			assert.Equal(t, c.name, string(m.Name))
			assert.Equal(t, step.KindCheck, m.Kind)
			assert.NotEmpty(t, m.Description)
			assert.Equal(t, c.needs, m.Needs)
			assert.Equal(t, c.provides, m.Provides)
			assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages)
		})
	}
}

// --- contract_paths_inside_folder ---

func TestContractPathsInsideFolder(t *testing.T) {
	const (
		noFiles = "the model returned no files to change"
		outside = "the model proposed a change to %q, which is outside the contract directory %q that declares this node"
		twice   = "the model returned %q more than once, so the content to record could not be told apart from the content to apply"
	)
	cases := []struct {
		name  string
		edits []remediation.Edit
		want  []remediation.Finding
	}{
		{"paths inside the directory pass", []remediation.Edit{
			edit("svc/contracts/fx.yaml", "a"), edit("svc/contracts/sub/other.yaml", "b"), edit("svc/contracts", "c"),
		}, nil},
		{"no files", nil, []remediation.Finding{{Code: "no_files", Message: noFiles}}},
		{"a path in another service", []remediation.Edit{edit("other/contracts/fx.yaml", "a")}, []remediation.Finding{{
			Code: "path_outside_contract_dir", File: "other/contracts/fx.yaml",
			Message: fmt.Sprintf(outside, "other/contracts/fx.yaml", contractDir),
		}}},
		{"a sibling directory with the same prefix", []remediation.Edit{edit("svc/contracts-old/fx.yaml", "a")}, []remediation.Finding{{
			Code: "path_outside_contract_dir", File: "svc/contracts-old/fx.yaml",
			Message: fmt.Sprintf(outside, "svc/contracts-old/fx.yaml", contractDir),
		}}},
		{"an upward traversal", []remediation.Edit{edit("svc/contracts/../../x.yaml", "a")}, []remediation.Finding{{
			Code: "path_outside_contract_dir", File: "svc/contracts/../../x.yaml",
			Message: fmt.Sprintf(outside, "svc/contracts/../../x.yaml", contractDir),
		}}},
		{"an absolute path", []remediation.Edit{edit("/svc/contracts/fx.yaml", "a")}, []remediation.Finding{{
			Code: "path_outside_contract_dir", File: "/svc/contracts/fx.yaml",
			Message: fmt.Sprintf(outside, "/svc/contracts/fx.yaml", contractDir),
		}}},
		{"an empty path", []remediation.Edit{edit("", "a")}, []remediation.Finding{{
			Code: "path_outside_contract_dir", Message: fmt.Sprintf(outside, "", contractDir),
		}}},
		{"a path twice", []remediation.Edit{edit("svc/contracts/fx.yaml", "a"), edit("svc/contracts/fx.yaml", "b")}, []remediation.Finding{{
			Code: "path_twice", File: "svc/contracts/fx.yaml", Message: fmt.Sprintf(twice, "svc/contracts/fx.yaml"),
		}}},
		{"two spellings of one path", []remediation.Edit{edit("svc/contracts/fx.yaml", "a"), edit("svc/contracts/./fx.yaml", "b")}, []remediation.Finding{{
			Code: "path_twice", File: "svc/contracts/./fx.yaml", Message: fmt.Sprintf(twice, "svc/contracts/./fx.yaml"),
		}}},
		{"every breach is found, in the order of the files", []remediation.Edit{
			edit("other/a.yaml", "a"), edit("svc/contracts/b.yaml", "b"), edit("svc/contracts/b.yaml", "c"),
		}, []remediation.Finding{
			{Code: "path_outside_contract_dir", File: "other/a.yaml", Message: fmt.Sprintf(outside, "other/a.yaml", contractDir)},
			{Code: "path_twice", File: "svc/contracts/b.yaml", Message: fmt.Sprintf(twice, "svc/contracts/b.yaml")},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newPass(t, nil)
			out := runCheck(t, contract.NewContractPathsInsideFolder(), p.input(c.edits...))
			assert.Equal(t, c.want, out.Result.Findings)
			assert.Equal(t, len(c.want) == 0, out.Result.Passed())
			assert.Zero(t, p.archive.fetches, "the path check reads no checkout")
		})
	}
}

func TestContractPathsInsideFolder_TheRepositoryRootContractDirectoryAcceptsAnyLocalPath(t *testing.T) {
	in := newPass(t, nil).input(edit("fx.yaml", "a"), edit("sub/dir/b.yaml", "b"))
	in.Evidence = remediation.NewEvidence().With(remediation.KeyContractFile, remediation.ContractFile{ContractDir: ".", RepoRoot: "."})
	assert.Empty(t, findings(t, contract.NewContractPathsInsideFolder(), in))
}

func TestContractPathsInsideFolder_NeedsTheContractFile(t *testing.T) {
	in := newPass(t, nil).input(edit("a.yaml", "a"))
	in.Evidence = remediation.NewEvidence()
	_, err := contract.NewContractPathsInsideFolder().Check(context.Background(), in)
	require.Error(t, err)
}

// --- yaml_reads ---

func TestYAMLReads(t *testing.T) {
	insp := &fakeInspector{bad: map[string]error{"bad: [": errors.New("yaml: line 1: did not find expected node content")}}
	p := newPass(t, nil)

	got := findings(t, contract.NewYAMLReads(insp), p.input(
		edit("svc/contracts/ok.yaml", "nodes: []"),
		edit("svc/contracts/bad.yaml", "bad: ["),
		edit("svc/contracts/bad.yml", "bad: ["),
		edit("svc/contracts/run.py", "bad: ["),
	))

	assert.Equal(t, []remediation.Finding{
		{
			Code: "unreadable_yaml", File: "svc/contracts/bad.yaml",
			Message: `the model returned "svc/contracts/bad.yaml", which is not a readable contract file: yaml: line 1: did not find expected node content`,
		},
		{
			Code: "unreadable_yaml", File: "svc/contracts/bad.yml",
			Message: `the model returned "svc/contracts/bad.yml", which is not a readable contract file: yaml: line 1: did not find expected node content`,
		},
	}, got, "only .yaml and .yml files are read, and each unreadable one is a finding")
	assert.Zero(t, p.archive.fetches, "the new content is read without a checkout")
}

func TestYAMLReads_PassesReadableFiles(t *testing.T) {
	out := runCheck(t, contract.NewYAMLReads(&fakeInspector{}), newPass(t, nil).input(edit("svc/contracts/ok.yaml", "nodes: []")))
	assert.True(t, out.Result.Passed())
}

// --- the checks that compare declarations before and after ---

const (
	beforeText = "before: yaml"
	afterText  = "after: yaml"
)

// declPass is a pass whose repository holds beforeText at the contract file and
// whose draft rewrites it to afterText.
func declPass(t *testing.T) (pass, step.CheckInput) {
	t.Helper()
	p := newPass(t, map[string]string{"svc/contracts/fx.yaml": beforeText})
	return p, p.input(edit("svc/contracts/fx.yaml", afterText))
}

func inspect(before, after []ports.NodeDeclaration) *fakeInspector {
	return &fakeInspector{decls: map[string][]ports.NodeDeclaration{beforeText: before, afterText: after}}
}

func TestNodeIdentityUnchanged(t *testing.T) {
	const (
		removed = "the model's answer no longer declares %s. A fix corrects what a node declares; removing or renaming " +
			"the node itself would leave the release nothing to validate, so the change could never be proven correct"
		changed = "the model's answer changed the fields that identify %s (%s). Those fields say which node the entry is, " +
			"so changing one makes it a different node rather than a repaired one"
	)
	script := func(to string) func(*ports.NodeIdentity) { return func(i *ports.NodeIdentity) { i.Script = to } }
	cases := []struct {
		name          string
		before, after []ports.NodeDeclaration
		want          []remediation.Finding
	}{
		{"an unchanged node passes", []ports.NodeDeclaration{decl("fx", "rates")}, []ports.NodeDeclaration{decl("fx", "rates")}, nil},
		{"a node the answer adds passes", nil, []ports.NodeDeclaration{decl("fx", "new")}, nil},
		{"a node whose reads and nothing else changed passes",
			[]ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates", "b")}, nil},
		{"a node the answer removes",
			[]ports.NodeDeclaration{decl("fx", "rates")}, nil,
			[]remediation.Finding{{Code: "identity_changed", Message: fmt.Sprintf(removed, "fx.rates")}}},
		{"a node whose script changed",
			[]ports.NodeDeclaration{decl("fx", "rates")},
			[]ports.NodeDeclaration{withIdentity(decl("fx", "rates"), script("other.py"))},
			[]remediation.Finding{{Code: "identity_changed", Message: fmt.Sprintf(changed, "fx.rates", `script "run.py" -> "other.py"`)}}},
		{"a node whose kind and owner changed",
			[]ports.NodeDeclaration{decl("fx", "rates")},
			[]ports.NodeDeclaration{withIdentity(decl("fx", "rates"), func(i *ports.NodeIdentity) { i.Kind = "python-csv"; i.Owner = "me" })},
			[]remediation.Finding{{Code: "identity_changed", Message: fmt.Sprintf(changed, "fx.rates", `kind "python-node" -> "python-csv", owner "" -> "me"`)}}},
		{"a node whose secret_ref changed",
			[]ports.NodeDeclaration{decl("fx", "rates")},
			[]ports.NodeDeclaration{withIdentity(decl("fx", "rates"), func(i *ports.NodeIdentity) { i.SecretRef = "continuo-api-x" })},
			[]remediation.Finding{{Code: "identity_changed", Message: fmt.Sprintf(changed, "fx.rates", `secret_ref "" -> "continuo-api-x"`)}}},
		{"a respelled case is a changed identity, not a removal",
			[]ports.NodeDeclaration{decl("fx", "rates")}, []ports.NodeDeclaration{decl("FX", "Rates")},
			[]remediation.Finding{{Code: "identity_changed", Message: fmt.Sprintf(changed, "fx.rates", `schema "fx" -> "FX", table "rates" -> "Rates"`)}}},
		{"every breached node is found, in key order",
			[]ports.NodeDeclaration{decl("fx", "b"), decl("fx", "a"), decl("fx", "c")},
			[]ports.NodeDeclaration{withIdentity(decl("fx", "b"), script("x.py")), decl("fx", "c")},
			[]remediation.Finding{
				{Code: "identity_changed", Message: fmt.Sprintf(removed, "fx.a")},
				{Code: "identity_changed", Message: fmt.Sprintf(changed, "fx.b", `script "run.py" -> "x.py"`)},
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, in := declPass(t)
			assert.Equal(t, c.want, findings(t, contract.NewNodeIdentityUnchanged(inspect(c.before, c.after)), in))
		})
	}
}

func TestNodeIdentityUnchanged_MovingANodeBetweenTheAnswersFilesIsNotARemoval(t *testing.T) {
	p := newPass(t, map[string]string{"svc/contracts/a.yaml": "a-before", "svc/contracts/b.yaml": "b-before"})
	insp := &fakeInspector{decls: map[string][]ports.NodeDeclaration{
		"a-before": {decl("fx", "rates")}, "a-after": nil,
		"b-before": nil, "b-after": {decl("fx", "rates")},
	}}
	in := p.input(edit("svc/contracts/a.yaml", "a-after"), edit("svc/contracts/b.yaml", "b-after"))
	assert.Empty(t, findings(t, contract.NewNodeIdentityUnchanged(insp), in))
}

func TestNodeIdentityUnchanged_AnOriginalThatDoesNotParseDeclaredNothing(t *testing.T) {
	_, in := declPass(t)
	insp := inspect(nil, []ports.NodeDeclaration{decl("fx", "rates")})
	insp.bad = map[string]error{beforeText: errors.New("not yaml")}
	assert.Empty(t, findings(t, contract.NewNodeIdentityUnchanged(insp), in))
}

func TestNodeIdentityUnchanged_AFileTheAnswerWroteUnreadableIsLeftToYAMLReads(t *testing.T) {
	_, in := declPass(t)
	insp := inspect([]ports.NodeDeclaration{decl("fx", "rates")}, nil)
	insp.bad = map[string]error{afterText: errors.New("not yaml")}
	assert.Empty(t, findings(t, contract.NewNodeIdentityUnchanged(insp), in))
}

func TestNodeIdentityUnchanged_OnlyYAMLFilesAreCompared(t *testing.T) {
	p := newPass(t, map[string]string{"svc/contracts/run.py": beforeText})
	in := p.input(edit("svc/contracts/run.py", afterText))
	insp := inspect([]ports.NodeDeclaration{decl("fx", "rates")}, nil)
	assert.Empty(t, findings(t, contract.NewNodeIdentityUnchanged(insp), in))
}

func TestReadsKept(t *testing.T) {
	const dropped = "the model's answer no longer declares the read %s of %s. Validation bind-checks the reads a contract still " +
		"declares, so deleting one hides the failure instead of repairing it: the node's script — which this fix " +
		"does not change — still performs that read, and no release would catch it again. A read may be corrected " +
		"or replaced, never dropped"
	cases := []struct {
		name          string
		before, after []ports.NodeDeclaration
		want          []remediation.Finding
	}{
		{"every read kept passes", []ports.NodeDeclaration{decl("fx", "rates", "a", "b")}, []ports.NodeDeclaration{decl("fx", "rates", "b", "a")}, nil},
		{"a read added passes", []ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates", "a", "b")}, nil},
		{"a node added passes", nil, []ports.NodeDeclaration{decl("fx", "new", "a")}, nil},
		{"a read dropped",
			[]ports.NodeDeclaration{decl("fx", "rates", "a", "b")}, []ports.NodeDeclaration{decl("fx", "rates", "a")},
			[]remediation.Finding{{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"b"`, "fx.rates")}}},
		{"a read renamed",
			[]ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates", "A")},
			[]remediation.Finding{{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"a"`, "fx.rates")}}},
		{"several reads dropped are named sorted",
			[]ports.NodeDeclaration{decl("fx", "rates", "z", "a", "m")}, []ports.NodeDeclaration{decl("fx", "rates", "m")},
			[]remediation.Finding{{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"a", "z"`, "fx.rates")}}},
		{"a sibling's read dropped names the sibling",
			[]ports.NodeDeclaration{decl("fx", "rates", "a"), decl("fx", "sib", "s")},
			[]ports.NodeDeclaration{decl("fx", "rates", "a"), decl("fx", "sib")},
			[]remediation.Finding{{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"s"`, "fx.sib")}}},
		{"a node removed is the identity check's finding, not a dropped read",
			[]ports.NodeDeclaration{decl("fx", "rates", "a")}, nil, nil},
		{"every node that dropped a read is found, in key order",
			[]ports.NodeDeclaration{decl("fx", "b", "x"), decl("fx", "a", "y")}, []ports.NodeDeclaration{decl("fx", "b"), decl("fx", "a")},
			[]remediation.Finding{
				{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"y"`, "fx.a")},
				{Code: "read_dropped", Message: fmt.Sprintf(dropped, `"x"`, "fx.b")},
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, in := declPass(t)
			assert.Equal(t, c.want, findings(t, contract.NewReadsKept(inspect(c.before, c.after)), in))
		})
	}
}

func TestCSVReadKept(t *testing.T) {
	const dropped = "the model's answer no longer declares the %q read of %s. A python-csv node's single read names the " +
		"file its runtime loads; that read's uri may be corrected, but the %q key itself may never be " +
		"removed or renamed, or the node would have nothing left for the runtime to read the file from"
	want := []remediation.Finding{{Code: "csv_read_dropped", Message: fmt.Sprintf(dropped, "csv", "fx.rates", "csv")}}
	cases := []struct {
		name          string
		before, after []ports.NodeDeclaration
		want          []remediation.Finding
	}{
		{"the csv read kept passes", []ports.NodeDeclaration{decl("fx", "rates", "csv")}, []ports.NodeDeclaration{decl("fx", "rates", "csv")}, nil},
		{"the csv read dropped", []ports.NodeDeclaration{decl("fx", "rates", "csv")}, []ports.NodeDeclaration{decl("fx", "rates")}, want},
		{"the csv read renamed", []ports.NodeDeclaration{decl("fx", "rates", "csv")}, []ports.NodeDeclaration{decl("fx", "rates", "file")}, want},
		{"another read dropped is the reads_kept check's finding",
			[]ports.NodeDeclaration{decl("fx", "rates", "csv", "x")}, []ports.NodeDeclaration{decl("fx", "rates", "csv")}, nil},
		{"a node that never read csv passes",
			[]ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates", "b")}, nil},
		{"a node removed is the identity check's finding",
			[]ports.NodeDeclaration{decl("fx", "rates", "csv")}, nil, nil},
		{"a sibling's csv read dropped names the sibling",
			[]ports.NodeDeclaration{decl("fx", "rates", "csv"), decl("fx", "sib", "csv")},
			[]ports.NodeDeclaration{decl("fx", "rates", "csv"), decl("fx", "sib")},
			[]remediation.Finding{{Code: "csv_read_dropped", Message: fmt.Sprintf(dropped, "csv", "fx.sib", "csv")}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, in := declPass(t)
			assert.Equal(t, c.want, findings(t, contract.NewCSVReadKept(inspect(c.before, c.after)), in))
		})
	}
}

func TestNoNewSecretRef(t *testing.T) {
	const added = "the answer adds node %s with a secret_ref; a fix may not grant credentials"
	secret := func(d ports.NodeDeclaration, ref string) ports.NodeDeclaration {
		return withIdentity(d, func(i *ports.NodeIdentity) { i.SecretRef = ref })
	}
	cases := []struct {
		name          string
		before, after []ports.NodeDeclaration
		want          []remediation.Finding
	}{
		{"a new node with no secret passes", nil, []ports.NodeDeclaration{decl("fx", "new")}, nil},
		{"a node that already named a secret passes",
			[]ports.NodeDeclaration{secret(decl("fx", "rates"), "continuo-api-x")}, []ports.NodeDeclaration{secret(decl("fx", "rates"), "continuo-api-x")}, nil},
		{"a new node naming a secret",
			nil, []ports.NodeDeclaration{secret(decl("fx", "new"), "continuo-api-x")},
			[]remediation.Finding{{Code: "secret_ref_added", Message: fmt.Sprintf(added, "fx.new")}}},
		{"every new node naming a secret is found, in key order",
			nil, []ports.NodeDeclaration{secret(decl("fx", "b"), "s"), secret(decl("fx", "a"), "s")},
			[]remediation.Finding{
				{Code: "secret_ref_added", Message: fmt.Sprintf(added, "fx.a")},
				{Code: "secret_ref_added", Message: fmt.Sprintf(added, "fx.b")},
			}},
		{"a secret added to an existing node is the identity check's finding",
			[]ports.NodeDeclaration{decl("fx", "rates")}, []ports.NodeDeclaration{secret(decl("fx", "rates"), "s")}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, in := declPass(t)
			assert.Equal(t, c.want, findings(t, contract.NewNoNewSecretRef(inspect(c.before, c.after)), in))
		})
	}
}

func TestDeclarationChecks_ReadTheOriginalsFromTheCheckoutTheDraftWasAppliedTo(t *testing.T) {
	p, in := declPass(t)
	insp := inspect([]ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates")})

	require.Len(t, findings(t, contract.NewReadsKept(insp), in), 1)

	assert.Equal(t, map[string]string{"svc/contracts/fx.yaml": beforeText}, p.scratch.Originals())
}

func TestDeclarationChecks_ALeftAlonePathIsLeftToTheCheckThatReportsIt(t *testing.T) {
	p := newPass(t, nil)
	in := p.input(edit("../escape.yaml", afterText))
	for _, c := range []step.Check{
		contract.NewNodeIdentityUnchanged(&fakeInspector{}),
		contract.NewReadsKept(&fakeInspector{}),
		contract.NewCSVReadKept(&fakeInspector{}),
		contract.NewNoNewSecretRef(&fakeInspector{}),
	} {
		assert.Empty(t, findings(t, c, in), c.Meta().Name)
	}
	assert.Zero(t, p.archive.fetches)
}

func TestChecksThatReadTheCheckout_NeedAScratch(t *testing.T) {
	in := newPass(t, nil).input(edit("svc/contracts/fx.yaml", "x"))
	in.Scratch = nil
	for _, c := range []step.Check{
		contract.NewNodeIdentityUnchanged(&fakeInspector{}),
		contract.NewReadsKept(&fakeInspector{}),
		contract.NewCSVReadKept(&fakeInspector{}),
		contract.NewNoNewSecretRef(&fakeInspector{}),
		contract.NewDeclaredOnce(&fakeLocator{}),
		contract.NewContractPackages(&fakePackager{}, "postgres"),
	} {
		_, err := c.Check(context.Background(), in)
		require.Error(t, err, c.Meta().Name)
	}
}

// --- declared_once ---

func relocated(dir string) *fakeLocator {
	l := theLocated
	l.ContractDir = dir
	return &fakeLocator{located: map[string]ports.Located{"fx.rates": l}}
}

func TestDeclaredOnce(t *testing.T) {
	cases := []struct {
		name    string
		locator *fakeLocator
		want    []remediation.Finding
	}{
		{"still declared once in the same directory passes", relocated(contractDir), nil},
		{"no longer declared", &fakeLocator{errs: map[string]error{"fx.rates": ports.ErrNodeNotDeclared}}, []remediation.Finding{{
			Code: "not_declared_once",
			Message: "after applying the model's answer, py.fx.rates is no longer declared by exactly one contract file " +
				"(node not declared in any contract yaml); a fix may correct what a node declares but never change which node is declared",
		}}},
		{"declared by several files", &fakeLocator{errs: map[string]error{"fx.rates": ports.ErrAmbiguousDeclaration}}, []remediation.Finding{{
			Code: "not_declared_once",
			Message: "after applying the model's answer, py.fx.rates is no longer declared by exactly one contract file " +
				"(node declared in more than one contract yaml); a fix may correct what a node declares but never change which node is declared",
		}}},
		{"moved to another directory", relocated("svc/other"), []remediation.Finding{{
			Code: "not_declared_once",
			Message: `the model's answer moved py.fx.rates out of the contract directory "svc/contracts" into "svc/other", ` +
				"so the fix would not be packaged with the node it repairs",
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, in := declPass(t)
			assert.Equal(t, c.want, findings(t, contract.NewDeclaredOnce(c.locator), in))
			require.Len(t, c.locator.calls, 1)
			assert.Equal(t, p.archive.dirs[0], c.locator.calls[0].root, "the search runs over the patched checkout")
			assert.Equal(t, "fx", c.locator.calls[0].schema)
			assert.Equal(t, "rates", c.locator.calls[0].table)
		})
	}
}

func TestDeclaredOnce_ASearchThatFailsIsReturnedSoTheWorkIsTriedAgain(t *testing.T) {
	_, in := declPass(t)
	l := &fakeLocator{errs: map[string]error{"fx.rates": errBoom}}
	_, err := contract.NewDeclaredOnce(l).Check(context.Background(), in)
	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "re-locate contract for py.fx.rates after applying the fix")
}

func TestDeclaredOnce_SearchesAfterTheAnswerIsApplied(t *testing.T) {
	p, in := declPass(t)
	var seen string
	l := &sniffLocator{read: func(root string) {
		b, err := os.ReadFile(filepath.Join(root, "svc", "contracts", "fx.yaml"))
		require.NoError(t, err)
		seen = string(b)
	}, located: theLocated}
	require.Empty(t, findings(t, contract.NewDeclaredOnce(l), in))
	assert.Equal(t, afterText, seen)
	assert.Equal(t, 1, p.archive.fetches)
}

type sniffLocator struct {
	read    func(root string)
	located ports.Located
}

func (s *sniffLocator) Locate(root, _, _ string) (ports.Located, error) {
	s.read(root)
	return s.located, nil
}

func TestDeclaredOnce_APathOutsideTheCheckoutIsAFindingAndNothingIsWritten(t *testing.T) {
	p := newPass(t, nil)
	l := relocated(contractDir)
	got := findings(t, contract.NewDeclaredOnce(l), p.input(edit("../escape.yaml", "x")))
	assert.Equal(t, []remediation.Finding{{
		Code: "path_outside_contract_dir", File: "../escape.yaml",
		Message: `the model proposed a change to "../escape.yaml", which is outside the contract directory "svc/contracts" that declares this node`,
	}}, got)
	assert.Empty(t, l.calls)
	assert.Zero(t, p.archive.fetches)
}

// --- contract_packages ---

func TestContractPackages(t *testing.T) {
	p, in := declPass(t)
	pk := &fakePackager{out: []byte("merged: contract")}

	out := runCheck(t, contract.NewContractPackages(pk, "trino"), in)

	assert.True(t, out.Result.Passed())
	assert.Equal(t, map[remediation.EvidenceKey]any{remediation.KeyPackagedContract: remediation.Bytes("merged: contract")}, out.Evidence)
	root := p.archive.dirs[0]
	assert.Equal(t, []mergeCall{{
		contractDir: filepath.Join(root, "svc", "contracts"),
		repoRoot:    filepath.Join(root, "svc"),
		service:     "svc",
		dialect:     "trino",
	}}, pk.calls)
}

func TestContractPackages_APackagerRefusalIsAFindingWithTheToolsMessage(t *testing.T) {
	_, in := declPass(t)
	pk := &fakePackager{err: fmt.Errorf("%w: node fx.rates: duplicate read %q", ports.ErrContractRejected, "a")}

	out := runCheck(t, contract.NewContractPackages(pk, "postgres"), in)

	assert.Equal(t, []remediation.Finding{{
		Code: "contract_rejected",
		Message: "the tool that packages a contract for release refused the model's answer, so no release could be built from it: " +
			`the packaging tool refused the contract: node fx.rates: duplicate read "a"`,
	}}, out.Result.Findings)
	assert.Empty(t, out.Evidence)
}

func TestContractPackages_AnyOtherPackagerFailureIsReturnedSoTheWorkIsTriedAgain(t *testing.T) {
	_, in := declPass(t)
	_, err := contract.NewContractPackages(&fakePackager{err: errBoom}, "postgres").Check(context.Background(), in)
	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "package contract for py.fx.rates")
}

func TestContractPackages_APathOutsideTheCheckoutIsAFindingAndNothingIsPackaged(t *testing.T) {
	p := newPass(t, nil)
	pk := &fakePackager{}
	got := findings(t, contract.NewContractPackages(pk, "postgres"), p.input(edit("/etc/x.yaml", "x")))
	require.Len(t, got, 1)
	assert.Equal(t, "path_outside_contract_dir", got[0].Code)
	assert.Empty(t, pk.calls)
}

// --- the pass ---

func TestScratch_RefetchesTheCheckoutInANewPass(t *testing.T) {
	a := &seedArchive{t: t, seed: map[string]string{"svc/contracts/fx.yaml": beforeText}}
	locator := relocated(contractDir)
	packager := &fakePackager{out: []byte("merged")}
	insp := inspect([]ports.NodeDeclaration{decl("fx", "rates", "a")}, []ports.NodeDeclaration{decl("fx", "rates", "a")})
	checks := []step.Check{
		contract.NewContractPathsInsideFolder(),
		contract.NewYAMLReads(insp),
		contract.NewNodeIdentityUnchanged(insp),
		contract.NewReadsKept(insp),
		contract.NewCSVReadKept(insp),
		contract.NewNoNewSecretRef(insp),
		contract.NewDeclaredOnce(locator),
		contract.NewContractPackages(packager, "postgres"),
	}

	runPass := func() {
		s := step.NewScratch(a, repoName, commit)
		defer s.Close()
		in := step.CheckInput{
			Failure:  failure(nil),
			Evidence: contractFileEvidence(),
			Draft:    remediation.Draft{Edits: []remediation.Edit{edit("svc/contracts/fx.yaml", afterText)}},
			Scratch:  s,
		}
		for _, c := range checks {
			out, err := c.Check(context.Background(), in)
			require.NoError(t, err, c.Meta().Name)
			require.True(t, out.Result.Passed(), c.Meta().Name)
		}
		assert.Equal(t, map[string]string{"svc/contracts/fx.yaml": beforeText}, s.Originals())
	}

	runPass()
	assert.Equal(t, 1, a.fetches, "one pass fetches once, whatever number of checks read the checkout")
	runPass()
	assert.Equal(t, 2, a.fetches, "a new pass fetches again")
}
