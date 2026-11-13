package source_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/source"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

var repoPaths = map[string]string{"svc": "services/svc"}

func failure(src string, n remediation.TriggerNode) remediation.Failure {
	return remediation.Failure{
		Trigger: remediation.Trigger{Source: src, ReleaseID: "rel-1", Repo: "org/repo", CommitSHA: "abc123"},
		Target:  n,
		Attempt: 2,
	}
}

func gather(t *testing.T, s *source.FailingNodeSource, f remediation.Failure) step.ContextOutput {
	t.Helper()
	out, err := s.Gather(context.Background(), step.ContextInput{Failure: f, Evidence: remediation.NewEvidence()})
	require.NoError(t, err)
	return out
}

func sourceFiles(t *testing.T, out step.ContextOutput) remediation.SourceFiles {
	t.Helper()
	require.Empty(t, out.Decline)
	sf, ok := out.Evidence[remediation.KeySourceFiles].(remediation.SourceFiles)
	require.True(t, ok, "the step provides source_files")
	return sf
}

func TestFailingNodeSource_Meta(t *testing.T) {
	m := source.NewFailingNodeSource(nil, nil, nil, nil).Meta()
	assert.Equal(t, "failing_node_source", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeySourceFiles}, m.Provides)
}

func TestFailingNodeSource_CompileReadsTheFileItsYmlSiblingsAndTheProjectFile(t *testing.T) {
	src := &fakeSource{files: map[string]string{
		"services/svc/models/x.sql":      "select 1",
		"services/svc/models/schema.yml": "version: 2",
		"services/svc/models/other.yaml": "other",
		"services/svc/models/readme.md":  "not shown",
		"services/svc/models/y.sql":      "not shown",
		"services/svc/dbt_project.yml":   "name: svc",
		"services/other/dbt_project.yml": "not shown",
	}}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}))

	sf := sourceFiles(t, out)
	assert.Equal(t, "services/svc/models/x.sql", sf.Primary)
	assert.Equal(t, []string{
		"services/svc/models/x.sql",
		"services/svc/models/other.yaml",
		"services/svc/models/schema.yml",
		"services/svc/dbt_project.yml",
	}, sf.Order)
	assert.Equal(t, "select 1", sf.Files["services/svc/models/x.sql"])
	assert.Len(t, sf.Files, 4)
}

func TestFailingNodeSource_CompileSkipsContextForANonSQLFile(t *testing.T) {
	src := &fakeSource{files: map[string]string{
		"services/svc/seeds/a.csv":      "a,b",
		"services/svc/seeds/schema.yml": "version: 2",
		"services/svc/dbt_project.yml":  "name: svc",
	}}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "seeds/a.csv"}))

	assert.Equal(t, []string{"services/svc/seeds/a.csv"}, sourceFiles(t, out).Order)
}

func TestFailingNodeSource_CompileIgnoresAFailingListingAndAMissingProjectFile(t *testing.T) {
	src := &fakeSource{
		files:  map[string]string{"services/svc/models/x.sql": "select 1"},
		dirErr: errBoom,
	}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}))

	assert.Equal(t, []string{"services/svc/models/x.sql"}, sourceFiles(t, out).Order)
}

func TestFailingNodeSource_Declines(t *testing.T) {
	missing := &fakeSource{files: map[string]string{}}
	cases := []struct {
		name string
		src  string
		node remediation.TriggerNode
		want string
	}{
		{"compile with no file", "compile", remediation.TriggerNode{NodeID: "svc"},
			"the compile error names no source file, so there is no file to fix"},
		{"compile with an unmapped service", "compile", remediation.TriggerNode{NodeID: "ghost", FilePath: "models/x.sql"},
			`service "ghost" has no repository path mapping, so its source cannot be read`},
		{"compile with a missing file", "compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"},
			"the offending file services/svc/models/x.sql does not exist at commit abc123"},
		{"parse of a python-csv node", "parse", remediation.TriggerNode{NodeID: "n1", NodeType: string(pkg_model.NodeTypePythonCsv)},
			"n1 is a python-csv node: its only read is an S3 URI, not SQL the parser can reject, " +
				"so remediation has no fix to offer; correct the contract by hand"},
		{"parse of a python-api node", "parse", remediation.TriggerNode{NodeID: "n1", NodeType: string(pkg_model.NodeTypePythonApi)},
			"n1 is a python-api node: python-api has no reads, so there is no SQL " +
				"the parser can reject and remediation has no fix to offer; correct the contract by hand"},
		{"parse of a python-node", "parse", remediation.TriggerNode{NodeID: "n1", NodeType: string(pkg_model.NodeTypePythonNode)},
			"n1 is a python-node node: its reads are declared in its service's contract.yaml, " +
				"which this lane does not edit; correct the contract by hand"},
		{"parse with no file", "parse", remediation.TriggerNode{NodeID: "n1", Service: "svc"},
			"the parse rejection names no source file, so there is no file to fix"},
		{"parse with no service", "parse", remediation.TriggerNode{NodeID: "n1", FilePath: "models/x.sql"},
			"the parse rejection names no service, so the source cannot be located"},
		{"seed the graph cannot place", "seed_build", remediation.TriggerNode{NodeID: "n1"},
			"neither the trigger nor the graph names the seed's csv file and service, so there is no file to fix"},
		{"duplicate_table of a python node", "duplicate_table", remediation.TriggerNode{NodeID: "n1", NodeType: string(pkg_model.NodeTypePythonNode)},
			"the claimant is a python node, whose relation is declared in its service's " +
				"contract.yaml rather than in the file the trigger names; rename it in the contract by hand"},
		{"duplicate_table of a dbt seed", "duplicate_table", remediation.TriggerNode{NodeID: "n1", NodeType: string(pkg_model.NodeTypeDbtSeed)},
			"the claimant is a dbt seed, whose relation name comes from the csv filename or " +
				"project config, never from the csv's contents; an edited csv would alter the seed's data, " +
				"not its name, so rename the seed by hand"},
		{"duplicate_table with no file", "duplicate_table", remediation.TriggerNode{NodeID: "n1", Service: "svc"},
			"the trigger names no source file or service for the claimant, so there is no file to fix"},
		{"duplicate_table with no service", "duplicate_table", remediation.TriggerNode{NodeID: "n1", FilePath: "models/x.sql"},
			"the trigger names no source file or service for the claimant, so there is no file to fix"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := source.NewFailingNodeSource(missing, &fakeLocator{}, repoPaths, discardLogger())
			out := gather(t, s, failure(c.src, c.node))
			assert.Equal(t, c.want, out.Decline)
			assert.Empty(t, out.Evidence)
		})
	}
}

func TestFailingNodeSource_ParseReadsTheFileUnderTheTriggersService(t *testing.T) {
	src := &fakeSource{files: map[string]string{
		"services/svc/models/x.sql":    "select 1 from raw",
		"services/svc/dbt_project.yml": "name: svc",
	}}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("parse", remediation.TriggerNode{NodeID: "model.svc.x", Service: "svc", FilePath: "models/x.sql", NodeType: "dbt-model"}))

	sf := sourceFiles(t, out)
	assert.Equal(t, "services/svc/models/x.sql", sf.Primary)
	assert.Equal(t, []string{"services/svc/models/x.sql", "services/svc/dbt_project.yml"}, sf.Order)
	assert.Equal(t, "svc", sf.Service)
}

func TestFailingNodeSource_SeedReadsOnlyTheCSVWithoutContext(t *testing.T) {
	src := &fakeSource{files: map[string]string{
		"services/svc/seeds/a.csv":     "a,b",
		"services/svc/dbt_project.yml": "name: svc",
	}}
	s := source.NewFailingNodeSource(src, &fakeLocator{}, repoPaths, discardLogger())

	out := gather(t, s, failure("seed_build", remediation.TriggerNode{NodeID: "seed.svc.a", Service: "svc", FilePath: "seeds/a.csv"}))

	assert.Equal(t, []string{"services/svc/seeds/a.csv"}, sourceFiles(t, out).Order)
}

func TestFailingNodeSource_SeedLocatesWhatTheTriggerLeavesOut(t *testing.T) {
	src := &fakeSource{files: map[string]string{"services/svc/seeds/a.csv": "a,b"}}
	loc := &fakeLocator{filePath: "seeds/a.csv", service: "svc"}
	s := source.NewFailingNodeSource(src, loc, repoPaths, discardLogger())

	out := gather(t, s, failure("seed_build", remediation.TriggerNode{NodeID: "seed.svc.a"}))

	assert.Equal(t, "services/svc/seeds/a.csv", sourceFiles(t, out).Primary)
	assert.Equal(t, []string{"seed.svc.a"}, loc.calls)

	loc.calls = nil
	gather(t, s, failure("seed_build", remediation.TriggerNode{NodeID: "seed.svc.a", Service: "svc", FilePath: "seeds/a.csv"}))
	assert.Empty(t, loc.calls, "a trigger that names both is not located")
}

func TestFailingNodeSource_SeedDeclinesWithTheLocatorsError(t *testing.T) {
	s := source.NewFailingNodeSource(&fakeSource{}, &fakeLocator{err: errBoom}, repoPaths, discardLogger())

	out := gather(t, s, failure("seed_build", remediation.TriggerNode{NodeID: "n1"}))

	assert.Equal(t, fmt.Sprintf("the seed's source location could not be resolved from the graph: %v", errBoom), out.Decline)
}

func TestFailingNodeSource_DuplicateTableReadsOnlyTheClaimant(t *testing.T) {
	src := &fakeSource{files: map[string]string{
		"services/svc/models/x.sql":      "select 1",
		"services/svc/models/schema.yml": "version: 2",
		"services/svc/dbt_project.yml":   "name: svc",
	}}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("duplicate_table", remediation.TriggerNode{NodeID: "model.svc.x", Service: "svc", FilePath: "models/x.sql", NodeType: "dbt-model"}))

	assert.Equal(t, []string{"services/svc/models/x.sql"}, sourceFiles(t, out).Order)
	assert.Equal(t, []string{"services/svc/models/x.sql"}, src.reads)
}

func TestFailingNodeSource_ATransientReadErrorIsReturned(t *testing.T) {
	src := &fakeSource{readErr: map[string]error{"services/svc/models/x.sql": errBoom}}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	_, err := s.Gather(context.Background(), step.ContextInput{
		Failure: failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}),
	})

	assert.ErrorIs(t, err, errBoom)
}

func TestFailingNodeSource_ASourceItDoesNotReadIsAnError(t *testing.T) {
	s := source.NewFailingNodeSource(&fakeSource{}, nil, repoPaths, discardLogger())

	_, err := s.Gather(context.Background(), step.ContextInput{Failure: failure("validation", remediation.TriggerNode{})})

	assert.ErrorContains(t, err, `"validation"`)
}
