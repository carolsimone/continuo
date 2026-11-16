package source_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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
	require.False(t, out.Declined)
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

func TestFailingNodeSource_CompileLeavesOutAMissingDirectoryAndAMissingProjectFile(t *testing.T) {
	src := &fakeSource{
		files:  map[string]string{"services/svc/models/x.sql": "select 1"},
		dirErr: ports.ErrSourceNotFound,
	}
	s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

	out := gather(t, s, failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}))

	assert.Equal(t, []string{"services/svc/models/x.sql"}, sourceFiles(t, out).Order)
}

// A context file that exists but cannot be read now is a transient error,
// returned so the step runs again, rather than a file silently left out of what
// the model is shown.
func TestFailingNodeSource_ATransientContextReadErrorIsReturned(t *testing.T) {
	cases := map[string]*fakeSource{
		"the directory listing": {
			files:  map[string]string{"services/svc/models/x.sql": "select 1"},
			dirErr: errBoom,
		},
		"a yml beside the file": {
			files:   map[string]string{"services/svc/models/x.sql": "select 1", "services/svc/models/schema.yml": "version: 2"},
			readErr: map[string]error{"services/svc/models/schema.yml": errBoom},
		},
		"the project file": {
			files:   map[string]string{"services/svc/models/x.sql": "select 1"},
			readErr: map[string]error{"services/svc/dbt_project.yml": errBoom},
		},
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

			out, err := s.Gather(context.Background(), step.ContextInput{
				Failure:  failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}),
				Evidence: remediation.NewEvidence(),
			})

			require.ErrorIs(t, err, errBoom)
			assert.False(t, out.Declined)
			assert.Empty(t, out.Evidence)
		})
	}
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
			assert.True(t, out.Declined)
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

// A locator error is transient: the locator answers an unknown node with an
// empty location and no error. It is returned so the step runs again.
func TestFailingNodeSource_SeedReturnsTheLocatorsError(t *testing.T) {
	s := source.NewFailingNodeSource(&fakeSource{}, &fakeLocator{err: errBoom}, repoPaths, discardLogger())

	out, err := s.Gather(context.Background(), step.ContextInput{
		Failure:  failure("seed_build", remediation.TriggerNode{NodeID: "n1"}),
		Evidence: remediation.NewEvidence(),
	})

	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, "locate seed n1 in the promoted graph: boom", err.Error())
	assert.False(t, out.Declined)
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

// errTooLarge is the error the reader returns for a file over its cap.
var errTooLarge = fmt.Errorf("github get p@abc123: source file exceeds %d bytes: %w", ports.MaxSourceBytes, ports.ErrSourceTooLarge)

// A yml beside the model or the project file that is over the read cap cannot
// be shown to the model, and is not a reason to fail: it is left out, and the
// step gathers the rest.
func TestFailingNodeSource_AnOversizedSideFileIsLeftOut(t *testing.T) {
	src := &fakeSource{
		files: map[string]string{
			"services/svc/models/x.sql":      "select 1",
			"services/svc/models/schema.yml": "version: 2",
			"services/svc/models/huge.yml":   "never read",
			"services/svc/dbt_project.yml":   "never read",
		},
		readErr: map[string]error{
			"services/svc/models/huge.yml": errTooLarge,
			"services/svc/dbt_project.yml": errTooLarge,
		},
	}
	var logs bytes.Buffer
	s := source.NewFailingNodeSource(src, nil, repoPaths, slog.New(slog.NewTextHandler(&logs, nil)))

	out := gather(t, s, failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}))

	sf := sourceFiles(t, out)
	assert.Equal(t, []string{"services/svc/models/x.sql", "services/svc/models/schema.yml"}, sf.Order)
	assert.NotContains(t, sf.Files, "services/svc/models/huge.yml")
	assert.NotContains(t, sf.Files, "services/svc/dbt_project.yml")
	for _, left := range []string{"services/svc/models/huge.yml", "services/svc/dbt_project.yml"} {
		assert.Contains(t, logs.String(), "level=WARN")
		assert.Contains(t, logs.String(), left, "the log names each file left out")
	}
	assert.Contains(t, logs.String(), fmt.Sprint(ports.MaxSourceBytes), "the log names the cap")
}

// The file to fix cannot be edited when it cannot be read, so the cluster is
// declined with a reason naming the file and the cap, and the step does not
// fail on every retry.
func TestFailingNodeSource_AnOversizedTargetFileDeclines(t *testing.T) {
	cases := []struct {
		name string
		src  string
		node remediation.TriggerNode
		file string
	}{
		{"compile", "compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}, "services/svc/models/x.sql"},
		{"parse", "parse", remediation.TriggerNode{NodeID: "n1", Service: "svc", FilePath: "models/x.sql"}, "services/svc/models/x.sql"},
		{"seed_build", "seed_build", remediation.TriggerNode{NodeID: "n1", Service: "svc", FilePath: "seeds/a.csv"}, "services/svc/seeds/a.csv"},
		{"duplicate_table", "duplicate_table", remediation.TriggerNode{NodeID: "n1", Service: "svc", FilePath: "models/x.sql", NodeType: "dbt-model"}, "services/svc/models/x.sql"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &fakeSource{readErr: map[string]error{c.file: errTooLarge}}
			s := source.NewFailingNodeSource(src, &fakeLocator{}, repoPaths, discardLogger())

			out := gather(t, s, failure(c.src, c.node))

			assert.True(t, out.Declined)
			assert.Equal(t, "the offending file "+c.file+" is over the "+fmt.Sprint(ports.MaxSourceBytes)+"-byte read limit, so no fix can edit it", out.Decline)
			assert.Empty(t, out.Evidence)
		})
	}
}

// A read the repository refuses for another reason, such as a token without
// permission, is a fault of the install and not a fact about the file: it is
// returned so the runner retries it and, at the limit, ends the remediation as
// a system failure.
func TestFailingNodeSource_AForbiddenReadStaysAStepError(t *testing.T) {
	forbidden := errors.New("github get p@abc123: status 403: Resource not accessible by integration")
	cases := map[string]map[string]error{
		"the file to fix":  {"services/svc/models/x.sql": forbidden},
		"a yml beside it":  {"services/svc/models/schema.yml": forbidden},
		"the project file": {"services/svc/dbt_project.yml": forbidden},
	}
	for name, readErr := range cases {
		t.Run(name, func(t *testing.T) {
			src := &fakeSource{
				files:   map[string]string{"services/svc/models/x.sql": "select 1", "services/svc/models/schema.yml": "version: 2"},
				readErr: readErr,
			}
			s := source.NewFailingNodeSource(src, nil, repoPaths, discardLogger())

			out, err := s.Gather(context.Background(), step.ContextInput{
				Failure:  failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"}),
				Evidence: remediation.NewEvidence(),
			})

			require.ErrorIs(t, err, forbidden)
			assert.NotErrorIs(t, err, ports.ErrSourceTooLarge)
			assert.NotErrorIs(t, err, ports.ErrSourceNotFound)
			assert.False(t, out.Declined)
			assert.Empty(t, out.Evidence)
		})
	}
}
