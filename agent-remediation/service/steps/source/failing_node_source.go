// Package source holds the steps of the flows whose fix is one edited
// repository file: the context source that reads the failing node's source
// files, the prompts that ask the model to rewrite one of them, and the
// pre-flight check that the file the model chose is one it was shown. Each step
// depends only on the ports it reads through.
package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// The trigger sources failing_node_source reads source files for.
const (
	sourceCompile        = "compile"
	sourceParse          = "parse"
	sourceSeedBuild      = "seed_build"
	sourceDuplicateTable = "duplicate_table"
)

// FailingNodeSource is the context source `failing_node_source`: the failing
// node's source file and, for a dbt model, the files around it.
type FailingNodeSource struct {
	source           ports.SourceReader
	locator          ports.NodeLocator
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.ContextSource = (*FailingNodeSource)(nil)

// NewFailingNodeSource builds the failing_node_source step. serviceRepoPaths
// maps a service name to its path inside the repository. It calls no port
// until Gather.
func NewFailingNodeSource(source ports.SourceReader, locator ports.NodeLocator, serviceRepoPaths map[string]string, logger *slog.Logger) *FailingNodeSource {
	if logger == nil {
		logger = slog.Default()
	}
	return &FailingNodeSource{source: source, locator: locator, serviceRepoPaths: serviceRepoPaths, logger: logger}
}

// Meta describes the step.
func (*FailingNodeSource) Meta() step.Meta {
	return step.Meta{
		Name: "failing_node_source",
		Kind: step.KindContext,
		Description: "Reads the failing node's source file from the repository at the trigger's commit, with the yml files beside it and " +
			"the service's dbt_project.yml for a compile or parse failure. Declines a failure whose source this cannot edit.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeySourceFiles},
	}
}

// Gather reads the files the trigger's source calls for. A failure that has no
// source file to fix declines the cluster with its reason. A read error other
// than a missing file is transient and is returned so the work is tried again.
func (s *FailingNodeSource) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	var (
		files      remediation.SourceFiles
		skipReason string
		err        error
	)
	switch f.Trigger.Source {
	case sourceCompile:
		files, skipReason, err = s.compile(ctx, f)
	case sourceParse:
		files, skipReason, err = s.parse(ctx, f)
	case sourceSeedBuild:
		files, skipReason, err = s.seed(ctx, f)
	case sourceDuplicateTable:
		files, skipReason, err = s.duplicateTable(ctx, f)
	default:
		return step.ContextOutput{}, fmt.Errorf("failing_node_source: no source files to read for a %q failure", f.Trigger.Source)
	}
	if err != nil {
		return step.ContextOutput{}, err
	}
	if skipReason != "" {
		s.logger.Info("source-file fix skipped", "node", f.Target.NodeID, "node_type", f.Target.NodeType,
			"source", f.Trigger.Source, "reason", skipReason)
		return step.ContextOutput{Declined: true, Decline: skipReason}, nil
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{remediation.KeySourceFiles: files}}, nil
}

func (s *FailingNodeSource) compile(ctx context.Context, f remediation.Failure) (remediation.SourceFiles, string, error) {
	if f.Target.FilePath == "" { // project-level error: no models/ path in the log
		return remediation.SourceFiles{}, "the compile error names no source file, so there is no file to fix", nil
	}
	// For compile the node id is the service discriminator (a synthetic id).
	return s.gatherWithContext(ctx, f, f.Target.NodeID)
}

func (s *FailingNodeSource) parse(ctx context.Context, f remediation.Failure) (remediation.SourceFiles, string, error) {
	n := f.Target
	switch {
	case n.NodeType == string(pkg_model.NodeTypePythonCsv):
		return remediation.SourceFiles{}, fmt.Sprintf("%s is a python-csv node: its only read is an S3 URI, not SQL the parser can reject, "+
			"so remediation has no fix to offer; correct the contract by hand", n.NodeID), nil
	case n.NodeType == string(pkg_model.NodeTypePythonApi):
		return remediation.SourceFiles{}, fmt.Sprintf("%s is a python-api node: python-api has no reads, so there is no SQL "+
			"the parser can reject and remediation has no fix to offer; correct the contract by hand", n.NodeID), nil
	case pkg_model.NodeType(n.NodeType).IsPython():
		return remediation.SourceFiles{}, fmt.Sprintf("%s is a %s node: its reads are declared in its service's contract.yaml, "+
			"which this lane does not edit; correct the contract by hand", n.NodeID, n.NodeType), nil
	case n.FilePath == "":
		return remediation.SourceFiles{}, "the parse rejection names no source file, so there is no file to fix", nil
	case n.Service == "":
		return remediation.SourceFiles{}, "the parse rejection names no service, so the source cannot be located", nil
	}
	return s.gatherWithContext(ctx, f, n.Service)
}

func (s *FailingNodeSource) seed(ctx context.Context, f remediation.Failure) (remediation.SourceFiles, string, error) {
	filePath, service := f.Target.FilePath, f.Target.Service
	if filePath == "" || service == "" {
		fp, svcName, err := s.locator.Locate(ctx, f.Target.NodeID)
		if err != nil {
			return remediation.SourceFiles{}, fmt.Sprintf("the seed's source location could not be resolved from the graph: %v", err), nil
		}
		if filePath == "" {
			filePath = fp
		}
		if service == "" {
			service = svcName
		}
	}
	if filePath == "" || service == "" {
		return remediation.SourceFiles{}, "neither the trigger nor the graph names the seed's csv file and service, so there is no file to fix", nil
	}
	return s.readOffendingFile(ctx, f, service, filePath)
}

// duplicateTable reads only the claimant the release changed. A python claimant
// declares its relation in its service's contract.yaml, and a dbt seed's
// relation name comes from its csv filename or the project's config, never from
// the csv's contents, so editing either file would not change which relation
// the node claims: both are declined. The competing claimant's source is never
// read, since its service and path are all the prompt needs to name it.
func (s *FailingNodeSource) duplicateTable(ctx context.Context, f remediation.Failure) (remediation.SourceFiles, string, error) {
	n := f.Target
	if pkg_model.NodeType(n.NodeType).IsPython() {
		return remediation.SourceFiles{}, "the claimant is a python node, whose relation is declared in its service's " +
			"contract.yaml rather than in the file the trigger names; rename it in the contract by hand", nil
	}
	if n.NodeType == string(pkg_model.NodeTypeDbtSeed) {
		return remediation.SourceFiles{}, "the claimant is a dbt seed, whose relation name comes from the csv filename or " +
			"project config, never from the csv's contents; an edited csv would alter the seed's data, " +
			"not its name, so rename the seed by hand", nil
	}
	if n.FilePath == "" || n.Service == "" {
		return remediation.SourceFiles{}, "the trigger names no source file or service for the claimant, so there is no file to fix", nil
	}
	return s.readOffendingFile(ctx, f, n.Service, n.FilePath)
}

// gatherWithContext reads the offending file named by the failing node's file
// path under the repository prefix of service and, when it is a .sql, its
// co-located yml siblings and the service's dbt_project.yml as best-effort
// context. A missing prefix or a missing offending file is a skip, returned as
// its reason; any other read error is transient and returned.
func (s *FailingNodeSource) gatherWithContext(ctx context.Context, f remediation.Failure, service string) (remediation.SourceFiles, string, error) {
	g, skipReason, err := s.readOffendingFile(ctx, f, service, f.Target.FilePath)
	if err != nil || skipReason != "" {
		return remediation.SourceFiles{}, skipReason, err
	}
	offending := g.Primary
	prefix := s.serviceRepoPaths[service]

	if strings.HasSuffix(offending, ".sql") {
		dir := path.Dir(offending)
		if paths, derr := s.source.ListDir(ctx, f.Trigger.Repo, f.Trigger.CommitSHA, dir); derr == nil {
			for _, p := range paths {
				if p == offending {
					continue
				}
				if strings.HasSuffix(p, ".yml") || strings.HasSuffix(p, ".yaml") {
					s.addFile(ctx, f, &g, p)
				}
			}
		}
		s.addFile(ctx, f, &g, path.Join(prefix, "dbt_project.yml"))
	}
	return g, "", nil
}

// readOffendingFile resolves filePath under service's repository prefix and
// reads it at the trigger's commit, returning it as a one-file SourceFiles.
func (s *FailingNodeSource) readOffendingFile(ctx context.Context, f remediation.Failure, service, filePath string) (remediation.SourceFiles, string, error) {
	prefix, ok := s.serviceRepoPaths[service]
	if !ok {
		return remediation.SourceFiles{}, fmt.Sprintf("service %q has no repository path mapping, so its source cannot be read", service), nil
	}
	offending := path.Join(prefix, filePath)
	content, err := s.source.ReadFile(ctx, f.Trigger.Repo, f.Trigger.CommitSHA, offending)
	if err != nil {
		if errors.Is(err, ports.ErrSourceNotFound) {
			return remediation.SourceFiles{}, fmt.Sprintf("the offending file %s does not exist at commit %s", offending, f.Trigger.CommitSHA), nil
		}
		return remediation.SourceFiles{}, "", err // transient: tried again
	}
	return remediation.SourceFiles{
		Files:   map[string]string{offending: content},
		Order:   []string{offending},
		Primary: offending,
		Service: service,
	}, "", nil
}

// addFile reads one best-effort context file, ignoring any read error.
func (s *FailingNodeSource) addFile(ctx context.Context, f remediation.Failure, g *remediation.SourceFiles, p string) {
	if _, seen := g.Files[p]; seen {
		return
	}
	c, err := s.source.ReadFile(ctx, f.Trigger.Repo, f.Trigger.CommitSHA, p)
	if err != nil {
		return // context is optional
	}
	g.Files[p] = c
	g.Order = append(g.Order, p)
}
