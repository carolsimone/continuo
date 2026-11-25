package validation

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
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// errNonDbtCandidate reports that the failing node's candidate source is not a
// dbt model, on either of two signals: a code bundle entry whose recorded
// runtime is not dbt (its RawCode is then the node's normalized contract entry
// rather than model source), checked regardless of node type, since the
// bundle's own recorded runtime is authoritative whenever it is present, or,
// only when the trigger carries no node type, a fallback repo path that does
// not end in ".sql" (a python node's script, which is the only non-dbt source
// this system tracks a path for; a trigger that does carry a node type trusts
// it instead of the extension, since a dbt snapshot's source can legitimately
// be a .yml file). Neither can be sent to the model or read as model source.
var errNonDbtCandidate = errors.New("candidate source is not a dbt model")

// sourceTooLargeError reports that the release's code bundle holds no source
// for the failing node and the repository file the step falls back to, Path,
// is over ports.MaxSourceBytes, so it can be neither shown to the model nor
// edited. Its text is the decline.
type sourceTooLargeError struct {
	NodeID string
	Path   string
}

func (e *sourceTooLargeError) Error() string {
	return fmt.Sprintf("the failing node %s's source is not in the release's code bundle, and its file %s in the repository is over the %d-byte read limit, so no fix can edit it",
		e.NodeID, e.Path, ports.MaxSourceBytes)
}

// CodeBundleSource is the context source `code_bundle_source`: the failing
// node's source as the release holds it, with the location it was found at.
type CodeBundleSource struct {
	candidates       ports.CandidateSourceReader
	source           ports.SourceReader
	locator          ports.NodeLocator
	projectRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.ContextSource = (*CodeBundleSource)(nil)

// NewCodeBundleSource builds the code_bundle_source step. projectRepoPaths maps
// a project name to its path inside the repository. It calls no port until
// Gather.
func NewCodeBundleSource(candidates ports.CandidateSourceReader, source ports.SourceReader, locator ports.NodeLocator, projectRepoPaths map[string]string, logger *slog.Logger) *CodeBundleSource {
	return &CodeBundleSource{
		candidates:       candidates,
		source:           source,
		locator:          locator,
		projectRepoPaths: projectRepoPaths,
		logger:           shared.LoggerOrDefault(logger),
	}
}

// Meta describes the step.
func (*CodeBundleSource) Meta() step.Meta {
	return step.Meta{
		Name: "code_bundle_source",
		Kind: step.KindContext,
		Description: "Reads the failing dbt node's source from the release's code bundle, falling back to the repository at the trigger's commit, " +
			"with the file path and project it was found for. Declines a node whose source is not a dbt model, one whose repository " +
			"file is over the read limit, and one whose source, file path, project or the project's repository path is unknown, " +
			"since no fix can edit it.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyCodeBundleSource},
	}
}

// Gather finds where the node's source lives, then reads it: the location the
// trigger carries is preferred, which release-controller stamps from the
// candidate topology, and only a trigger that carries none falls back to the
// promoted graph, because the rejected release was never promoted. The source
// itself is the bundle's, with the repository read as the fallback. A node the
// graph does not know and a source that is not there are permanent misses,
// which leave the location or the source empty and are not errors. A failed
// lookup or read is transient and is returned so the work is tried again.
//
// A source that is not a dbt model declines the cluster, and so does a
// repository fallback file over the read limit, and a node whose location or
// source stayed unknown: a fix edits the file the node's
// source lives in, so with no source, file path, project or repository path
// for the project there is no edit to make, and no model call is worth making.
// Each decline carries its reason.
func (s *CodeBundleSource) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	n := f.Target
	filePath, projectName := n.FilePath, n.Project
	if filePath == "" {
		var err error
		filePath, projectName, err = s.locator.Locate(ctx, n.NodeID)
		if err != nil {
			return step.ContextOutput{}, fmt.Errorf("locate %s: %w", n.NodeID, err)
		}
	}

	source, origin, err := s.resolve(ctx, f, filePath, projectName)
	if errors.Is(err, errNonDbtCandidate) {
		s.logger.Info("validation fix: failing node's candidate source is not a dbt model; skipping",
			"node", n.NodeID, "error", err)
		return step.ContextOutput{Declined: true, Decline: err.Error()}, nil
	}
	var tooLarge *sourceTooLargeError
	if errors.As(err, &tooLarge) {
		return step.ContextOutput{Declined: true, Decline: tooLarge.Error()}, nil
	}
	if err != nil {
		return step.ContextOutput{}, err
	}
	if reason := s.unreachable(n.NodeID, filePath, projectName, source); reason != "" {
		s.logger.Info("validation fix: failing node's source cannot be edited; skipping", "node", n.NodeID, "reason", reason)
		return step.ContextOutput{Declined: true, Decline: reason}, nil
	}
	s.logger.Info("validation fix: candidate source resolved", "node", n.NodeID, "source_origin", origin)
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyCodeBundleSource: remediation.CandidateSource{
			Source:   source,
			Origin:   origin,
			FilePath: filePath,
			Project:  projectName,
		},
	}}, nil
}

// unreachable is why no fix can edit the node's source, or "" when it can: the
// node has no file path or project, the project has no repository path, or the
// source was found nowhere. The location is judged first, because with none the
// source could only have come from the bundle.
func (s *CodeBundleSource) unreachable(nodeID, filePath, projectName, source string) string {
	switch {
	case filePath == "":
		return fmt.Sprintf("the failing node %s has no file path, so there is no source file to edit", nodeID)
	case projectName == "":
		return fmt.Sprintf("the failing node %s names no project, so there is no source file to edit", nodeID)
	}
	if _, ok := s.projectRepoPaths[projectName]; !ok {
		return fmt.Sprintf("project %q has no repository path mapping", projectName)
	}
	if source == "" {
		return fmt.Sprintf("the failing node %s's source is in neither the release's code bundle nor the repository at the trigger's commit", nodeID)
	}
	return ""
}

// resolve returns the failing candidate's raw source. Order: the release's code
// bundle (exact failing source, keyed by unique id, no path mapping needed),
// then the repo file at the release's commit, then "". A bundle entry that is
// not a dbt node returns errNonDbtCandidate immediately; every permanent bundle
// miss, including a bundle for a different release and a dbt entry with no
// source text, walks down to the repo fallback instead. There, an empty file
// path, an empty project name, or a project with no entry in projectRepoPaths
// leaves the source unresolved, since these are location gaps and say nothing
// about the node's kind. Only when the trigger carries no node type does a
// resolved path that does not end in ".sql" also return errNonDbtCandidate: a
// trigger that does carry a node type is trusted over the extension and always
// proceeds to the read, where a missing file leaves the source unresolved, a
// file over the reader's size cap returns a *sourceTooLargeError naming it,
// and any other read error is returned.
func (s *CodeBundleSource) resolve(ctx context.Context, f remediation.Failure, filePath, projectName string) (source, origin string, err error) {
	n := f.Target
	src, berr := s.candidates.NodeSource(ctx, f.Trigger.CodeBundleURI, n.NodeID, f.Trigger.ReleaseID)
	if berr == nil {
		if src.Runtime != ports.RuntimeDbt {
			return "", "", fmt.Errorf("node %q runtime %q: %w", n.NodeID, src.Runtime, errNonDbtCandidate)
		}
		if src.RawCode != "" {
			return src.RawCode, "bundle", nil
		}
		s.logger.Warn("candidate source in bundle is empty; falling back to repo read",
			"node", n.NodeID, "bundle_uri", f.Trigger.CodeBundleURI)
	} else if !errors.Is(berr, ports.ErrNotFound) {
		return "", "", fmt.Errorf("fetch code bundle: %w", berr)
	} else {
		s.logger.Warn("candidate source not in bundle; falling back to repo read",
			"node", n.NodeID, "bundle_uri", f.Trigger.CodeBundleURI)
	}
	if filePath == "" || projectName == "" {
		return "", "", nil
	}
	prefix, ok := s.projectRepoPaths[projectName]
	if !ok {
		return "", "", nil
	}
	// With no node type the fallback path's kind is unknown, and a non-.sql
	// path is a python node's script, refused without being read.
	if n.NodeType == "" && !strings.HasSuffix(filePath, ".sql") {
		return "", "", fmt.Errorf("node %q path %q: %w", n.NodeID, filePath, errNonDbtCandidate)
	}
	full := path.Join(prefix, filePath)
	content, gerr := s.source.ReadFile(ctx, f.Trigger.Repo, f.Trigger.CommitSHA, full)
	if errors.Is(gerr, ports.ErrSourceNotFound) {
		s.logger.Warn("repo fallback has no such file", "node", n.NodeID, "path", full)
		return "", "", nil
	}
	if errors.Is(gerr, ports.ErrSourceTooLarge) {
		s.logger.Warn("repo fallback file is over the read limit; no source from the repository",
			"node", n.NodeID, "path", full, "max_bytes", ports.MaxSourceBytes)
		return "", "", &sourceTooLargeError{NodeID: n.NodeID, Path: full}
	}
	if gerr != nil {
		return "", "", fmt.Errorf("read %s: %w", full, gerr)
	}
	return content, "github", nil
}
