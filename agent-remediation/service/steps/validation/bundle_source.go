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

// CodeBundleSource is the context source `code_bundle_source`: the failing
// node's source as the release holds it, with the location it was found at.
type CodeBundleSource struct {
	candidates       ports.CandidateSourceReader
	source           ports.SourceReader
	locator          ports.NodeLocator
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.ContextSource = (*CodeBundleSource)(nil)

// NewCodeBundleSource builds the code_bundle_source step. serviceRepoPaths maps
// a service name to its path inside the repository. It calls no port until
// Gather.
func NewCodeBundleSource(candidates ports.CandidateSourceReader, source ports.SourceReader, locator ports.NodeLocator, serviceRepoPaths map[string]string, logger *slog.Logger) *CodeBundleSource {
	return &CodeBundleSource{
		candidates:       candidates,
		source:           source,
		locator:          locator,
		serviceRepoPaths: serviceRepoPaths,
		logger:           loggerOrDefault(logger),
	}
}

// Meta describes the step.
func (*CodeBundleSource) Meta() step.Meta {
	return step.Meta{
		Name: "code_bundle_source",
		Kind: step.KindContext,
		Description: "Reads the failing dbt node's source from the release's code bundle, falling back to the repository at the trigger's commit, " +
			"with the file path and service it was found for. Declines a node whose source is not a dbt model.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyCodeBundleSource},
	}
}

// Gather finds where the node's source lives, then reads it: the location the
// trigger carries is preferred, which release-controller stamps from the
// candidate topology, and only a trigger that carries none falls back to the
// promoted graph, because the rejected release was never promoted. That lookup
// is best-effort: a failure leaves the location empty, which only makes the
// source fix degrade to the candidate. The source itself is the bundle's, with
// the repository read as the fallback. A permanent miss leaves the source empty
// and is not an error, and a transient read error is returned so the work is
// tried again. A source that is not a dbt model declines the cluster.
func (s *CodeBundleSource) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	n := f.Target
	filePath, serviceName := n.FilePath, n.Service
	if filePath == "" {
		var err error
		filePath, serviceName, err = s.locator.Locate(ctx, n.NodeID)
		if err != nil {
			s.logger.Warn("node location unavailable; source fix will degrade to candidate",
				"node", n.NodeID, "error", err)
			filePath, serviceName = "", ""
		}
	}

	source, origin, err := s.resolve(ctx, f, filePath, serviceName)
	if errors.Is(err, errNonDbtCandidate) {
		s.logger.Info("validation fix: failing node's candidate source is not a dbt model; skipping",
			"node", n.NodeID, "error", err)
		return step.ContextOutput{Declined: true}, nil
	}
	if err != nil {
		return step.ContextOutput{}, err
	}
	if origin != "" {
		s.logger.Info("validation fix: candidate source resolved", "node", n.NodeID, "source_origin", origin)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyCodeBundleSource: remediation.CandidateSource{
			Source:   source,
			Origin:   origin,
			FilePath: filePath,
			Service:  serviceName,
		},
	}}, nil
}

// resolve returns the failing candidate's raw source. Order: the release's code
// bundle (exact failing source, keyed by unique id, no path mapping needed),
// then the repo file at the release's commit, then "". A bundle entry that is
// not a dbt node returns errNonDbtCandidate immediately; every permanent bundle
// miss, including a bundle for a different release and a dbt entry with no
// source text, walks down to the repo fallback instead. There, an empty file
// path, an empty service name, or a service with no entry in serviceRepoPaths
// still degrades quietly to an unresolved source, since these are location gaps
// and say nothing about the node's kind. Only when the trigger carries no node
// type does a resolved path that does not end in ".sql" also return
// errNonDbtCandidate: a trigger that does carry a node type is trusted over the
// extension and always proceeds to the read, where any error degrades quietly.
func (s *CodeBundleSource) resolve(ctx context.Context, f remediation.Failure, filePath, serviceName string) (source, origin string, err error) {
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
	if filePath == "" || serviceName == "" {
		return "", "", nil
	}
	prefix, ok := s.serviceRepoPaths[serviceName]
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
	if gerr != nil {
		s.logger.Warn("repo fallback read failed; using candidate proposal",
			"node", n.NodeID, "path", full, "error", gerr)
		return "", "", nil
	}
	return content, "github", nil
}
