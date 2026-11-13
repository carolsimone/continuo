// Package upstream holds the steps of the upstream family: the fix of a
// shared-upstream or cross-service cluster, made at the changed ancestor the
// failing members share as their cause. The context source upstream_target
// reads the ancestor and finds where it lives, and the prompt step upstream
// asks the model for its corrected source.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"unicode/utf8"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// maxDiffBytes caps the change diff (what this release changed in the ancestor
// relative to the code that runs now) so one large refactor cannot dominate the
// prompt.
const maxDiffBytes = 8192

// Target is the context source `upstream_target`: the changed ancestor an
// upstream cluster's fix edits, with its source and where it lives.
type Target struct {
	candidates       ports.CandidateSourceReader
	locator          ports.NodeLocator
	versions         ports.VersionReader
	sanitizer        ports.LogSanitizer
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.ContextSource = (*Target)(nil)

// NewTarget builds the upstream_target step. serviceRepoPaths maps a service
// name to its path inside the repository. It calls no port until Gather.
func NewTarget(candidates ports.CandidateSourceReader, locator ports.NodeLocator, versions ports.VersionReader, sanitizer ports.LogSanitizer, serviceRepoPaths map[string]string, logger *slog.Logger) *Target {
	return &Target{
		candidates:       candidates,
		locator:          locator,
		versions:         versions,
		sanitizer:        sanitizer,
		serviceRepoPaths: serviceRepoPaths,
		logger:           loggerOrDefault(logger),
	}
}

// Meta describes the step.
func (*Target) Meta() step.Meta {
	return step.Meta{
		Name: "upstream_target",
		Kind: step.KindContext,
		Description: "Reads the changed ancestor of an upstream cluster from the release's code bundle, finds the file and service that declare it, " +
			"and diffs it against the version that runs now. Declines a cluster with no failing member, an ancestor that is not in the bundle, " +
			"is not a dbt model, cannot be located, or whose service has no repository path.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyUpstreamTarget},
	}
}

// Gather reads the ancestor from the code bundle, which holds it whether or not
// it failed itself because it changed in this release. Where it lives comes
// from the rejected release's own candidate topology when the trigger carries
// it, and from the promoted graph otherwise, so a brand-new ancestor, which the
// promoted graph cannot place, declines the cluster. Every other reason the
// ancestor cannot be edited also declines it: the cluster has no member, the
// bundle does not hold the ancestor, it is not a dbt model, or its service has
// no repository path. A transient bundle read error is returned so the work is
// tried again.
func (s *Target) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	targetID := f.TargetNodeID
	if len(f.Members) == 0 {
		return s.decline(targetID, "an upstream cluster needs at least one failing member"), nil
	}
	src, err := s.candidates.NodeSource(ctx, f.Trigger.CodeBundleURI, targetID, f.Trigger.ReleaseID)
	if errors.Is(err, ports.ErrNotFound) {
		return s.decline(targetID, "the changed ancestor's source is not in the release's code bundle"), nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("fetch code bundle for %s: %w", targetID, err)
	}
	if src.Runtime != ports.RuntimeDbt {
		return s.decline(targetID, fmt.Sprintf("the changed ancestor %s is not a dbt model (runtime %q)", targetID, src.Runtime)), nil
	}
	filePath, serviceName, err := s.locate(ctx, f)
	if err != nil || filePath == "" || serviceName == "" {
		return s.decline(targetID, fmt.Sprintf("the changed ancestor %s cannot be located in the promoted graph", targetID)), nil
	}
	if _, ok := s.serviceRepoPaths[serviceName]; !ok {
		return s.decline(targetID, fmt.Sprintf("service %q has no repository path mapping", serviceName)), nil
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyUpstreamTarget: remediation.UpstreamTarget{
			NodeID:        targetID,
			Source:        src.RawCode,
			FilePath:      filePath,
			Service:       serviceName,
			OwnChangeDiff: s.ownChangeDiff(ctx, targetID, src.RawCode),
		},
	}}, nil
}

// decline logs why the cluster falls back and returns the decision carrying the
// reason.
func (s *Target) decline(targetID, reason string) step.ContextOutput {
	s.logger.Info("upstream fix skipped; members fall back to independent fixes", "target", targetID, "reason", reason)
	return step.ContextOutput{Declined: true, Decline: reason}
}

// locate resolves where to edit the changed ancestor. The rejected release's own
// candidate location wins whenever the trigger carries it: this release may have
// renamed or moved the ancestor, in which case the promoted graph still points
// at the file it used to live in, and an edit routed there would rewrite source
// that no longer declares the node. The promoted graph is the fallback for a
// rejection that carries no per-node topology.
func (s *Target) locate(ctx context.Context, f remediation.Failure) (filePath, serviceName string, err error) {
	if filePath, serviceName = ancestorLocation(f.Trigger, f.TargetNodeID); filePath != "" {
		return filePath, serviceName, nil
	}
	return s.locator.Locate(ctx, f.TargetNodeID)
}

// ancestorLocation is where the rejected release's candidate topology declares
// the changed ancestor, as the trigger carries it on the failing nodes that
// descend from it. Both are empty when no node carries a complete location for
// it.
func ancestorLocation(t remediation.Trigger, ancestorID string) (filePath, serviceName string) {
	for _, n := range t.Nodes {
		for _, a := range n.ChangedAncestors {
			if a.NodeID == ancestorID && a.FilePath != "" && a.Service != "" {
				return a.FilePath, a.Service
			}
		}
	}
	return "", ""
}

// ownChangeDiff is the unified diff from the ancestor's promoted version to its
// candidate source, sanitized before it is truncated, like every other source
// string sent to the model, so a secret in a changed line is redacted rather
// than sent. It is empty when the ancestor has no promoted version (it is new)
// or the version read fails, which is logged.
func (s *Target) ownChangeDiff(ctx context.Context, nodeID, candidateSource string) string {
	cur, ok, err := s.versions.CurrentVersion(ctx, nodeID)
	if err != nil {
		s.logger.Warn("current version unavailable; omitting own-change diff", "node", nodeID, "error", err)
		return ""
	}
	if !ok {
		return ""
	}
	return truncateDiff(s.sanitizer.Sanitize(proposal.ComputeUnifiedDiff(cur.RawCode, candidateSource, nodeID)), maxDiffBytes)
}

// truncateDiff caps a diff at max bytes, appending a marker when it was cut. The
// cut is backed up to a UTF-8 rune boundary so truncation never splits a
// multibyte character, which would render as U+FFFD in the prompt.
func truncateDiff(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n… (diff truncated)"
}

func loggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}
