package contract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// FailingSiblings is the context source `failing_siblings`: it refuses a
// failing python node whose contract directory holds another node that also
// failed the original release.
type FailingSiblings struct {
	archive  ports.RepoArchive
	locator  ports.ContractLocator
	releases ports.ReleaseReader
	logger   *slog.Logger
}

var _ step.ContextSource = (*FailingSiblings)(nil)

// NewFailingSiblings builds the failing_siblings step. It calls no port until
// Gather.
func NewFailingSiblings(archive ports.RepoArchive, locator ports.ContractLocator, releases ports.ReleaseReader, logger *slog.Logger) *FailingSiblings {
	return &FailingSiblings{archive: archive, locator: locator, releases: releases, logger: loggerOrDefault(logger)}
}

// Meta describes the step.
func (*FailingSiblings) Meta() step.Meta {
	return step.Meta{
		Name: "failing_siblings",
		Kind: step.KindContext,
		Description: "Declines a failing python node when another node declared in the same contract directory also failed validation in the release, " +
			"because a fix is packaged from that whole directory and its verification run would be failed by the sibling. Provides no evidence.",
		Needs: []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyContractFile},
	}
}

// Gather is the check that a fix for this node is verifiable at all. The fix
// is packaged from the whole contract directory, so its verification run
// re-runs every node declared in it. Another node in that directory that also
// failed the original release fails that run however correct the fix is, so
// the cluster is declined up front rather than spending a full verification
// run, and the single release-queue slot it takes, proving it.
//
// The release's failing nodes are read from the original release, and each
// other id is resolved against a fresh checkout of the repository to see where
// it is declared. A node no contract yaml declares, such as a dbt model or a
// python node in another service, is not packaged by this fix and cannot fail
// its verification run. The checkout is removed before returning.
func (s *FailingSiblings) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	file, ok := remediation.Get[remediation.ContractFile](in.Evidence, remediation.KeyContractFile)
	if !ok {
		return step.ContextOutput{}, fmt.Errorf("failing_siblings: the evidence holds no contract file")
	}
	f := in.Failure
	root, cleanup, reason, err := fetchCheckout(ctx, s.archive, f)
	if err != nil {
		return step.ContextOutput{}, err
	}
	if reason != "" {
		return declined(s.logger, f, reason), nil
	}
	defer cleanup()

	sibling, err := s.siblingFailure(ctx, f, file.ContractDir, root)
	if err != nil {
		return step.ContextOutput{}, err
	}
	if sibling == "" {
		return step.ContextOutput{}, nil
	}
	return declined(s.logger, f, fmt.Sprintf(
		"%s also failed validation in release %s and is declared in the same contract directory (%s) as %s. "+
			"A fix is packaged from that whole directory, so any verification run of it would re-run both nodes "+
			"and be failed by %s. Fix both nodes together.",
		sibling, f.Trigger.ReleaseID, file.ContractDir, f.Target.NodeID, sibling)), nil
}

// siblingFailure returns the id of another node of the original release's
// failing set that is declared in contractDir, or "" when there is none. A
// release read that fails is returned rather than guessed past, so the work is
// tried again: this decides whether the verification run can mean anything at
// all.
func (s *FailingSiblings) siblingFailure(ctx context.Context, f remediation.Failure, contractDir, root string) (string, error) {
	releaseID, nodeID := f.Trigger.ReleaseID, f.Target.NodeID
	nodeErrors, err := s.releases.FailingNodes(ctx, releaseID)
	if err != nil {
		return "", fmt.Errorf("read failing nodes of release %s: %w", releaseID, err)
	}
	others := make([]string, 0, len(nodeErrors))
	for id := range nodeErrors {
		if id != nodeID {
			others = append(others, id)
		}
	}
	// Sorted so a release with several siblings always names the same one, and
	// a re-run of the same attempt records the same rationale.
	sort.Strings(others)

	for _, id := range others {
		schema, table, ok := splitNodeID(id)
		if !ok {
			continue
		}
		other, lerr := s.locator.Locate(root, schema, table)
		if lerr != nil {
			// Not declared anywhere, declared twice, or unsearchable: in none of
			// those cases is this node known to share the directory being
			// packaged, and uncertainty about someone else's node is not a
			// reason to abandon a fix for this one.
			if !errors.Is(lerr, ports.ErrNodeNotDeclared) {
				s.logger.Warn("could not place another failing node; proceeding as if it is declared elsewhere",
					"node", id, "release", releaseID, "error", lerr)
			}
			continue
		}
		if other.ContractDir == contractDir {
			return id, nil
		}
	}
	return "", nil
}
