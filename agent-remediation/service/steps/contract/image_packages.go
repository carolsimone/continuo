package contract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// ImagePackages is the context source `image_packages`: the package list of
// the rejected release's image of the failing node's project, which the
// contract_packages check packs the fix's bundle with.
type ImagePackages struct {
	images ports.ImagePackagesSource
	logger *slog.Logger
}

var _ step.ContextSource = (*ImagePackages)(nil)

// NewImagePackages builds the image_packages step. It calls no port until
// Gather.
func NewImagePackages(images ports.ImagePackagesSource, logger *slog.Logger) *ImagePackages {
	return &ImagePackages{images: images, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*ImagePackages) Meta() step.Meta {
	return step.Meta{
		Name: "image_packages",
		Kind: step.KindContext,
		Description: "Reads the package list of the rejected release's image of the failing node's project, from the release's contract bundle " +
			"or, for a release with none, from the project's odcs-bundle.json. Declines a release that records no package list for the project, " +
			"since no fix could be packed for it.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyImagePackages},
	}
}

// Gather returns the package list. A release that records none for the project
// is known before any draft and no draft can change it, so the cluster is
// declined with the reason, which is logged; any other read error is the
// step's error, so the runner tries it again.
func (s *ImagePackages) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	packages, err := s.images.ImagePackages(ctx, f.Trigger.ReleaseID, f.Target.Project)
	if errors.Is(err, ports.ErrNoImagePackages) {
		reason := err.Error()
		s.logger.Info(reason, "release", f.Trigger.ReleaseID, "node", f.Target.NodeID, "project", f.Target.Project)
		return step.ContextOutput{Declined: true, Decline: reason}, nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("read the image packages of release %s for %s: %w", f.Trigger.ReleaseID, f.Target.Project, err)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyImagePackages: remediation.ImagePackages(packages),
	}}, nil
}
