package cross

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// The names the checks of this file register under.
const (
	nameEditsMapToAService workflow.StepName = "edits_map_to_a_service"
	nameNoPathEditedTwice  workflow.StepName = "no_path_edited_twice"
	nameOneManifestKind    workflow.StepName = "one_manifest_kind"
)

var crossStage = []workflow.Stage{workflow.StageCrossCluster}

// finish wraps at most one finding as the result of the check called name. Each
// check reports the first edit it refuses, as one refusal.
func finish(name workflow.StepName, f *remediation.Finding) remediation.CheckResult {
	res := remediation.CheckResult{Check: string(name)}
	if f != nil {
		res.Findings = []remediation.Finding{*f}
	}
	return res
}

// --- edits_map_to_a_service ---

// EditsMapToAService is the check `edits_map_to_a_service`: every edit of the
// try lies inside a service the install configures a repository path for.
type EditsMapToAService struct {
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*EditsMapToAService)(nil)

// NewEditsMapToAService builds the edits_map_to_a_service step. serviceRepoPaths
// maps a service name to its repository path.
func NewEditsMapToAService(serviceRepoPaths map[string]string, logger *slog.Logger) *EditsMapToAService {
	return &EditsMapToAService{serviceRepoPaths: serviceRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*EditsMapToAService) Meta() step.Meta {
	return step.Meta{
		Name:        nameEditsMapToAService,
		Kind:        step.KindCheck,
		Description: "Fails a try with an edit whose path no configured service owns, because a verification run is submitted for one service.",
		Stages:      crossStage,
	}
}

// CheckAll finds one edit_outside_services finding for the first edit, in
// cluster order, that no configured service owns.
func (c *EditsMapToAService) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	e, ok := shared.FirstUnmapped(c.serviceRepoPaths, shared.EditsOf(in.Drafts))
	if !ok {
		return finish(nameEditsMapToAService, nil), nil
	}
	c.logger.Error("an edit belongs to no configured service; the attempt cannot be verified",
		"release", in.Trigger.ReleaseID, "path", e.Path, "target", e.TargetNodeID)
	return finish(nameEditsMapToAService, &remediation.Finding{
		Code: shared.CodeEditOutsideServices, Message: shared.UnmappedError(e).Error(), File: e.Path,
	}), nil
}

// --- no_path_edited_twice ---

// NoPathEditedTwice is the check `no_path_edited_twice`: no two edits of the
// try change one repository path.
type NoPathEditedTwice struct {
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*NoPathEditedTwice)(nil)

// NewNoPathEditedTwice builds the no_path_edited_twice step.
func NewNoPathEditedTwice(serviceRepoPaths map[string]string, logger *slog.Logger) *NoPathEditedTwice {
	return &NoPathEditedTwice{serviceRepoPaths: serviceRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*NoPathEditedTwice) Meta() step.Meta {
	return step.Meta{
		Name:        nameNoPathEditedTwice,
		Kind:        step.KindCheck,
		Description: "Fails a try with two edits to one file, because a source overlay holds one file per path and the second edit would replace the first.",
		Stages:      crossStage,
	}
}

// CheckAll finds one path_edited_twice finding for the first edit to a path an
// earlier edit of the try changed. An edit no service owns is left to
// edits_map_to_a_service.
func (c *NoPathEditedTwice) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	e, ok := shared.FirstDuplicate(c.serviceRepoPaths, shared.EditsOf(in.Drafts))
	if !ok {
		return finish(nameNoPathEditedTwice, nil), nil
	}
	c.logger.Error("two of the attempt's edits change the same file; the attempt cannot be verified",
		"release", in.Trigger.ReleaseID, "path", e.Path, "target", e.TargetNodeID)
	return finish(nameNoPathEditedTwice, &remediation.Finding{
		Code: shared.CodePathEditedTwice, Message: shared.DuplicateError(e).Error(), File: e.Path,
	}), nil
}

// --- one_manifest_kind ---

// OneManifestKind is the check `one_manifest_kind`: no service has its python
// contract and an edit to anything else in one try. It reads the
// packaged_contract evidence of each cluster when there is one, and every
// other cluster simply has none.
type OneManifestKind struct {
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*OneManifestKind)(nil)

// NewOneManifestKind builds the one_manifest_kind step.
func NewOneManifestKind(serviceRepoPaths map[string]string, logger *slog.Logger) *OneManifestKind {
	return &OneManifestKind{serviceRepoPaths: serviceRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*OneManifestKind) Meta() step.Meta {
	return step.Meta{
		Name: nameOneManifestKind,
		Kind: step.KindCheck,
		Description: "Fails a try that, in one service, packages a python contract and also edits something that is not a python node's " +
			"contract, because a verification run verifies one manifest kind.",
		Stages: crossStage,
	}
}

// CheckAll finds one mixed_manifest_kinds finding for the first service, in
// name order, whose packaged contract comes with an edit that is not the
// contract of a failing python node.
func (c *OneManifestKind) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	service, e, ok := shared.FirstMixed(c.serviceRepoPaths, in.Trigger, in.Drafts)
	if !ok {
		return finish(nameOneManifestKind, nil), nil
	}
	c.logger.Error("a service's edits span two manifest kinds; the attempt cannot be verified",
		"release", in.Trigger.ReleaseID, "service", service, "edit", e.Path, "target", e.TargetNodeID)
	return finish(nameOneManifestKind, &remediation.Finding{
		Code: shared.CodeMixedManifestKinds, Message: shared.MixedError(service).Error(), File: e.Path,
	}), nil
}
