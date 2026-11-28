package cross

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// The names the checks of this file register under.
const (
	nameEditsMapToAProject flow.StepName = "edits_map_to_a_project"
	nameNoPathEditedTwice  flow.StepName = "no_path_edited_twice"
	nameOneManifestKind    flow.StepName = "one_manifest_kind"
)

var crossStage = []flow.Stage{flow.StageCrossCluster}

// finish wraps at most one finding as the result of the check called name. Each
// check reports the first edit it refuses, as one refusal.
func finish(name flow.StepName, f *remediation.Finding) remediation.CheckResult {
	res := remediation.CheckResult{Check: string(name)}
	if f != nil {
		res.Findings = []remediation.Finding{*f}
	}
	return res
}

// --- edits_map_to_a_project ---

// EditsMapToAProject is the check `edits_map_to_a_project`: every edit of the
// try lies inside a project the install configures a repository path for.
type EditsMapToAProject struct {
	projectRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*EditsMapToAProject)(nil)

// NewEditsMapToAProject builds the edits_map_to_a_project step. projectRepoPaths
// maps a project name to its repository path.
func NewEditsMapToAProject(projectRepoPaths map[string]string, logger *slog.Logger) *EditsMapToAProject {
	return &EditsMapToAProject{projectRepoPaths: projectRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*EditsMapToAProject) Meta() step.Meta {
	return step.Meta{
		Name:        nameEditsMapToAProject,
		Kind:        step.KindCheck,
		Description: "Fails a try with an edit whose path no configured project owns, because a release check is submitted for one project.",
		Stages:      crossStage,
	}
}

// CheckAll finds one edit_outside_projects finding for the first edit, in
// cluster order, that no configured project owns.
func (c *EditsMapToAProject) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	e, ok := shared.FirstUnmapped(c.projectRepoPaths, shared.EditsOf(in.Drafts))
	if !ok {
		return finish(nameEditsMapToAProject, nil), nil
	}
	c.logger.Error("an edit belongs to no configured project; the try cannot be verified",
		"release", in.Trigger.ReleaseID, "path", e.Path, "target", e.TargetNodeID)
	return finish(nameEditsMapToAProject, &remediation.Finding{
		Code: shared.CodeEditOutsideProjects, Message: shared.UnmappedError(e).Error(), File: e.Path,
	}), nil
}

// --- no_path_edited_twice ---

// NoPathEditedTwice is the check `no_path_edited_twice`: no two edits of the
// try change one repository path.
type NoPathEditedTwice struct {
	projectRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*NoPathEditedTwice)(nil)

// NewNoPathEditedTwice builds the no_path_edited_twice step.
func NewNoPathEditedTwice(projectRepoPaths map[string]string, logger *slog.Logger) *NoPathEditedTwice {
	return &NoPathEditedTwice{projectRepoPaths: projectRepoPaths, logger: shared.LoggerOrDefault(logger)}
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
// earlier edit of the try changed. An edit no project owns is left to
// edits_map_to_a_project.
func (c *NoPathEditedTwice) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	e, ok := shared.FirstDuplicate(c.projectRepoPaths, shared.EditsOf(in.Drafts))
	if !ok {
		return finish(nameNoPathEditedTwice, nil), nil
	}
	c.logger.Error("two of the try's edits change the same file; the try cannot be verified",
		"release", in.Trigger.ReleaseID, "path", e.Path, "target", e.TargetNodeID)
	return finish(nameNoPathEditedTwice, &remediation.Finding{
		Code: shared.CodePathEditedTwice, Message: shared.DuplicateError(e).Error(), File: e.Path,
	}), nil
}

// --- one_manifest_kind ---

// OneManifestKind is the check `one_manifest_kind`: no project has its python
// contract and an edit to anything else in one try. It reads the
// packaged_contract evidence of each cluster when there is one, and every
// other cluster simply has none.
type OneManifestKind struct {
	projectRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.CrossCheck = (*OneManifestKind)(nil)

// NewOneManifestKind builds the one_manifest_kind step.
func NewOneManifestKind(projectRepoPaths map[string]string, logger *slog.Logger) *OneManifestKind {
	return &OneManifestKind{projectRepoPaths: projectRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*OneManifestKind) Meta() step.Meta {
	return step.Meta{
		Name: nameOneManifestKind,
		Kind: step.KindCheck,
		Description: "Fails a try that, in one project, packages a python contract and also edits something that is not a python node's " +
			"contract, because a release check verifies one manifest kind.",
		Stages: crossStage,
	}
}

// CheckAll finds one mixed_manifest_kinds finding for the first project, in
// name order, whose packaged contract comes with an edit that is not the
// contract of a failing python node.
func (c *OneManifestKind) CheckAll(_ context.Context, in step.CrossInput) (remediation.CheckResult, error) {
	project, e, ok := shared.FirstMixed(c.projectRepoPaths, in.Trigger, in.Drafts)
	if !ok {
		return finish(nameOneManifestKind, nil), nil
	}
	c.logger.Error("a project's edits span two manifest kinds; the try cannot be verified",
		"release", in.Trigger.ReleaseID, "project", project, "edit", e.Path, "target", e.TargetNodeID)
	return finish(nameOneManifestKind, &remediation.Finding{
		Code: shared.CodeMixedManifestKinds, Message: shared.MixedError(project).Error(), File: e.Path,
	}), nil
}
