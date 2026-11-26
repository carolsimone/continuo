package shared

import (
	"errors"
	"fmt"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// ErrUnmappedEdit marks an edit whose path lies outside every project this
// install configures a repository path for. Nothing downstream can run such an
// edit: a verification run is submitted for one project, and the overlay it
// lays down is relative to that project's directory.
var ErrUnmappedEdit = errors.New("edit belongs to no configured project")

// ErrMixedManifestKinds marks a project whose edits would have to be verified
// as two different manifest kinds at once: a packaged python contract plus at
// least one edit to something that is not a python node's contract. A
// verification run parses one kind, so whichever were chosen would verify only
// half the change and silently ignore the rest.
var ErrMixedManifestKinds = errors.New("mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind")

// ErrDuplicateEditPath marks a try carrying two edits to one repository path. A
// source overlay holds one file per path and a python project is verified by
// one packaged contract, so the second edit would silently replace the first.
var ErrDuplicateEditPath = errors.New("two edits change the same file")

// The finding codes of the three refusals of a try no verification run could
// judge.
const (
	CodeEditOutsideProjects = "edit_outside_projects"
	CodePathEditedTwice     = "path_edited_twice"
	CodeMixedManifestKinds  = "mixed_manifest_kinds"
)

// RefusalFinding reports the finding for an error Group returned: its code and
// its message, the error's text. It reports false for any other error.
func RefusalFinding(err error) (remediation.Finding, bool) {
	var code string
	switch {
	case errors.Is(err, ErrUnmappedEdit):
		code = CodeEditOutsideProjects
	case errors.Is(err, ErrDuplicateEditPath):
		code = CodePathEditedTwice
	case errors.Is(err, ErrMixedManifestKinds):
		code = CodeMixedManifestKinds
	default:
		return remediation.Finding{}, false
	}
	return remediation.Finding{Code: code, Message: err.Error()}, true
}

// ProjectEdits is the part of a try one verification run judges: the edits that
// land in one project, with the packaged contract that verifies them when the
// project is a python one. Prefix is the project's repository path, which a
// dbt run's overlay paths are relative to.
type ProjectEdits struct {
	Project  string
	Prefix   string
	Edits    []remediation.Edit
	Contract []byte
}

// EditsOf returns every cluster draft's edits, in cluster order.
func EditsOf(drafts []step.ClusterDraft) []remediation.Edit {
	var edits []remediation.Edit
	for _, d := range drafts {
		edits = append(edits, d.Draft.Edits...)
	}
	return edits
}

// contractsByProject maps a project to the packaged contract of the cluster
// whose first edit is in it. The project is derived from the cluster's own
// edit paths, exactly as the edits are grouped, so a contract and the edits it
// packages always end up on the same verification run. A later cluster of the
// same project replaces an earlier one's contract.
func contractsByProject(projectRepoPaths map[string]string, drafts []step.ClusterDraft) map[string][]byte {
	contracts := map[string][]byte{}
	for _, d := range drafts {
		if len(d.Draft.Edits) == 0 {
			continue
		}
		contract, ok := remediation.Get[remediation.Bytes](d.Evidence, remediation.KeyPackagedContract)
		if !ok || contract == nil {
			continue
		}
		if project, _, ok := proposal.ProjectForPath(projectRepoPaths, d.Draft.Edits[0].Path); ok {
			contracts[project] = contract
		}
	}
	return contracts
}

// FirstUnmapped returns the first edit no configured project owns.
func FirstUnmapped(projectRepoPaths map[string]string, edits []remediation.Edit) (remediation.Edit, bool) {
	for _, e := range edits {
		if _, _, ok := proposal.ProjectForPath(projectRepoPaths, e.Path); !ok {
			return e, true
		}
	}
	return remediation.Edit{}, false
}

// FirstDuplicate returns the first edit to a path an earlier edit already
// changed. An edit no project owns takes no part: it is refused as unmapped.
func FirstDuplicate(projectRepoPaths map[string]string, edits []remediation.Edit) (remediation.Edit, bool) {
	seen := map[string]bool{}
	for _, e := range edits {
		if _, _, ok := proposal.ProjectForPath(projectRepoPaths, e.Path); !ok {
			continue
		}
		if seen[e.Path] {
			return e, true
		}
		seen[e.Path] = true
	}
	return remediation.Edit{}, false
}

// FirstMixed returns the first project, in name order, that has a packaged
// contract and an edit that is not the contract of a python node the trigger
// names, with that edit. An edit qualifies as a contract edit only when it
// changes the source of a failing node the trigger carries and that node is a
// python kind.
func FirstMixed(projectRepoPaths map[string]string, t remediation.Trigger, drafts []step.ClusterDraft) (project string, edit remediation.Edit, found bool) {
	contracts := contractsByProject(projectRepoPaths, drafts)
	byProject := map[string][]remediation.Edit{}
	for _, e := range EditsOf(drafts) {
		if s, _, ok := proposal.ProjectForPath(projectRepoPaths, e.Path); ok {
			byProject[s] = append(byProject[s], e)
		}
	}
	projects := make([]string, 0, len(contracts))
	for s := range contracts {
		projects = append(projects, s)
	}
	sort.Strings(projects)
	for _, s := range projects {
		for _, e := range byProject[s] {
			if !isPythonNodeEdit(t, e) {
				return s, e, true
			}
		}
	}
	return "", remediation.Edit{}, false
}

func isPythonNodeEdit(t remediation.Trigger, e remediation.Edit) bool {
	for _, n := range t.Nodes {
		if n.NodeID == e.TargetNodeID {
			return pkg_model.NodeType(n.NodeType).IsPython()
		}
	}
	return false
}

// Group splits a try's edits into one ProjectEdits per edited project, sorted
// by project name. It refuses, with ErrUnmappedEdit, ErrDuplicateEditPath or
// ErrMixedManifestKinds, wrapped with the path or project as the cross-cluster
// checks report them, a try no verification run could judge.
func Group(projectRepoPaths map[string]string, t remediation.Trigger, drafts []step.ClusterDraft) ([]ProjectEdits, error) {
	edits := EditsOf(drafts)
	if e, ok := FirstUnmapped(projectRepoPaths, edits); ok {
		return nil, UnmappedError(e)
	}
	if e, ok := FirstDuplicate(projectRepoPaths, edits); ok {
		return nil, DuplicateError(e)
	}
	if s, _, ok := FirstMixed(projectRepoPaths, t, drafts); ok {
		return nil, MixedError(s)
	}
	contracts := contractsByProject(projectRepoPaths, drafts)
	byProject := map[string]*ProjectEdits{}
	for _, e := range edits {
		project, prefix, _ := proposal.ProjectForPath(projectRepoPaths, e.Path)
		g, ok := byProject[project]
		if !ok {
			g = &ProjectEdits{Project: project, Prefix: prefix, Contract: contracts[project]}
			byProject[project] = g
		}
		g.Edits = append(g.Edits, e)
	}
	out := make([]ProjectEdits, 0, len(byProject))
	for _, g := range byProject {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Project < out[j].Project })
	return out, nil
}

// UnmappedError is the refusal of an edit no configured project owns.
func UnmappedError(e remediation.Edit) error {
	return fmt.Errorf("%w: %s", ErrUnmappedEdit, e.Path)
}

// DuplicateError is the refusal of an edit to a path an earlier edit changed.
func DuplicateError(e remediation.Edit) error {
	return fmt.Errorf("%w: %s", ErrDuplicateEditPath, e.Path)
}

// MixedError is the refusal of a project that mixes a python contract with
// edits of another manifest kind.
func MixedError(project string) error {
	return fmt.Errorf("project %s %w", project, ErrMixedManifestKinds)
}
