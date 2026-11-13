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

// ErrUnmappedEdit marks an edit whose path lies outside every service this
// install configures a repository path for. Nothing downstream can run such an
// edit: a verification run is submitted for one service, and the overlay it
// lays down is relative to that service's project.
var ErrUnmappedEdit = errors.New("edit belongs to no configured service")

// ErrMixedManifestKinds marks a service whose edits would have to be verified
// as two different manifest kinds at once: a packaged python contract plus at
// least one edit to something that is not a python node's contract. A
// verification run parses one kind, so whichever were chosen would verify only
// half the change and silently ignore the rest.
var ErrMixedManifestKinds = errors.New("mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind")

// ErrDuplicateEditPath marks a try carrying two edits to one repository path. A
// source overlay holds one file per path and a python service is verified by
// one packaged contract, so the second edit would silently replace the first.
var ErrDuplicateEditPath = errors.New("two edits change the same file")

// The finding codes of the three refusals of a try no verification run could
// judge.
const (
	CodeEditOutsideServices = "edit_outside_services"
	CodePathEditedTwice     = "path_edited_twice"
	CodeMixedManifestKinds  = "mixed_manifest_kinds"
)

// RefusalFinding reports the finding for an error Group returned: its code and
// its message, the error's text. It reports false for any other error.
func RefusalFinding(err error) (remediation.Finding, bool) {
	var code string
	switch {
	case errors.Is(err, ErrUnmappedEdit):
		code = CodeEditOutsideServices
	case errors.Is(err, ErrDuplicateEditPath):
		code = CodePathEditedTwice
	case errors.Is(err, ErrMixedManifestKinds):
		code = CodeMixedManifestKinds
	default:
		return remediation.Finding{}, false
	}
	return remediation.Finding{Code: code, Message: err.Error()}, true
}

// ServiceEdits is the part of a try one verification run judges: the edits that
// land in one service, with the packaged contract that verifies them when the
// service is a python one. Prefix is the service's repository path, which a
// dbt run's overlay paths are relative to.
type ServiceEdits struct {
	Service  string
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

// contractsByService maps a service to the packaged contract of the cluster
// whose first edit is in it. The service is derived from the cluster's own
// edit paths, exactly as the edits are grouped, so a contract and the edits it
// packages always end up on the same verification run. A later cluster of the
// same service replaces an earlier one's contract.
func contractsByService(serviceRepoPaths map[string]string, drafts []step.ClusterDraft) map[string][]byte {
	contracts := map[string][]byte{}
	for _, d := range drafts {
		if len(d.Draft.Edits) == 0 {
			continue
		}
		contract, ok := remediation.Get[remediation.Bytes](d.Evidence, remediation.KeyPackagedContract)
		if !ok || contract == nil {
			continue
		}
		if service, _, ok := proposal.ServiceForPath(serviceRepoPaths, d.Draft.Edits[0].Path); ok {
			contracts[service] = contract
		}
	}
	return contracts
}

// FirstUnmapped returns the first edit no configured service owns.
func FirstUnmapped(serviceRepoPaths map[string]string, edits []remediation.Edit) (remediation.Edit, bool) {
	for _, e := range edits {
		if _, _, ok := proposal.ServiceForPath(serviceRepoPaths, e.Path); !ok {
			return e, true
		}
	}
	return remediation.Edit{}, false
}

// FirstDuplicate returns the first edit to a path an earlier edit already
// changed. An edit no service owns takes no part: it is refused as unmapped.
func FirstDuplicate(serviceRepoPaths map[string]string, edits []remediation.Edit) (remediation.Edit, bool) {
	seen := map[string]bool{}
	for _, e := range edits {
		if _, _, ok := proposal.ServiceForPath(serviceRepoPaths, e.Path); !ok {
			continue
		}
		if seen[e.Path] {
			return e, true
		}
		seen[e.Path] = true
	}
	return remediation.Edit{}, false
}

// FirstMixed returns the first service, in name order, that has a packaged
// contract and an edit that is not the contract of a python node the trigger
// names, with that edit. An edit qualifies as a contract edit only when it
// changes the source of a failing node the trigger carries and that node is a
// python kind.
func FirstMixed(serviceRepoPaths map[string]string, t remediation.Trigger, drafts []step.ClusterDraft) (service string, edit remediation.Edit, found bool) {
	contracts := contractsByService(serviceRepoPaths, drafts)
	byService := map[string][]remediation.Edit{}
	for _, e := range EditsOf(drafts) {
		if s, _, ok := proposal.ServiceForPath(serviceRepoPaths, e.Path); ok {
			byService[s] = append(byService[s], e)
		}
	}
	services := make([]string, 0, len(contracts))
	for s := range contracts {
		services = append(services, s)
	}
	sort.Strings(services)
	for _, s := range services {
		for _, e := range byService[s] {
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

// Group splits a try's edits into one ServiceEdits per edited service, sorted
// by service name. It refuses, with ErrUnmappedEdit, ErrDuplicateEditPath or
// ErrMixedManifestKinds, wrapped with the path or service as the cross-cluster
// checks report them, a try no verification run could judge.
func Group(serviceRepoPaths map[string]string, t remediation.Trigger, drafts []step.ClusterDraft) ([]ServiceEdits, error) {
	edits := EditsOf(drafts)
	if e, ok := FirstUnmapped(serviceRepoPaths, edits); ok {
		return nil, UnmappedError(e)
	}
	if e, ok := FirstDuplicate(serviceRepoPaths, edits); ok {
		return nil, DuplicateError(e)
	}
	if s, _, ok := FirstMixed(serviceRepoPaths, t, drafts); ok {
		return nil, MixedError(s)
	}
	contracts := contractsByService(serviceRepoPaths, drafts)
	byService := map[string]*ServiceEdits{}
	for _, e := range edits {
		service, prefix, _ := proposal.ServiceForPath(serviceRepoPaths, e.Path)
		g, ok := byService[service]
		if !ok {
			g = &ServiceEdits{Service: service, Prefix: prefix, Contract: contracts[service]}
			byService[service] = g
		}
		g.Edits = append(g.Edits, e)
	}
	out := make([]ServiceEdits, 0, len(byService))
	for _, g := range byService {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out, nil
}

// UnmappedError is the refusal of an edit no configured service owns.
func UnmappedError(e remediation.Edit) error {
	return fmt.Errorf("%w: %s", ErrUnmappedEdit, e.Path)
}

// DuplicateError is the refusal of an edit to a path an earlier edit changed.
func DuplicateError(e remediation.Edit) error {
	return fmt.Errorf("%w: %s", ErrDuplicateEditPath, e.Path)
}

// MixedError is the refusal of a service that mixes a python contract with
// edits of another manifest kind.
func MixedError(service string) error {
	return fmt.Errorf("service %s %w", service, ErrMixedManifestKinds)
}
