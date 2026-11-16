// Package serialization maps remediation's domain trigger onto the wire payload
// of remediation.requested:v3, keeping the domain event package free of json
// struct tags and of the shared event types. It sits outside adapters/ so the
// classifier handler (which builds and marshals the trigger) may map through it
// without importing an adapter, and outside domain/ so the tags live away from
// the domain type.
package serialization

import (
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/remediation/domain/event"
)

func changedAncestorsFromDomain(in []event.ChangedAncestor) []pkgevents.RemediationRequestedAncestor {
	if in == nil {
		return nil
	}
	out := make([]pkgevents.RemediationRequestedAncestor, len(in))
	for i, a := range in {
		out[i] = pkgevents.RemediationRequestedAncestor(a)
	}
	return out
}

func unfixableFromDomain(in *event.Unfixable) *pkgevents.Unfixable {
	if in == nil {
		return nil
	}
	return &pkgevents.Unfixable{Reason: string(in.Reason), Detail: in.Detail}
}

func nodesFromDomain(in []event.FailingNode) []pkgevents.RemediationRequestedNode {
	if in == nil {
		return nil
	}
	out := make([]pkgevents.RemediationRequestedNode, len(in))
	for i, n := range in {
		out[i] = pkgevents.RemediationRequestedNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			Category:             n.Category,
			ErrorSignature:       n.ErrorSignature,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
			ChangedAncestors:     changedAncestorsFromDomain(n.ChangedAncestors),
			Unfixable:            unfixableFromDomain(n.Unfixable),
		}
	}
	return out
}

// RemediationRequestedFromDomain maps the domain trigger to the payload of a
// remediation.requested:v3 entry.
func RemediationRequestedFromDomain(e event.RemediationRequested) pkgevents.RemediationRequested {
	return pkgevents.RemediationRequested{
		Source:           string(e.Source),
		ReleaseID:        e.ReleaseID,
		RemediationRound: e.RemediationRound,
		Repo:             e.Repo,
		CommitSHA:        e.CommitSHA,
		CodeBundleURI:    e.CodeBundleURI,
		ClassifiedAt:     e.ClassifiedAt,
		Nodes:            nodesFromDomain(e.Nodes),
	}
}
