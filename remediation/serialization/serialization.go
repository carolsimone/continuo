// Package serialization maps remediation's domain trigger onto the wire payload
// of heal.triggered:v1, keeping the domain event package free of json
// struct tags and of the shared event types. It sits outside adapters/ so the
// classifier handler (which builds and marshals the trigger) may map through it
// without importing an adapter, and outside domain/ so the tags live away from
// the domain type.
package serialization

import (
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/remediation/domain/event"
)

func changedAncestorsFromDomain(in []event.ChangedAncestor) []pkgevents.HealTriggeredAncestor {
	if in == nil {
		return nil
	}
	out := make([]pkgevents.HealTriggeredAncestor, len(in))
	for i, a := range in {
		out[i] = pkgevents.HealTriggeredAncestor(a)
	}
	return out
}

func unfixableFromDomain(in *event.Unfixable) *pkgevents.Unfixable {
	if in == nil {
		return nil
	}
	return &pkgevents.Unfixable{Reason: string(in.Reason), Detail: in.Detail}
}

func nodesFromDomain(in []event.FailingNode) []pkgevents.HealTriggeredNode {
	if in == nil {
		return nil
	}
	out := make([]pkgevents.HealTriggeredNode, len(in))
	for i, n := range in {
		out[i] = pkgevents.HealTriggeredNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			Category:             n.Category,
			ErrorSignature:       n.ErrorSignature,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Project:              n.Project,
			NodeType:             n.NodeType,
			OtherProject:         n.OtherProject,
			OtherFilePath:        n.OtherFilePath,
			ChangedAncestors:     changedAncestorsFromDomain(n.ChangedAncestors),
			Unfixable:            unfixableFromDomain(n.Unfixable),
		}
	}
	return out
}

// HealTriggeredFromDomain maps the domain trigger to the payload of a
// heal.triggered:v1 entry.
func HealTriggeredFromDomain(e event.HealTriggered) pkgevents.HealTriggered {
	return pkgevents.HealTriggered{
		Source:        string(e.Source),
		ReleaseID:     e.ReleaseID,
		Round:         e.Round,
		Repo:          e.Repo,
		CommitSHA:     e.CommitSHA,
		CodeBundleURI: e.CodeBundleURI,
		ClassifiedAt:  e.ClassifiedAt,
		Nodes:         nodesFromDomain(e.Nodes),
	}
}
