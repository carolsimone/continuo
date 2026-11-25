package serialization

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

// TriggerFromRequested maps the payload of a heal trigger to the trigger it
// carries, the unfixable nodes included. The tenant is not part of the payload,
// so TenantID is left empty for the caller to set from the message's envelope.
func TriggerFromRequested(p events.RemediationRequested) remediation.Trigger {
	t := remediation.Trigger{
		Source:        p.Source,
		ReleaseID:     p.ReleaseID,
		Round:         p.RemediationRound,
		Repo:          p.Repo,
		CommitSHA:     p.CommitSHA,
		CodeBundleURI: p.CodeBundleURI,
		Nodes:         make([]remediation.TriggerNode, 0, len(p.Nodes)),
	}
	for _, n := range p.Nodes {
		node := remediation.TriggerNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			ErrorSignature:       n.ErrorSignature,
			Category:             n.Category,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Project:              n.Project,
			NodeType:             n.NodeType,
			OtherProject:         n.OtherProject,
			OtherFilePath:        n.OtherFilePath,
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, remediation.ChangedAncestor(a))
		}
		if n.Unfixable != nil {
			node.Unfixable = &remediation.Unfixable{
				Reason: model.HandoverReason(n.Unfixable.Reason),
				Detail: n.Unfixable.Detail,
			}
		}
		t.Nodes = append(t.Nodes, node)
	}
	return t
}

// requestedFromTrigger is the payload of a trigger. The tenant is not part of
// the payload: it travels on the message's envelope. A trigger carries no
// classification time, so classified_at is empty.
func requestedFromTrigger(t remediation.Trigger) events.RemediationRequested {
	p := events.RemediationRequested{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationRound: t.Round,
		Repo:             t.Repo,
		CommitSHA:        t.CommitSHA,
		CodeBundleURI:    t.CodeBundleURI,
		Nodes:            make([]events.RemediationRequestedNode, 0, len(t.Nodes)),
	}
	for _, n := range t.Nodes {
		node := events.RemediationRequestedNode{
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
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, events.RemediationRequestedAncestor(a))
		}
		if n.Unfixable != nil {
			node.Unfixable = &events.Unfixable{Reason: string(n.Unfixable.Reason), Detail: n.Unfixable.Detail}
		}
		p.Nodes = append(p.Nodes, node)
	}
	return p
}

// EncodeTrigger is the payload of a trigger as a remediation stores it, which
// DecodeTrigger reads back as the same trigger without its tenant.
func EncodeTrigger(t remediation.Trigger) ([]byte, error) {
	raw, err := json.Marshal(requestedFromTrigger(t))
	if err != nil {
		return nil, fmt.Errorf("marshal remediation.requested payload: %w", err)
	}
	return raw, nil
}

// DecodeTrigger decodes the payload of a heal trigger as a store holds it, a
// remediation.requested:v3 payload or one stored from remediation.requested:v2,
// into the trigger it carries, as TriggerFromRequested maps it. A missing round
// is round 1. It errors on a payload that events.DecodeRemediationRequestedPayload
// refuses.
func DecodeTrigger(payload []byte) (remediation.Trigger, error) {
	p, err := events.DecodeRemediationRequestedPayload(payload)
	if err != nil {
		return remediation.Trigger{}, err
	}
	return TriggerFromRequested(p), nil
}
