package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
)

// flowRejectedRelease is the flow a heal trigger for a rejected release runs.
const flowRejectedRelease = "rejected-release"

// Accepted is one heal trigger as the stream delivered it: the decoded trigger,
// the payload bytes it was decoded from, the message's id and stream fields,
// and the id of the producer's outbox entry when the message carries one.
type Accepted struct {
	Trigger       remediation.Trigger
	Payload       []byte
	MessageID     string
	OutboxEntryID *uuid.UUID
	Fields        map[string]string
}

// Accept records a heal trigger as a running remediation, in one transaction
// that also claims the message: a redelivered message, or the same outbox entry
// republished under a new message id, commits nothing and returns nil. A
// trigger for a release and round that already has a remediation only marks
// its message completed. A trigger with no failing node is acknowledged with
// nothing written. The work itself starts when a worker drives the
// remediation.
func (r *Runner) Accept(ctx context.Context, a Accepted) error {
	t := a.Trigger
	if t.Round < 1 {
		t.Round = 1
	}
	if t.TenantID == "" {
		t.TenantID = events.DefaultTenantID
	}
	if len(t.Nodes) == 0 {
		r.logger.Info("remediation.requested trigger carries no failing nodes — nothing to fix",
			"release", t.ReleaseID, "message_id", a.MessageID)
		return nil
	}

	u := r.d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin (accept): %w", err)
	}
	defer func() { _ = u.Rollback() }()

	msgID, dup, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, u.MessageProcessingRepo(), r.logger,
		a.MessageID, streams.RemediationRequestedV2, a.Payload, a.OutboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if dup {
		r.logger.Info("duplicate remediation.requested trigger — skipping",
			"message_id", a.MessageID, "release", t.ReleaseID)
		return nil
	}

	now := r.clock.Now()
	rem := remediation.Remediation{
		ID:               uuid.New(),
		TenantID:         t.TenantID,
		ReleaseID:        t.ReleaseID,
		Round:            t.Round,
		Flow:             flowRejectedRelease,
		Status:           remediation.StatusRunning,
		TriggerPayload:   a.Payload,
		TriggerMessageID: a.MessageID,
		TriggerFields:    a.Fields,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	created, err := u.RemediationRepo().Create(ctx, rem)
	if err != nil {
		return fmt.Errorf("create remediation: %w", err)
	}
	if !created {
		r.logger.Info("remediation.requested trigger for a release round that already has a remediation — nothing to start",
			"message_id", a.MessageID, "release", t.ReleaseID, "round", t.Round)
	}
	if err := u.MessageProcessingRepo().UpdateState(ctx, msgID, messageprocessing.StateCompleted); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit (accept): %w", err)
	}
	if created {
		r.logger.Info("remediation accepted", "remediation", rem.ID, "release", t.ReleaseID, "round", t.Round, "nodes", len(t.Nodes))
	}
	return nil
}

// triggerWire is the remediation.requested:v2 payload in its wire field names.
type triggerWire struct {
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	// RemediationRound is missing or 0 for round 1.
	RemediationRound int               `json:"remediation_round"`
	Repo             string            `json:"repo"`
	CommitSHA        string            `json:"commit_sha"`
	CodeBundleURI    string            `json:"code_bundle_uri,omitempty"`
	Nodes            []triggerNodeWire `json:"nodes"`
}

// triggerNodeWire is one failing node of a triggerWire.
type triggerNodeWire struct {
	NodeID               string                `json:"node_id"`
	RelationID           string                `json:"relation_id,omitempty"`
	Category             string                `json:"category"`
	ErrorSignature       string                `json:"error_signature"`
	Reason               string                `json:"reason"`
	ErrorExcerpt         string                `json:"error_excerpt,omitempty"`
	DBTLogURI            string                `json:"dbt_log_uri"`
	CandidateArtifactURI string                `json:"candidate_artifact_uri,omitempty"`
	FilePath             string                `json:"file_path,omitempty"`
	Service              string                `json:"service,omitempty"`
	NodeType             string                `json:"node_type,omitempty"`
	OtherService         string                `json:"other_service,omitempty"`
	OtherFilePath        string                `json:"other_file_path,omitempty"`
	ChangedAncestors     []changedAncestorWire `json:"changed_ancestors,omitempty"`
}

// changedAncestorWire is one entry of a failing node's changed_ancestors.
type changedAncestorWire struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Service  string `json:"service,omitempty"`
	Depth    int    `json:"depth,omitempty"`
}

// DecodeTrigger decodes a remediation.requested:v2 payload into a trigger. A
// missing or 0 round is round 1. The tenant is not part of the payload, so the
// trigger's TenantID is left empty for the caller to set from the message. It
// errors on a payload that is not JSON.
func DecodeTrigger(payload []byte) (remediation.Trigger, error) {
	var w triggerWire
	if err := json.Unmarshal(payload, &w); err != nil {
		return remediation.Trigger{}, fmt.Errorf("unmarshal remediation.requested payload: %w", err)
	}
	t := remediation.Trigger{
		Source:        w.Source,
		ReleaseID:     w.ReleaseID,
		Round:         max(w.RemediationRound, 1),
		Repo:          w.Repo,
		CommitSHA:     w.CommitSHA,
		CodeBundleURI: w.CodeBundleURI,
		Nodes:         make([]remediation.TriggerNode, 0, len(w.Nodes)),
	}
	for _, n := range w.Nodes {
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
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, remediation.ChangedAncestor(a))
		}
		t.Nodes = append(t.Nodes, node)
	}
	return t, nil
}
