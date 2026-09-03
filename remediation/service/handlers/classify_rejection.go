package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/event"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/domain/repository"
)

// ClassifyRejection triages every failed node of one rejected release: each
// node is classified deterministically and its decision recorded (emit and
// drop alike), and — when at least one node is newly recorded with an emit
// decision — ONE batched remediation.requested trigger carrying exactly those
// nodes is enqueued, all in a single transaction. The release, not the node,
// is the unit of remediation, so a release failing on several nodes yields
// one trigger, one proposal and one pull request downstream. Idempotency is
// the decision repository's natural key: a redelivered rejection re-records
// nothing new and therefore re-emits nothing.
func ClassifyRejection(ctx context.Context, deps Deps, evs []failure.FailureEvidence) error {
	if len(evs) == 0 {
		return nil
	}
	round := evs[0].RemediationRound
	if round < 1 {
		round = 1
	}

	type classified struct {
		ev failure.FailureEvidence
		c  failure.Classification
	}
	items := make([]classified, 0, len(evs))
	for i := range evs {
		ev := evs[i]
		ev.RemediationRound = round
		c, err := classify(ctx, deps, &ev)
		if err != nil {
			return err
		}
		items = append(items, classified{ev: ev, c: c})
	}

	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	var emit []classified
	for _, it := range items {
		inserted, err := u.DecisionRepo().Upsert(ctx, repository.ClassificationDecision{
			Source: it.ev.Source, ReleaseID: it.ev.ReleaseID, RemediationRound: it.ev.RemediationRound,
			NodeID: it.ev.NodeID, Category: it.c.Category, ErrorSignature: it.c.Signature,
			Decision: it.c.Decision, Reason: it.c.Reason, DBTLogURI: it.ev.DBTLogURI, CreatedAt: deps.Clock.Now(),
		})
		if err != nil {
			return fmt.Errorf("upsert decision %s: %w", it.ev.NodeID, err)
		}
		// The classifier's Decision is the single gate on emitting (drop for an
		// infrastructure failure); only a newly-recorded emit joins the batch,
		// so a redelivery never re-emits.
		emitted := inserted && it.c.Decision == failure.DecisionEmit
		deps.Logger.Info("classified failure",
			"node", it.ev.NodeID, "release", it.ev.ReleaseID,
			"category", it.c.Category, "decision", it.c.Decision, "reason", it.c.Reason,
			"emitted", emitted)
		if emitted {
			emit = append(emit, it)
		}
	}

	if len(emit) > 0 {
		head := emit[0].ev
		payload := event.RemediationRequested{
			EventID:          event.RemediationEventID(head.ReleaseID, round).String(),
			Source:           string(head.Source),
			ReleaseID:        head.ReleaseID,
			RemediationRound: round,
			Repo:             head.Repo,
			CommitSHA:        head.CommitSHA,
			CodeBundleURI:    head.CodeBundleURI,
			ClassifiedAt:     deps.Clock.Now().Format("2006-01-02T15:04:05Z07:00"),
		}
		for _, it := range emit {
			payload.Nodes = append(payload.Nodes, event.FailingNode{
				NodeID: it.ev.NodeID, RelationID: it.ev.RelationID,
				Category: string(it.c.Category), ErrorSignature: it.c.Signature, Reason: it.c.Reason, ErrorExcerpt: it.c.Excerpt,
				DBTLogURI: it.ev.DBTLogURI, CandidateArtifactURI: it.ev.CandidateArtifactURI,
				FilePath: it.ev.FilePath, Service: it.ev.Service, NodeType: it.ev.NodeType,
				OtherService: it.ev.OtherService, OtherFilePath: it.ev.OtherFilePath,
				ChangedAncestors: changedAncestorsOf(it.ev),
			})
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal trigger: %w", err)
		}
		if err := u.OutboxRepo().Create(ctx, &outbox.Entry{
			ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(payload.EventID)),
			AggregateType: "remediation",
			AggregateID:   event.AggregateIDForRelease(head.ReleaseID),
			EventType:     event.EventType,
			Payload:       body,
			StreamName:    streams.RemediationRequestedV2,
			Status:        "pending",
			MaxRetries:    outbox.DefaultMaxRetries,
			CreatedAt:     deps.Clock.Now(),
		}); err != nil {
			return fmt.Errorf("enqueue trigger: %w", err)
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("rejection classified", "release", evs[0].ReleaseID, "round", round,
		"nodes", len(items), "emitted_nodes", len(emit))
	return nil
}

// changedAncestorsOf projects an evidence's changed ancestors onto the trigger
// event's own shape, keeping each ancestor's candidate location: the agent
// fixes the ancestor in the file THIS release declares for it, not wherever the
// promoted graph still places it.
func changedAncestorsOf(ev failure.FailureEvidence) []event.ChangedAncestor {
	if len(ev.ChangedAncestors) == 0 {
		return nil
	}
	out := make([]event.ChangedAncestor, 0, len(ev.ChangedAncestors))
	for _, a := range ev.ChangedAncestors {
		out = append(out, event.ChangedAncestor{NodeID: a.NodeID, FilePath: a.FilePath, Service: a.Service})
	}
	return out
}
