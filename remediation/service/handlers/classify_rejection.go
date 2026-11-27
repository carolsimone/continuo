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
	"github.com/carolsimone/continuo/remediation/serialization"
)

// ClassifyRejection triages every failed node of one rejected release: each
// node is classified deterministically and its decision recorded (emit and
// drop alike), and, when at least one node is newly recorded, ONE batched
// heal.triggered trigger carrying exactly those nodes is enqueued, all
// in a single transaction. A node the classifier drops (no change to the
// release's source fixes it) is carried too, marked unfixable with its reason,
// so the release page can name it and why no fix was proposed. The release,
// not the node, is the unit of remediation, so a release failing on several
// nodes yields one trigger, one fix and one pull request downstream. The
// trigger's outbox row is stored under tenantID, the tenant of the message the
// rejection came from. Idempotency is the decision repository's natural key: a
// redelivered rejection re-records nothing new and therefore re-emits and
// counts nothing. Each newly recorded decision is counted on deps.Metrics once
// the transaction has committed.
func ClassifyRejection(ctx context.Context, deps Deps, tenantID string, evs []failure.FailureEvidence) error {
	if len(evs) == 0 {
		return nil
	}
	if tenantID == "" {
		return fmt.Errorf("classify release %s: %w", evs[0].ReleaseID, outbox.ErrNoTenant)
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

	var recorded []classified
	for _, it := range items {
		inserted, err := u.DecisionRepo().Upsert(ctx, repository.ClassificationDecision{
			Source: it.ev.Source, ReleaseID: it.ev.ReleaseID, RemediationRound: it.ev.RemediationRound,
			NodeID: it.ev.NodeID, Category: it.c.Category, ErrorSignature: it.c.Signature,
			Decision: it.c.Decision, Reason: it.c.Reason, DBTLogURI: it.ev.DBTLogURI, CreatedAt: deps.Clock.Now(),
		})
		if err != nil {
			return fmt.Errorf("upsert decision %s: %w", it.ev.NodeID, err)
		}
		// Only a newly recorded node joins the batch, so a redelivery never
		// re-emits. The classifier's Decision says whether a fix can reach the
		// node, not whether the trigger carries it: both kinds do.
		deps.Logger.Info("classified failure",
			"node", it.ev.NodeID, "release", it.ev.ReleaseID,
			"category", it.c.Category, "decision", it.c.Decision, "reason", it.c.Reason,
			"unfixable", string(it.c.Unfixable), "recorded", inserted)
		if inserted {
			recorded = append(recorded, it)
		}
	}

	if len(recorded) > 0 {
		head := recorded[0].ev
		eventID := event.RemediationEventID(head.ReleaseID, round).String()
		payload := event.HealTriggered{
			Source:           head.Source,
			ReleaseID:        head.ReleaseID,
			RemediationRound: round,
			Repo:             head.Repo,
			CommitSHA:        head.CommitSHA,
			CodeBundleURI:    head.CodeBundleURI,
			ClassifiedAt:     deps.Clock.Now().Format("2006-01-02T15:04:05Z07:00"),
		}
		for _, it := range recorded {
			node := event.FailingNode{
				NodeID: it.ev.NodeID, RelationID: it.ev.RelationID,
				Category: string(it.c.Category), ErrorSignature: it.c.Signature, Reason: it.c.Reason, ErrorExcerpt: it.c.Excerpt,
				DBTLogURI: it.ev.DBTLogURI, CandidateArtifactURI: it.ev.CandidateArtifactURI,
				FilePath: it.ev.FilePath, Project: it.ev.Project, NodeType: it.ev.NodeType,
				OtherProject: it.ev.OtherProject, OtherFilePath: it.ev.OtherFilePath,
				ChangedAncestors: changedAncestorsOf(it.ev),
			}
			if it.c.Decision == failure.DecisionDrop {
				if !it.c.Unfixable.IsValid() {
					return fmt.Errorf("node %s of release %s is dropped (%s) with no unfixable reason", it.ev.NodeID, it.ev.ReleaseID, it.c.Reason)
				}
				node.Unfixable = &event.Unfixable{Reason: it.c.Unfixable, Detail: it.c.UnfixableDetail()}
			}
			payload.Nodes = append(payload.Nodes, node)
		}
		body, err := json.Marshal(serialization.HealTriggeredFromDomain(payload))
		if err != nil {
			return fmt.Errorf("marshal trigger: %w", err)
		}
		stored, err := outbox.WrapTenant(tenantID, body)
		if err != nil {
			return fmt.Errorf("store trigger under tenant %q: %w", tenantID, err)
		}
		if err := u.OutboxRepo().Create(ctx, &outbox.Entry{
			ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID)),
			AggregateType: "remediation",
			AggregateID:   event.AggregateIDForRelease(head.ReleaseID),
			EventType:     event.EventType,
			Payload:       stored,
			StreamName:    streams.HealTriggeredV1,
			Status:        "pending",
		}); err != nil {
			return fmt.Errorf("enqueue trigger: %w", err)
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// Each decision is counted once its transaction has committed, so a
	// rollback counts nothing and the redelivery that records it counts it
	// once. A node recorded before is not in recorded, so a redelivery counts
	// nothing it already counted.
	metrics := deps.classifierMetrics()
	for _, it := range recorded {
		metrics.Classified(string(it.c.Decision), it.c.Reason)
	}
	deps.Logger.Info("rejection classified", "release", evs[0].ReleaseID, "round", round,
		"nodes", len(items), "recorded_nodes", len(recorded))
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
		out = append(out, event.ChangedAncestor{NodeID: a.NodeID, FilePath: a.FilePath, Project: a.Project, Depth: a.Depth})
	}
	return out
}
