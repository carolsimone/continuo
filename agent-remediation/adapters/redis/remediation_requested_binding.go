package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// failInFlightTimeout bounds the single-row recovery UPDATE the drop handler
// runs, so a slow database can never stall the consumer's reclaim sweep on the
// off-critical drop path.
const failInFlightTimeout = 10 * time.Second

// TriggerFromPayload decodes the payload of a heal trigger as a store holds it
// (a remediation.requested:v3 payload, or one stored from
// remediation.requested:v2) into the handlers.Trigger of the legacy engine: the
// failing nodes a fix can reach. A node the classifier marked unfixable is left
// out, since no fix is proposed for it. The dedup identity fields (MessageID,
// OutboxEntryID) are left unset: they belong to the Redis message that
// delivered the payload rather than to the payload itself, so a caller
// replaying stored bytes supplies an identity of its own. RawPayload is set to
// raw so a replayed trigger carries the same bytes the original did. Returns an
// error if the payload cannot be decoded.
func TriggerFromPayload(raw []byte) (handlers.Trigger, error) {
	p, err := pkgevents.DecodeRemediationRequestedPayload(raw)
	if err != nil {
		return handlers.Trigger{}, fmt.Errorf("decode remediation.requested payload: %w", err)
	}
	return legacyTrigger(p, raw), nil
}

// legacyTrigger maps a decoded heal trigger to the handlers.Trigger of the
// legacy engine, which works the nodes a fix can reach and never sees an
// unfixable one. raw is the payload the trigger was decoded from.
func legacyTrigger(p pkgevents.RemediationRequested, raw []byte) handlers.Trigger {
	nodes := make([]handlers.TriggerNode, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		if n.Unfixable != nil {
			continue
		}
		node := handlers.TriggerNode{
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
			node.ChangedAncestors = append(node.ChangedAncestors, handlers.ChangedAncestor(a))
		}
		nodes = append(nodes, node)
	}
	return handlers.Trigger{
		Source:           p.Source,
		ReleaseID:        p.ReleaseID,
		RemediationRound: p.RemediationRound,
		Repo:             p.Repo,
		CommitSHA:        p.CommitSHA,
		CodeBundleURI:    p.CodeBundleURI,
		Nodes:            nodes,
		RawPayload:       raw,
	}
}

// NewRemediationRequestedLegacyConsumer constructs the StreamConsumer of
// remediation.requested:v3 for the legacy engine: each message proposes one
// fix attempt per rejected release via handlers.ProposeFix, over the failing
// nodes a fix can reach, and a message
// dead-lettered after its last delivery fails the in-flight attempt row it
// left behind. The consumer group is created idempotently by
// StreamConsumer.Start; call Start(ctx) in a goroutine to begin consuming.
func NewRemediationRequestedLegacyConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV3,
		streams.AgentRemediationRemediationRequested,
		newLegacyRemediationRequestedHandler(deps),
		logger,
		pkgredis.WithOnDrop(failInFlightOnDrop(logger, func(ctx context.Context, releaseID, reason string) (int, error) {
			ctx, cancel := context.WithTimeout(ctx, failInFlightTimeout)
			defer cancel()
			return handlers.FailInFlight(ctx, deps, releaseID, reason)
		})),
	)
}

// NewRemediationRequestedRunnerConsumer constructs the StreamConsumer of
// remediation.requested:v3 for the runner engine: each message is handed to
// accept, which records it as a remediation the worker drives. The consumer
// group is created idempotently by StreamConsumer.Start; call Start(ctx) in a
// goroutine to begin consuming.
func NewRemediationRequestedRunnerConsumer(rc *goredis.Client, accept func(context.Context, runner.Accepted) error, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV3,
		streams.AgentRemediationRemediationRequested,
		newRunnerRemediationRequestedHandler(accept),
		logger,
	)
}

// requestedMessage decodes one remediation.requested:v3 message: its envelope,
// the heal trigger its payload carries and the payload's bytes. A message that
// does not decode is a permanent failure: the error wraps events.ErrPermanent,
// so the consumer dead-letters it.
func requestedMessage(msg goredis.XMessage) (pkgevents.Envelope, pkgevents.RemediationRequested, []byte, error) {
	fields := messageFields(msg)
	env, p, err := pkgevents.DecodeRemediationRequested(fields)
	if err != nil {
		return pkgevents.Envelope{}, pkgevents.RemediationRequested{}, nil,
			fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRequestedV3, err)
	}
	return env, p, []byte(fields["payload"]), nil
}

// newRunnerRemediationRequestedHandler returns the MessageHandler that accepts
// each remediation.requested:v3 message's trigger as a remediation, unfixable
// nodes included, with the tenant the envelope carries (empty when it names
// none), the payload bytes, the message's dedup id, the producer's outbox entry
// id and the stream's fields. A message that does not decode is a permanent
// failure: the handler returns events.ErrPermanent, so the consumer
// dead-letters it. Any other error is returned as is, so the consumer
// redelivers it.
func newRunnerRemediationRequestedHandler(accept func(context.Context, runner.Accepted) error) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		env, p, payload, err := requestedMessage(msg)
		if err != nil {
			return err
		}
		trigger := serialization.TriggerFromRequested(p)
		trigger.TenantID = env.TenantID
		return accept(ctx, runner.Accepted{
			Trigger:       trigger,
			Payload:       payload,
			MessageID:     pkgredis.DedupMessageID(ctx, msg),
			OutboxEntryID: messageprocessing.ExtractOutboxEntryID(msg.Values),
			Fields:        streamFields(msg),
		})
	}
}

// newLegacyRemediationRequestedHandler returns the MessageHandler that
// proposes one fix attempt for the release of each remediation.requested:v3
// message, over the failing nodes a fix can reach: the nodes the classifier
// marked unfixable are left out, and a message left with none is acknowledged
// with nothing written. A message that does not decode is a permanent failure:
// the handler returns events.ErrPermanent, so the consumer dead-letters it. Any
// other error is returned as is, so the consumer redelivers it.
func newLegacyRemediationRequestedHandler(deps handlers.Deps) pkgredis.MessageHandler {
	return legacyRemediationRequestedHandler(func(ctx context.Context, t handlers.Trigger) error {
		return handlers.ProposeFix(ctx, deps, t)
	})
}

// legacyRemediationRequestedHandler is newLegacyRemediationRequestedHandler over
// propose, which works the trigger of the nodes a fix can reach.
func legacyRemediationRequestedHandler(propose func(context.Context, handlers.Trigger) error) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		_, p, payload, err := requestedMessage(msg)
		if err != nil {
			return err
		}
		trigger := legacyTrigger(p, payload)
		trigger.MessageID = pkgredis.DedupMessageID(ctx, msg)
		trigger.OutboxEntryID = messageprocessing.ExtractOutboxEntryID(msg.Values)
		return propose(ctx, trigger)
	}
}

// messageFields copies the message's fields as strings.
func messageFields(msg goredis.XMessage) map[string]string {
	out := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// streamFields copies the message's fields as strings, without the payload,
// which a remediation stores on its own.
func streamFields(msg goredis.XMessage) map[string]string {
	out := messageFields(msg)
	delete(out, "payload")
	return out
}

// failInFlightOnDrop builds the drop handler that closes out a remediation
// attempt the stream consumer abandoned. markGenerating commits an in-flight
// 'generating' row before the model is called; if the trigger then fails on
// every redelivery and is dead-lettered on its fifth delivery, that row is left
// reporting a fix as forever generating — and the release's "Try again" stays
// blocked behind it. This decodes the dead-lettered trigger's release id and
// fails that row via fail.
//
// A message with no payload, or one whose payload cannot be decoded, named no
// release and never created a row (markGenerating runs only after a successful
// decode), so it is ignored.
func failInFlightOnDrop(logger *slog.Logger, fail func(ctx context.Context, releaseID, reason string) (int, error)) pkgredis.DropHandler {
	return func(ctx context.Context, msg goredis.XMessage, cause error) {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return
		}
		t, err := TriggerFromPayload([]byte(raw))
		if err != nil || t.ReleaseID == "" {
			return
		}
		reason := fmt.Sprintf("remediation trigger dead-lettered: %v", cause)
		n, ferr := fail(ctx, t.ReleaseID, reason)
		if ferr != nil {
			logger.Error("could not fail in-flight remediation row after its trigger was dead-lettered",
				"stream", streams.RemediationRequestedV3, "message_id", msg.ID, "release", t.ReleaseID, "error", ferr)
			return
		}
		if n > 0 {
			logger.Warn("failed in-flight remediation row after its trigger was dead-lettered",
				"stream", streams.RemediationRequestedV3, "message_id", msg.ID, "release", t.ReleaseID, "cause", cause)
		}
	}
}
