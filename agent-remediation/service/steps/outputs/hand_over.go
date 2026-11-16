package outputs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

const nameHandOver workflow.StepName = "hand_over"

// HandOver is the give-up output `hand_over`. It announces on
// remediation.handed_over:v1 every node a remediation leaves to a human, each
// with the reason it was handed over for.
type HandOver struct{}

var _ step.GiveUpOutput = (*HandOver)(nil)

// NewHandOver builds the hand_over step.
func NewHandOver() *HandOver {
	return &HandOver{}
}

// Meta describes the step. It reads no evidence.
func (*HandOver) Meta() step.Meta {
	return step.Meta{
		Name: nameHandOver,
		Kind: step.KindOutput,
		Description: "Enqueues one remediation.handed_over:v1 event naming every node the remediation leaves to a human, " +
			"each with its reason code and message, in the transaction that ends the remediation.",
	}
}

// GiveUp enqueues the remediation.handed_over:v1 event of the nodes
// in.HandedOver names, in u's transaction, its payload stored under the
// remediation's tenant. Tries is how many tries the remediation made. The
// entry id is derived from the remediation, so a remediation announces its
// hand-over once: a second enqueue collides with the first. With no node
// handed over, nothing is written. No inbound message drove the write, so the
// entry carries no message-processing provenance.
func (*HandOver) GiveUp(ctx context.Context, u uow.UnitOfWork, in step.OutputInput) error {
	if len(in.HandedOver) == 0 {
		return nil
	}
	rem := in.Remediation
	nodes := make([]events.HandedOverNode, 0, len(in.HandedOver))
	for _, h := range in.HandedOver {
		nodes = append(nodes, events.HandedOverNode{NodeID: h.NodeID, Reason: string(h.Code), Message: h.Message})
	}
	body, err := json.Marshal(events.RemediationHandedOver{
		RemediationID:    rem.ID.String(),
		ReleaseID:        rem.ReleaseID,
		RemediationRound: rem.Round,
		Flow:             rem.Flow,
		Tries:            rem.CurrentTry,
		Nodes:            nodes,
	})
	if err != nil {
		return fmt.Errorf("marshal handed-over event: %w", err)
	}
	stored, err := outbox.WrapTenant(rem.TenantID, body)
	if err != nil {
		return fmt.Errorf("store handed-over event under its tenant: %w", err)
	}
	return u.OutboxRepo().Create(ctx, &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("handed_over|"+rem.ID.String())),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(rem.ReleaseID),
		EventType:     event.HandedOverEventType,
		Payload:       stored,
		StreamName:    streams.RemediationHandedOverV1,
		Status:        "pending",
	})
}
