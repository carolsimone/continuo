package events

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// DecodeHealTriggered reads one heal.triggered:v1 entry. Every
// error wraps ErrMalformedEnvelope: an unreadable envelope, another
// schema_version, a payload that is not JSON, or one that breaks the rules
// DecodeHealTriggeredPayload lists.
func DecodeHealTriggered(fields map[string]string) (Envelope, HealTriggered, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, HealTriggered{}, err
	}
	if env.SchemaVersion != HealTriggeredSchemaVersion {
		return Envelope{}, HealTriggered{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	p, err := unmarshalHealTriggered(payload)
	if err != nil {
		return Envelope{}, HealTriggered{}, err
	}
	if err := validHealTriggered(p); err != nil {
		return Envelope{}, HealTriggered{}, err
	}
	return env, p, nil
}

// DecodeHealTriggeredPayload reads the payload of a heal trigger as a store
// holds it: the heal.triggered:v1 payload as the stream carried it.
// Every error wraps ErrMalformedEnvelope: a payload that is not JSON, an
// empty release_id, a round below 1 (a missing one included), a source outside the
// remediation_source vocabulary, a node without a node_id, or an unfixable
// reason other than the two the classifier records
// (model.HandoverReasonClassifierInfra and
// model.HandoverReasonClassifierUnhealableParse). A trigger with no node
// reads: the consumer decides what it means.
func DecodeHealTriggeredPayload(raw []byte) (HealTriggered, error) {
	p, err := unmarshalHealTriggered(raw)
	if err != nil {
		return HealTriggered{}, err
	}
	if err := validHealTriggered(p); err != nil {
		return HealTriggered{}, err
	}
	return p, nil
}

func unmarshalHealTriggered(raw []byte) (HealTriggered, error) {
	var p HealTriggered
	if err := json.Unmarshal(raw, &p); err != nil {
		return HealTriggered{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	return p, nil
}

func validHealTriggered(p HealTriggered) error {
	if p.ReleaseID == "" {
		return fmt.Errorf("%w: no release_id", ErrMalformedEnvelope)
	}
	if p.Round < 1 {
		return fmt.Errorf("%w: round %d for release %s", ErrMalformedEnvelope, p.Round, p.ReleaseID)
	}
	if !model.RemediationSource(p.Source).IsValid() {
		return fmt.Errorf("%w: source %q for release %s", ErrMalformedEnvelope, p.Source, p.ReleaseID)
	}
	for i, n := range p.Nodes {
		if n.NodeID == "" {
			return fmt.Errorf("%w: node %d of release %s has no node_id", ErrMalformedEnvelope, i, p.ReleaseID)
		}
		if n.Unfixable == nil {
			continue
		}
		switch model.HandoverReason(n.Unfixable.Reason) {
		case model.HandoverReasonClassifierInfra, model.HandoverReasonClassifierUnhealableParse:
		default:
			return fmt.Errorf("%w: unfixable reason %q for node %s", ErrMalformedEnvelope, n.Unfixable.Reason, n.NodeID)
		}
	}
	return nil
}
