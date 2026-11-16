package events

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// DecodeRemediationRequested reads one remediation.requested:v3 entry. Every
// error wraps ErrMalformedEnvelope: an unreadable envelope, another
// schema_version, a payload that is not JSON, or one that breaks the rules
// DecodeRemediationRequestedPayload lists. A v3 entry always carries its
// round, so a remediation_round below 1 is refused here, a missing one
// included.
func DecodeRemediationRequested(fields map[string]string) (Envelope, RemediationRequested, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, RemediationRequested{}, err
	}
	if env.SchemaVersion != RemediationRequestedSchemaVersion {
		return Envelope{}, RemediationRequested{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	p, err := unmarshalRemediationRequested(payload)
	if err != nil {
		return Envelope{}, RemediationRequested{}, err
	}
	if err := validRemediationRequested(p); err != nil {
		return Envelope{}, RemediationRequested{}, err
	}
	return env, p, nil
}

// DecodeRemediationRequestedPayload reads the payload of a remediation
// trigger as a store holds it: a remediation.requested:v3 payload, or one
// remediation.requested:v2 stored, whose event_id is ignored and whose nodes
// are all fixable. A stored payload that has no remediation_round, or a 0,
// is round 1: the field was added after the first v2 payloads were stored.
// Every error wraps ErrMalformedEnvelope: a payload that is not JSON, an
// empty release_id, a negative remediation_round, a source outside the
// remediation_source vocabulary, a node without a node_id, or an unfixable
// reason other than the two the classifier records
// (model.HandoverReasonClassifierInfra and
// model.HandoverReasonClassifierUnhealableParse). A trigger with no node
// reads: the consumer decides what it means.
func DecodeRemediationRequestedPayload(raw []byte) (RemediationRequested, error) {
	p, err := unmarshalRemediationRequested(raw)
	if err != nil {
		return RemediationRequested{}, err
	}
	if p.RemediationRound == 0 {
		p.RemediationRound = 1
	}
	if err := validRemediationRequested(p); err != nil {
		return RemediationRequested{}, err
	}
	return p, nil
}

func unmarshalRemediationRequested(raw []byte) (RemediationRequested, error) {
	var p RemediationRequested
	if err := json.Unmarshal(raw, &p); err != nil {
		return RemediationRequested{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	return p, nil
}

func validRemediationRequested(p RemediationRequested) error {
	if p.ReleaseID == "" {
		return fmt.Errorf("%w: no release_id", ErrMalformedEnvelope)
	}
	if p.RemediationRound < 1 {
		return fmt.Errorf("%w: remediation_round %d for release %s", ErrMalformedEnvelope, p.RemediationRound, p.ReleaseID)
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
