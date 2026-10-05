package events

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrMalformedEnvelope reports stream fields that do not carry a readable
// envelope: a missing or unparseable envelope field, or no payload.
var ErrMalformedEnvelope = errors.New("malformed event envelope")

// ParseEnvelope reads the envelope fields Envelope.Fields writes and returns
// the envelope with the raw JSON payload.
func ParseEnvelope(fields map[string]string) (Envelope, []byte, error) {
	payload, ok := fields["payload"]
	if !ok || payload == "" {
		return Envelope{}, nil, fmt.Errorf("%w: no payload field", ErrMalformedEnvelope)
	}
	version, err := strconv.Atoi(fields["schema_version"])
	if err != nil {
		return Envelope{}, nil, fmt.Errorf("%w: schema_version %q", ErrMalformedEnvelope, fields["schema_version"])
	}
	occurred, err := time.Parse(occurredAtLayout, fields["occurred_at"])
	if err != nil {
		return Envelope{}, nil, fmt.Errorf("%w: occurred_at %q", ErrMalformedEnvelope, fields["occurred_at"])
	}
	return Envelope{
		EventID:       fields["event_id"],
		TenantID:      fields["tenant_id"],
		OccurredAt:    occurred.UTC(),
		Producer:      fields["producer"],
		SchemaVersion: version,
	}, []byte(payload), nil
}

// StreamIDTime returns the time encoded in a Redis stream id "<ms>-<seq>".
func StreamIDTime(id string) (time.Time, bool) {
	ms, _, ok := strings.Cut(id, "-")
	if !ok {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(n).UTC(), true
}
