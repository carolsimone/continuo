package redis

import (
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// stringFields copies a stream message's fields as strings, the shape the
// pkg/events envelope decoders read.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// decodePayload extracts the "payload" field from a Redis stream message and
// unmarshals it into dst.
func decodePayload(msg goredis.XMessage, dst any) error {
	raw, ok := msg.Values["payload"].(string)
	if !ok {
		return fmt.Errorf("missing or non-string payload field")
	}
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}
