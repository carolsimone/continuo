package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// stringFields copies a stream message's fields as strings. XREADGROUP returns
// every value as a string; any other value is formatted with %v.
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

// tenantField returns the tenant_id field of a stream entry, the tenant its
// producer published it for. An entry without one is permanent
// (events.ErrPermanent): it has no tenant to be written under, so the
// consumer dead-letters it. what names the entry's event in the error.
func tenantField(msg goredis.XMessage, what string) (string, error) {
	tenantID, _ := msg.Values["tenant_id"].(string)
	if tenantID == "" {
		return "", fmt.Errorf("%w: %s message %s carries no tenant_id", events.ErrPermanent, what, msg.ID)
	}
	return tenantID, nil
}
