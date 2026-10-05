package redis

import goredis "github.com/redis/go-redis/v9"

// Fields dead-letter-controller adds when it redrives an entry. RedriveGroupField
// names the one consumer group that should process the entry (absent when every
// group should, as for an event that was never published); RedrivenFromField
// names the dead letter it came from.
const (
	RedriveGroupField = "redrive_group"
	RedrivenFromField = "redriven_from"
)

// routeRedrive reports whether msg is for group. When it is, it returns msg
// with the redrive fields removed, so the handler sees exactly the original
// fields.
func routeRedrive(msg goredis.XMessage, group string) (goredis.XMessage, bool) {
	target, addressed := msg.Values[RedriveGroupField]
	_, redriven := msg.Values[RedrivenFromField]
	if !addressed && !redriven {
		return msg, true
	}
	if addressed {
		if s, _ := target.(string); s != group {
			return msg, false
		}
	}
	values := make(map[string]any, len(msg.Values))
	for k, v := range msg.Values {
		if k == RedriveGroupField || k == RedrivenFromField {
			continue
		}
		values[k] = v
	}
	return goredis.XMessage{ID: msg.ID, Values: values}, true
}
