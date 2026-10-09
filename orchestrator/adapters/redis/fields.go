package redis

import "fmt"

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
