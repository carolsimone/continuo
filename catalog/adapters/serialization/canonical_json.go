package serialization

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// CanonicalJSON writes a decoded JSON value (a string, a number, a bool, nil,
// a map[string]any or a []any) as compact JSON with every object's keys
// sorted and no HTML escaping, so one value always renders the same text. A
// value that has no JSON form, which a decoded document never holds, renders
// as its Go print form.
func CanonicalJSON(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprint(v)
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}
