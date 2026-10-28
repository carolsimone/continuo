package schedule

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireShapeMatches fails unless emitted has exactly the keys schema
// declares, at every depth. An array is compared through its first element; an
// empty array or a null is not descended into.
func requireShapeMatches(t *testing.T, schema, emitted json.RawMessage) {
	t.Helper()
	var s, e any
	require.NoError(t, json.Unmarshal(schema, &s))
	require.NoError(t, json.Unmarshal(emitted, &e))
	compareShape(t, "$", s, e)
}

func compareShape(t *testing.T, path string, schema, emitted any) {
	t.Helper()
	switch sv := schema.(type) {
	case map[string]any:
		ev, ok := emitted.(map[string]any)
		if !ok {
			assert.Nil(t, emitted, "%s: schema declares an object, output is %T", path, emitted)
			return
		}
		assert.ElementsMatch(t, mapKeys(sv), mapKeys(ev), "%s: keys differ", path)
		for k, child := range sv {
			if ec, ok := ev[k]; ok {
				compareShape(t, path+"."+k, child, ec)
			}
		}
	case []any:
		ev, ok := emitted.([]any)
		require.True(t, ok, "%s: schema declares an array, output is %T", path, emitted)
		if len(sv) > 0 && len(ev) > 0 {
			compareShape(t, path+"[]", sv[0], ev[0])
		}
	}
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
