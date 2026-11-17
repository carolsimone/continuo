package serialization_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/catalog/adapters/serialization"
)

func TestCanonicalJSON_WritesAScalarAsJSON(t *testing.T) {
	assert.Equal(t, `"NUMERIC(12,2)"`, serialization.CanonicalJSON("NUMERIC(12,2)"))
	assert.Equal(t, `true`, serialization.CanonicalJSON(true))
	assert.Equal(t, `null`, serialization.CanonicalJSON(nil))
	assert.Equal(t, `12`, serialization.CanonicalJSON(float64(12)))
	assert.Equal(t, `0.5`, serialization.CanonicalJSON(0.5))
}

func TestCanonicalJSON_WritesAnObjectCompactWithSortedKeys(t *testing.T) {
	value := map[string]any{
		"name":     "day",
		"required": true,
		"nested":   map[string]any{"b": float64(2), "a": []any{"x", nil}},
	}

	assert.Equal(t, `{"name":"day","nested":{"a":["x",null],"b":2},"required":true}`, serialization.CanonicalJSON(value))
}

func TestCanonicalJSON_DoesNotEscapeHTMLCharacters(t *testing.T) {
	assert.Equal(t, `"a<b>&c"`, serialization.CanonicalJSON("a<b>&c"))
}

func TestCanonicalJSON_WritesAnEmptyContainerAsItself(t *testing.T) {
	assert.Equal(t, `{}`, serialization.CanonicalJSON(map[string]any{}))
	assert.Equal(t, `[]`, serialization.CanonicalJSON([]any{}))
}

func TestCanonicalJSON_WritesANumberAsItsText(t *testing.T) {
	assert.Equal(t, `9007199254740993`, serialization.CanonicalJSON(json.Number("9007199254740993")))
	assert.Equal(t, `{"a":1.5}`, serialization.CanonicalJSON(map[string]any{"a": json.Number("1.5")}))
}
