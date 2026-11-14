package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvidence_GetReturnsTheTypedValue(t *testing.T) {
	e := NewEvidence().With(KeyDBTLog, Text("boom"))
	v, ok := Get[Text](e, KeyDBTLog)
	assert.True(t, ok)
	assert.Equal(t, Text("boom"), v)
	_, ok = Get[SourceFiles](e, KeyDBTLog)
	assert.False(t, ok)
	_, ok = Get[Text](e, KeyCandidateSQL)
	assert.False(t, ok)
}

func TestEvidence_WithReturnsACopy(t *testing.T) {
	base := NewEvidence().With(KeyDBTLog, Text("one"))
	next := base.With(KeyDBTLog, Text("two")).With(KeyCandidateSQL, Text("select 1"))
	v, _ := Get[Text](base, KeyDBTLog)
	assert.Equal(t, Text("one"), v)
	assert.Equal(t, []EvidenceKey{KeyDBTLog}, base.Keys())
	assert.Equal(t, []EvidenceKey{KeyCandidateSQL, KeyDBTLog}, next.Keys())
}

func TestEvidence_ZeroValueIsUsable(t *testing.T) {
	var e Evidence
	_, ok := Get[Text](e, KeyDBTLog)
	assert.False(t, ok)
	assert.Empty(t, e.Keys())
	v, _ := Get[Text](e.With(KeyDBTLog, Text("x")), KeyDBTLog)
	assert.Equal(t, Text("x"), v)
}
