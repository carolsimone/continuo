package neo4jinfra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The text a :ContractVersion holds decodes back to the same text: a number
// beyond float64's exact range keeps every digit, so the document still
// matches the contract hash taken over it.
func TestContractDocument_ANumberRoundTripsAsItsText(t *testing.T) {
	text := `{"id":"analytics.t","limit":9007199254740993,"ratio":0.1}`

	document, err := decodeDocument(text)
	require.NoError(t, err)

	assert.Equal(t, json.Number("9007199254740993"), document["limit"])
	again, err := canonicalJSON(document)
	require.NoError(t, err)
	assert.Equal(t, text, again)
}

func TestContractDocument_TextAfterTheDocumentIsAnError(t *testing.T) {
	_, err := decodeDocument(`{"id":"a"} {"id":"b"}`)
	require.Error(t, err)
}
