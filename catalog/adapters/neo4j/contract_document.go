package neo4jinfra

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// canonicalJSON encodes a stored contract as the text a :ContractVersion
// holds: compact JSON with every object's keys sorted and no HTML escaping, so
// one document always encodes to the same text.
func canonicalJSON(document map[string]any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(document); err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// decodeDocument decodes the text a :ContractVersion holds back to a contract
// document: objects as map[string]any, arrays as []any, numbers as float64.
func decodeDocument(text string) (map[string]any, error) {
	var document map[string]any
	if err := json.Unmarshal([]byte(text), &document); err != nil {
		return nil, fmt.Errorf("decode a stored contract: %w", err)
	}
	return document, nil
}
