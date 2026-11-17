package neo4jinfra

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// canonicalJSON encodes a stored contract as the text a :ContractVersion
// holds: compact JSON with every object's keys sorted and no HTML escaping, so
// one document always encodes to the same text. A json.Number is written as
// its own text.
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
// document: objects as map[string]any, arrays as []any, numbers as
// json.Number, the text stored, so a number keeps the exact value the
// contract hash was taken over and canonicalJSON writes it back unchanged.
func decodeDocument(text string) (map[string]any, error) {
	var document map[string]any
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	if err := dec.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode a stored contract: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode a stored contract: text after the document")
	}
	return document, nil
}
