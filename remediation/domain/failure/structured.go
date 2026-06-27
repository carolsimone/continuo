package failure

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// StructuredResult is the parsed structured validation-result contract emitted
// by the validation pod (validation_runner.py / seed wrapper) and uploaded by
// k8s-controller. It is the preferred classification signal; the text log is the
// fallback when it is absent. status uses dbt's RunStatus vocabulary.
type StructuredResult struct {
	Status   string `json:"status"`
	Message  string `json:"message"`
	Failures int    `json:"failures"`
	UniqueID string `json:"unique_id"`
}

// ParseStructuredResult decodes the structured contract. The validation pod's
// uploaded object can carry a stderr/log preamble before the JSON (and trailing
// output after it), so rather than strict-unmarshalling the whole body — which
// fails on any surrounding text and made genuine logic failures classify as
// unknown — it locates the JSON object: scan from each '{', decode a single JSON
// value (ignoring trailing content), and accept the first that yields a
// status-bearing record. A body with no such object is a real error so the
// caller can log it and fall back to the text log rather than misclassifying.
func ParseStructuredResult(raw []byte) (*StructuredResult, error) {
	for start := bytes.IndexByte(raw, '{'); start >= 0; {
		var sr StructuredResult
		if err := json.NewDecoder(bytes.NewReader(raw[start:])).Decode(&sr); err == nil && sr.Status != "" {
			return &sr, nil
		}
		next := bytes.IndexByte(raw[start+1:], '{')
		if next < 0 {
			break
		}
		start += next + 1
	}
	return nil, fmt.Errorf("no structured validation-result JSON object found in %d bytes", len(raw))
}
