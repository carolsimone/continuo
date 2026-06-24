package failure

import "encoding/json"

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

// ParseStructuredResult decodes the structured contract. A malformed body is a
// real error so the caller can log it and fall back to the text log rather than
// silently misclassifying.
func ParseStructuredResult(raw []byte) (*StructuredResult, error) {
	var sr StructuredResult
	if err := json.Unmarshal(raw, &sr); err != nil {
		return nil, err
	}
	return &sr, nil
}
