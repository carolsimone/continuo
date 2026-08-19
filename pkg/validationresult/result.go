package validationresult

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Result is the structured validation-result contract's JSON body, decoded
// from between the sentinel markers. The validation pod and the python
// production harness both emit it via
// continuo_validation_contract/result.py's result_block(); k8s-controller
// uploads it as run_results_uri and prefers its message as a failed task's
// error_message, and remediation reads it back to classify the failure.
// status uses dbt's RunStatus vocabulary.
type Result struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	Failures      int    `json:"failures"`
	UniqueID      string `json:"unique_id"`
}

// supportedRunStatuses is the structured-result contract's status
// vocabulary — dbt's RunStatus, per continuo_validation_contract/result.py's
// docstring. Parse accepts a candidate only when its status is one of these.
var supportedRunStatuses = map[string]bool{
	"success": true,
	"error":   true,
	"fail":    true,
	"skipped": true,
}

// Split removes the structured-result sentinel block from a pod log and
// returns the cleaned log plus the inner single-line JSON. When no
// well-formed block is present (production dbt jobs, old images, truncated
// logs) it returns the log unchanged and an empty structured string — the
// caller then degrades to the text-log-only path.
func Split(log string) (cleanLog, structuredJSON string) {
	bi := strings.Index(log, SentinelBegin)
	if bi < 0 {
		return log, ""
	}
	ei := strings.Index(log, SentinelEnd)
	if ei < 0 || ei < bi {
		return log, ""
	}
	inner := strings.TrimSpace(log[bi+len(SentinelBegin) : ei])
	clean := log[:bi] + log[ei+len(SentinelEnd):]
	return clean, strings.TrimSpace(inner)
}

// Parse decodes the JSON object inside a structured validation-result block
// (typically the string Split already isolated between the sentinel
// markers). Real pod logs can carry a stderr preamble before the JSON
// object, and/or trailing text after it, inside those markers, so this scans
// for the object rather than unmarshalling the whole body: try each '{' in
// turn, decode a single JSON value from there (ignoring anything after it),
// and accept the first candidate whose schema_version matches SchemaVersion
// and whose status is one of supportedRunStatuses. Because the scan
// tolerates arbitrary preamble text, it can otherwise reach an unrelated
// status-bearing JSON object before the real result; requiring both checks
// is what tells the two apart — a candidate that decodes but fails either
// one is not the contract's block, so scanning continues to the next '{'
// rather than accepting it. An error is returned when no such object is
// found, so callers can fall back to a text-log-only path rather than
// misclassifying.
//
// This guard is only as good as SchemaVersion: a future contract schema
// bump must update that constant too, or every block from the new schema is
// silently rejected here.
func Parse(raw []byte) (*Result, error) {
	for start := bytes.IndexByte(raw, '{'); start >= 0; {
		var candidate Result
		if err := json.NewDecoder(bytes.NewReader(raw[start:])).Decode(&candidate); err == nil &&
			candidate.SchemaVersion == SchemaVersion &&
			supportedRunStatuses[candidate.Status] {
			return &candidate, nil
		}
		next := bytes.IndexByte(raw[start+1:], '{')
		if next < 0 {
			break
		}
		start += next + 1
	}
	return nil, fmt.Errorf("no structured validation-result JSON object found in %d bytes", len(raw))
}
