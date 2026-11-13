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
// continuo_engine_contract/result.py's result_block(); execution-controller
// uploads it as run_results_uri and prefers its message as a failed task's
// error_message, and remediation reads it back to classify the failure.
// status uses dbt's RunStatus vocabulary.
type Result struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	Failures      int    `json:"failures"`
	UniqueID      string `json:"unique_id"`
	// Reason is a machine-readable cause the pod names, empty when it names none;
	// the python harness writes image_contract_mismatch when its image does not
	// hash to the dispatched content_hash.
	Reason string `json:"reason"`
}

// supportedRunStatuses is the structured-result contract's status
// vocabulary — dbt's RunStatus, per continuo_engine_contract/result.py's
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
	if r, ok := scanBlock(raw, func(r *Result) bool {
		return r.SchemaVersion == SchemaVersion && supportedRunStatuses[r.Status]
	}); ok {
		return r, nil
	}
	return nil, fmt.Errorf("no structured validation-result JSON object found in %d bytes", len(raw))
}

// The values of a preflight block's op field, which are the VALIDATION_OP
// values of the two runs that print one.
const (
	// OpSeedPreflight is the block of the check that lists what depends on a
	// seed's table before the seed loads.
	OpSeedPreflight = "seed_preflight"
	// OpProbeViews is the block of the probe that queries each view outside
	// the allowed set that a seed's preflight found.
	OpProbeViews = "probe_views"
)

// Preflight is the result block of a seed preflight or a view probe: a Result
// (its Status, Message, Failures and Reason) plus the findings of the
// warehouse. A check-mode preflight that stops the load has status error,
// failures 1 and the reason that stopped it.
type Preflight struct {
	Result
	// Op is OpSeedPreflight or OpProbeViews.
	Op string `json:"op"`
	// DropSemantics is how the engine treats a drop of a table that views read.
	DropSemantics string `json:"drop_semantics"`
	// Completeness is exact or best_effort: whether Dependents lists every dependant.
	Completeness string `json:"completeness"`
	// Checked is true when the warehouse was queried to the end.
	Checked bool `json:"checked"`
	// Dependents are the objects that depend on the seed's table.
	Dependents []Dependent `json:"dependents"`
	// Views are the answers of a view probe.
	Views []ProbedView `json:"views"`
}

// Dependent is one object of a preflight's findings.
type Dependent struct {
	Schema  string `json:"schema"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Owner   string `json:"owner"`
	Depth   int    `json:"depth"`
	Allowed bool   `json:"allowed"`
}

// ProbedView is the answer of a probe of one view.
type ProbedView struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
}

// ParsePreflight decodes the preflight or probe block in raw with the scan
// Parse uses, and accepts the first candidate that Parse would accept and whose
// op is OpSeedPreflight or OpProbeViews. An error is returned when there is
// none.
func ParsePreflight(raw []byte) (*Preflight, error) {
	if p, ok := scanBlock(raw, func(p *Preflight) bool {
		return p.SchemaVersion == SchemaVersion && supportedRunStatuses[p.Status] &&
			(p.Op == OpSeedPreflight || p.Op == OpProbeViews)
	}); ok {
		return p, nil
	}
	return nil, fmt.Errorf("no seed preflight or view probe JSON object found in %d bytes", len(raw))
}

// scanBlock decodes a T from each '{' of raw in turn, ignoring what follows
// each object, and returns the first that accept approves.
func scanBlock[T any](raw []byte, accept func(*T) bool) (*T, bool) {
	for start := bytes.IndexByte(raw, '{'); start >= 0; {
		var candidate T
		if err := json.NewDecoder(bytes.NewReader(raw[start:])).Decode(&candidate); err == nil && accept(&candidate) {
			return &candidate, true
		}
		next := bytes.IndexByte(raw[start+1:], '{')
		if next < 0 {
			break
		}
		start += next + 1
	}
	return nil, false
}
