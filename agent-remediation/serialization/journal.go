package serialization

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// The runner's journal keeps one remediation_step row per run of a step, and
// a done row's output column holds one of the records below as JSON. A
// record's own keys are set by its tags. The domain values it carries (a
// draft, a check result, a piece of evidence) carry no tags and are encoded by
// their Go field names, so renaming a field of one changes the rows the
// journal stores and reads back; journal_golden_test.go pins the stored JSON of
// every record and of every evidence key's value.

// EvidenceTypes gives a constructor of the zero value of the Go type evidence
// key k holds, and false for a key nothing provides. The runner's registry's
// EvidenceType method is one.
type EvidenceTypes func(k remediation.EvidenceKey) (func() any, bool)

// EncodedEvidence is evidence encoded one value at a time, by key, so each
// value decodes into the Go type its key holds.
type EncodedEvidence map[remediation.EvidenceKey]json.RawMessage

// EncodeEvidence encodes each piece of evidence a step gathered on its own. It
// refuses a key types does not know, since its value could not be decoded
// again. No evidence encodes as nil.
func EncodeEvidence(values map[remediation.EvidenceKey]any, types EvidenceTypes) (EncodedEvidence, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(EncodedEvidence, len(values))
	for k, v := range values {
		if _, ok := types(k); !ok {
			return nil, fmt.Errorf("evidence %q has no registered type", k)
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("encode evidence %q: %w", k, err)
		}
		out[k] = raw
	}
	return out, nil
}

// AddTo returns ev with every encoded piece of evidence added, each decoded
// into the Go type types gives its key, in key order.
func (e EncodedEvidence) AddTo(ev remediation.Evidence, types EvidenceTypes) (remediation.Evidence, error) {
	keys := make([]remediation.EvidenceKey, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		zero, ok := types(k)
		if !ok {
			return ev, fmt.Errorf("evidence %q has no registered type", k)
		}
		ptr := reflect.New(reflect.TypeOf(zero()))
		if err := json.Unmarshal(e[k], ptr.Interface()); err != nil {
			return ev, fmt.Errorf("decode evidence %q: %w", k, err)
		}
		ev = ev.With(k, ptr.Elem().Interface())
	}
	return ev, nil
}

// ContextRecord is a context step's output as its row stores it: the evidence
// it gathered, or its decision to decline the cluster.
type ContextRecord struct {
	Evidence EncodedEvidence `json:"evidence,omitempty"`
	Declined bool            `json:"declined,omitempty"`
	Decline  string          `json:"decline,omitempty"`
}

// PromptRecord is a prompt step's output as its row stores it: the model's
// draft, or its decision to decline the cluster.
type PromptRecord struct {
	Draft    remediation.Draft `json:"draft"`
	Declined bool              `json:"declined,omitempty"`
	Decline  string            `json:"decline,omitempty"`
}

// CheckRecord is a check's output as its row stores it: its result and, for a
// pre-flight check, the evidence it gathered. A cross-cluster check and the
// gate store a result only.
type CheckRecord struct {
	Result   remediation.CheckResult `json:"result"`
	Evidence EncodedEvidence         `json:"evidence,omitempty"`
}

// PreflightRecord is the output of the runner's pre-flight pass of a cluster:
// the draft as the pass left it.
type PreflightRecord struct {
	Draft remediation.Draft `json:"draft"`
}

// JournalRecord is any record a journal row stores as its output.
type JournalRecord interface {
	ContextRecord | PromptRecord | CheckRecord | PreflightRecord
}

// EncodeRecord encodes rec as a journal row's output.
func EncodeRecord[R JournalRecord](rec R) ([]byte, error) {
	return json.Marshal(rec)
}

// DecodeRecord decodes a journal row's output into the record its step stored.
// An output that does not decode returns the zero record and the decoder's
// error.
func DecodeRecord[R JournalRecord](output []byte) (R, error) {
	var rec R
	if err := json.Unmarshal(output, &rec); err != nil {
		var zero R
		return zero, err
	}
	return rec, nil
}

// evidenceValues is every piece of evidence ev holds, by key.
func evidenceValues(ev remediation.Evidence) map[remediation.EvidenceKey]any {
	out := make(map[remediation.EvidenceKey]any, len(ev.Keys()))
	for _, k := range ev.Keys() {
		v, _ := remediation.Get[any](ev, k)
		out[k] = v
	}
	return out
}

// digest is the hex SHA-256 of raw.
func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// InputDigest identifies what a step of a cluster ran on: the cluster's
// evidence and the round. Two runs with the same digest were given the same
// input.
func InputDigest(ev remediation.Evidence, round int) (string, error) {
	raw, err := json.Marshal(struct {
		Evidence map[remediation.EvidenceKey]any `json:"evidence"`
		Round    int                             `json:"round"`
	}{evidenceValues(ev), round})
	if err != nil {
		return "", fmt.Errorf("encode the step's input: %w", err)
	}
	return digest(raw), nil
}

// DigestedDraft is one draft a flow-level step was given: the number of its
// cluster, the draft and the cluster's evidence.
type DigestedDraft struct {
	Cluster  int
	Draft    remediation.Draft
	Evidence remediation.Evidence
}

// DraftsDigest identifies what a flow-level step ran on: every draft it was
// given, with its cluster and evidence.
func DraftsDigest(drafts []DigestedDraft) (string, error) {
	type entry struct {
		Cluster  int                             `json:"cluster"`
		Draft    remediation.Draft               `json:"draft"`
		Evidence map[remediation.EvidenceKey]any `json:"evidence"`
	}
	entries := make([]entry, 0, len(drafts))
	for _, d := range drafts {
		entries = append(entries, entry{Cluster: d.Cluster, Draft: d.Draft, Evidence: evidenceValues(d.Evidence)})
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return "", fmt.Errorf("encode the step's drafts: %w", err)
	}
	return digest(raw), nil
}
