package runner

import (
	"cmp"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"unicode/utf8"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/sanitize"
)

// findingUnknownPlaceholder is the code of the finding the runner makes when a
// draft holds placeholder text its Redactor never issued. It is also the name
// the runner gives that rule's result in the pre-flight pass.
const findingUnknownPlaceholder = "unknown_placeholder"

// leftoverPlaceholder matches what is left of a placeholder in any case,
// closed or not, such as [REDACTED:email:1] or [redacted:email:1 with its
// bracket lost.
var leftoverPlaceholder = regexp.MustCompile(`(?i)\[redacted[a-z0-9:_-]*\]?`)

// redaction is what one prompt run of a cluster shows the model, cleaned by
// one Redactor. The Redactor is built from the journal's raw contents in the
// same order on every run, so a resumed runner issues the same placeholders:
// first the evidence the cluster gathered, then the findings of every round
// before the cluster's own, round by round.
type redaction struct {
	redactor *sanitize.Redactor
	evidence remediation.Evidence
	failure  remediation.Failure
	// findings are the cleaned findings of the round before the cluster's,
	// which a revision shows the model.
	findings []remediation.Finding
}

// redactionFor builds the redaction of the cluster's round from ev, the raw
// evidence the cluster gathered, and the raw findings the journal holds for
// each earlier round's pre-flight pass. Every round's findings go through the
// Redactor, so a value a finding of round 1 quotes keeps its placeholder in
// every later round and no number means two values in one request.
func (r *Runner) redactionFor(run tryRun, c remediation.Cluster, ev remediation.Evidence) (redaction, error) {
	red := redaction{redactor: sanitize.NewRedactor()}
	red.evidence = cleanEvidence(ev, red.redactor)
	f, ok := remediation.Get[remediation.Failure](red.evidence, remediation.KeyFailure)
	if !ok {
		return red, fmt.Errorf("the evidence holds no %s", remediation.KeyFailure)
	}
	red.failure = f
	for round := 1; round < c.Round; round++ {
		pass, done := run.journal.done(c.Cluster, stepPreflight, round)
		if !done {
			return red, fmt.Errorf("round %d's pre-flight pass has no done row", round)
		}
		red.findings = cleanValue(pass.Findings, red.redactor.Clean)
	}
	return red, nil
}

// prompt returns in with the cleaned failure, evidence and findings of the
// redaction. The previous round's request and answer stay as the journal holds
// them: the request was cleaned when it was sent and the answer holds the
// model's placeholders, so cleaning either again would number its placeholders
// as text of their own.
func (red redaction) prompt(in step.PromptInput) step.PromptInput {
	in.Failure, in.Evidence = red.failure, red.evidence
	if in.Round > 0 {
		in.Findings = red.findings
	}
	return in
}

// cleanEvidence returns a copy of e in which every text is cleaned by r: every
// string, at any depth of structs, slices, arrays, maps, pointers and
// interfaces, and every byte slice that is valid UTF-8. Keys are cleaned in
// sorted order and so are the entries of a map, so the numbering is the same
// on every run. e is never changed.
func cleanEvidence(e remediation.Evidence, r *sanitize.Redactor) remediation.Evidence {
	out := remediation.NewEvidence()
	for _, k := range e.Keys() {
		v, _ := remediation.Get[any](e, k)
		out = out.With(k, cleanValue(v, r.Clean))
	}
	return out
}

// restoreDraft returns d with every placeholder r issued replaced by its value,
// in every text of the draft, and a finding for each placeholder left in the
// text that leaves the runner: an edit's path and content, which a pull
// request writes, and the rationale, which it shows. A placeholder r never
// issued and the remains of one in another case or without its bracket are
// all left, each found once per text, in the order they appear.
func restoreDraft(d remediation.Draft, r *sanitize.Redactor) (remediation.Draft, []remediation.Finding) {
	restored := cleanValue(d, func(s string) string {
		out, _ := r.Restore(s)
		return out
	})
	var findings []remediation.Finding
	find := func(text, file string) {
		out, unknown := r.Restore(text)
		for _, p := range leftovers(out, unknown) {
			findings = append(findings, remediation.Finding{
				Code:    findingUnknownPlaceholder,
				Message: fmt.Sprintf("the answer holds %s, which was never in what you were shown; keep the text you were shown", p),
				File:    file,
			})
		}
	}
	for _, e := range d.Edits {
		find(e.Path, "")
	}
	for i, e := range d.Edits {
		find(e.Content, restored.Edits[i].Path)
	}
	find(d.Rationale, "")
	return restored, findings
}

// leftovers is every placeholder Restore left in restored, unknown first and
// then the remains of placeholders it could not read, each once.
func leftovers(restored string, unknown []string) []string {
	out := slices.Clone(unknown)
	for _, p := range leftoverPlaceholder.FindAllString(restored, -1) {
		if !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	return out
}

// cleanValue returns a copy of v with fn applied to every text it holds, as
// cleanEvidence describes.
func cleanValue[T any](v T, fn func(string) string) T {
	rv := reflect.ValueOf(&v).Elem()
	out, _ := mapText(rv, fn).Interface().(T)
	return out
}

// mapText returns a copy of v with fn applied to every text it holds. A struct
// is copied whole and then each exported field is mapped; an unexported field
// keeps its value.
func mapText(v reflect.Value, fn func(string) string) reflect.Value {
	out := reflect.New(v.Type()).Elem()
	switch v.Kind() {
	case reflect.String:
		out.SetString(fn(v.String()))
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b := slices.Clone(v.Bytes())
			if utf8.Valid(b) {
				b = []byte(fn(string(b)))
			}
			out.SetBytes(b)
			return out
		}
		out.Set(reflect.MakeSlice(v.Type(), v.Len(), v.Len()))
		for i := range v.Len() {
			out.Index(i).Set(mapText(v.Index(i), fn))
		}
	case reflect.Array:
		for i := range v.Len() {
			out.Index(i).Set(mapText(v.Index(i), fn))
		}
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out.Set(reflect.MakeMapWithSize(v.Type(), v.Len()))
		keys := v.MapKeys()
		slices.SortFunc(keys, func(a, b reflect.Value) int { return cmp.Compare(sortKey(a), sortKey(b)) })
		for _, k := range keys {
			out.SetMapIndex(mapText(k, fn), mapText(v.MapIndex(k), fn))
		}
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		p := reflect.New(v.Type().Elem())
		p.Elem().Set(mapText(v.Elem(), fn))
		out.Set(p)
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out.Set(mapText(v.Elem(), fn))
	case reflect.Struct:
		out.Set(v)
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(mapText(v.Field(i), fn))
			}
		}
	default:
		out.Set(v)
	}
	return out
}

// sortKey orders the keys of a map: a string key by its text, any other key by
// its printed value.
func sortKey(k reflect.Value) string {
	if k.Kind() == reflect.String {
		return k.String()
	}
	return fmt.Sprint(k.Interface())
}
