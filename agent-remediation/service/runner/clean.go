package runner

import (
	"cmp"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
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
// first the evidence the cluster gathered, then the findings of every draft
// before the cluster's own, draft by draft.
type redaction struct {
	redactor *sanitize.Redactor
	evidence remediation.Evidence
	failure  remediation.Failure
	// findings are the cleaned findings of the draft before the cluster's,
	// which a revision shows the model.
	findings []remediation.Finding
}

// redactionFor builds the redaction of the cluster's draft from ev, the raw
// evidence the cluster gathered, and the raw findings the journal holds for
// each earlier draft's pre-flight pass. Every draft's findings go through the
// Redactor, so a value a finding of draft 1 quotes keeps its placeholder in
// every later draft and no number means two values in one request.
func (r *Runner) redactionFor(run tryRun, c remediation.Cluster, ev remediation.Evidence) (redaction, error) {
	red := redaction{redactor: sanitize.NewRedactor()}
	red.evidence = cleanEvidence(ev, red.redactor)
	f, ok := remediation.Get[remediation.Failure](red.evidence, remediation.KeyFailure)
	if !ok {
		return red, fmt.Errorf("the evidence holds no %s", remediation.KeyFailure)
	}
	red.failure = f
	for draft := 1; draft < c.Draft; draft++ {
		pass, done := run.journal.done(c.Cluster, stepPreflight, draft)
		if !done {
			return red, fmt.Errorf("draft %d's pre-flight pass has no done row", draft)
		}
		red.findings = cleanValue(pass.Findings, red.redactor.Clean)
	}
	return red, nil
}

// prompt returns in with the cleaned failure, evidence and findings of the
// redaction. The previous draft's request and answer stay as the journal holds
// them: the request was cleaned when it was sent and the answer holds the
// model's placeholders, so cleaning either again would number its placeholders
// as text of their own.
func (red redaction) prompt(in step.PromptInput) step.PromptInput {
	in.Failure, in.Evidence = red.failure, red.evidence
	if in.Revision > 0 {
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

// placeholderToken matches a placeholder as a Redactor writes it, numbered
// ([redacted:email:1]) or fixed ([redacted:email]), and numberedPlaceholder
// a numbered one only.
var (
	placeholderToken    = regexp.MustCompile(`\[redacted:[a-z]+(?::[0-9]+)?\]`)
	numberedPlaceholder = regexp.MustCompile(`^\[redacted:[a-z]+:[0-9]+\]$`)
)

// restoreDraft returns d with the placeholders r issued replaced by their
// values, and a finding for each placeholder the draft may not keep:
//
//   - In an edit's content a placeholder is replaced only when its value is in
//     the edit's original or in a text of code the cluster was shown (code).
//     A value the model saw only in a log, a precedent or a finding never goes
//     into a file: its placeholder stays in the content and is a finding.
//     Placeholders r never issued stay too, for markerFindings to judge once
//     the edit's original is known.
//   - In an edit's path, which a pull request writes, a placeholder is
//     replaced only when its value is in one of code or in an edit's
//     original. Any other issued placeholder stays and is a finding.
//   - In the rationale, which a pull request shows, a placeholder is replaced
//     by the same rule as in a path. Any other placeholder stays as it is,
//     with no finding: a marker in a pull request's text is harmless, a value
//     from a log is not.
//   - In the path and the rationale, each numbered placeholder r never issued
//     is a finding.
//
// A value is in a text when it occurs there as a whole token (shownIn).
//
// Every other text of the draft is restored whole.
func restoreDraft(d remediation.Draft, r *sanitize.Redactor, code []string) (remediation.Draft, []remediation.Finding) {
	restored := cleanValue(d, func(s string) string {
		out, _ := r.Restore(s)
		return out
	})
	var findings []remediation.Finding
	unknownIn := func(text, file string) {
		_, unknown := r.Restore(text)
		for _, p := range unknown {
			if numberedPlaceholder.MatchString(p) {
				findings = append(findings, unknownFinding(placeholderNeverShown, p, file))
			}
		}
	}
	originals := make([]string, 0, len(restored.Edits))
	for _, e := range restored.Edits {
		originals = append(originals, e.Original)
	}
	for i, e := range d.Edits {
		unknownIn(e.Path, "")
		path, refused := restoreShown(e.Path, r, inAny(code, originals))
		restored.Edits[i].Path = path
		for _, p := range refused {
			findings = append(findings, unknownFinding(placeholderFromALogInPath, p, ""))
		}
		content, refused := restoreShown(e.Content, r, inAny([]string{originals[i]}, code))
		restored.Edits[i].Content = content
		for _, p := range refused {
			findings = append(findings, unknownFinding(placeholderFromALog, p, path))
		}
	}
	unknownIn(d.Rationale, "")
	restored.Rationale, _ = restoreShown(d.Rationale, r, inAny(code, originals))
	return restored, findings
}

// inAny reports whether a value is in any text of the lists, as shownIn
// tells.
func inAny(lists ...[]string) func(value string) bool {
	return func(value string) bool {
		for _, texts := range lists {
			if slices.ContainsFunc(texts, func(t string) bool { return shownIn(t, value) }) {
				return true
			}
		}
		return false
	}
}

// shownIn reports whether value occurs in text as a whole token: where the
// value begins with a word character (a letter, a digit or an underscore), the
// character before it is not one, and where it ends with one, the character
// after it is not one. So admin is in user = 'admin' but not in administrator.
func shownIn(text, value string) bool {
	if value == "" {
		return false
	}
	first, _ := utf8.DecodeRuneInString(value)
	last, _ := utf8.DecodeLastRuneInString(value)
	for from := 0; from <= len(text)-len(value); {
		i := strings.Index(text[from:], value)
		if i < 0 {
			return false
		}
		start, end := from+i, from+i+len(value)
		before, _ := utf8.DecodeLastRuneInString(text[:start])
		after, _ := utf8.DecodeRuneInString(text[end:])
		if (!isWord(first) || start == 0 || !isWord(before)) && (!isWord(last) || end == len(text) || !isWord(after)) {
			return true
		}
		_, size := utf8.DecodeRuneInString(text[start:])
		from = start + size
	}
	return false
}

// isWord reports whether c is a word character: a letter, a digit or an
// underscore.
func isWord(c rune) bool {
	return c == '_' || unicode.IsLetter(c) || unicode.IsDigit(c)
}

// restoreShown returns text with each placeholder r issued replaced by its
// value when shown reports the value, in one pass, and the issued placeholders
// it left because shown does not, each once, in the order they appear.
// Placeholders r never issued stay as they are.
func restoreShown(text string, r *sanitize.Redactor, shown func(value string) bool) (string, []string) {
	var refused []string
	out := placeholderToken.ReplaceAllStringFunc(text, func(p string) string {
		value, unknown := r.Restore(p)
		if len(unknown) > 0 {
			return p
		}
		if shown(value) {
			return value
		}
		if !slices.Contains(refused, p) {
			refused = append(refused, p)
		}
		return p
	})
	return out, refused
}

// markerFindings is a finding for each redaction marker an edit of d adds to
// its file: a placeholder r never issued, or the remains of one in another
// case or without its bracket, that the edit's content holds more times than
// its original does (a new file's original is empty). So a file that already
// holds such a marker can be fixed with the marker kept. The issued
// placeholders restoreDraft left in the content are its own findings.
func markerFindings(d remediation.Draft, r *sanitize.Redactor) []remediation.Finding {
	var findings []remediation.Finding
	for _, e := range d.Edits {
		var seen []string
		for _, m := range leftoverPlaceholder.FindAllString(e.Content, -1) {
			if slices.Contains(seen, m) {
				continue
			}
			seen = append(seen, m)
			if issued(r, m) {
				continue
			}
			if strings.Count(e.Content, m) > strings.Count(e.Original, m) {
				findings = append(findings, unknownFinding(placeholderAdded, m, e.Path))
			}
		}
	}
	return findings
}

// issued reports whether token is a numbered placeholder r issued.
func issued(r *sanitize.Redactor, token string) bool {
	if !numberedPlaceholder.MatchString(token) {
		return false
	}
	_, unknown := r.Restore(token)
	return len(unknown) == 0
}

// The messages of the finding unknown_placeholder, each naming the token.
const (
	placeholderAdded          = "the answer adds a redaction marker (%s) the file did not hold; keep the text you were shown"
	placeholderFromALog       = "the answer holds a placeholder (%s) for a value from a log or a check, not from the code; leave that value out of the file"
	placeholderFromALogInPath = "the answer names a file with a placeholder (%s) for a value from a log or a check, not from the code; leave that value out of the path"
	placeholderNeverShown     = "the answer holds a placeholder (%s) that was never in what you were shown; keep the text you were shown"
)

// unknownFinding is the finding unknown_placeholder with message naming token,
// for file. The token is written without its brackets and colons, so that
// cleaning the finding for the next draft never takes it for a placeholder or
// a secret and the model reads it as it was found.
func unknownFinding(message, token, file string) remediation.Finding {
	name := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(token, "["), "]"), ":", " ")
	return remediation.Finding{Code: findingUnknownPlaceholder, Message: fmt.Sprintf(message, name), File: file}
}

// codeTexts is every text of the evidence ev holds under a key of the data
// class code, as classOf tells the class of each key.
func codeTexts(ev remediation.Evidence, classOf func(remediation.EvidenceKey) (workflow.DataClass, bool)) []string {
	var texts []string
	for _, k := range ev.Keys() {
		if class, ok := classOf(k); !ok || class != workflow.ClassCode {
			continue
		}
		v, _ := remediation.Get[any](ev, k)
		cleanValue(v, func(s string) string {
			if s != "" {
				texts = append(texts, s)
			}
			return s
		})
	}
	return texts
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
