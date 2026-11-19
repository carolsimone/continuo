package sanitize

import (
	"regexp"
	"strconv"
	"strings"
)

// numberedMarker matches a numbered placeholder; its kind is capture group 1.
var numberedMarker = regexp.MustCompile(`\[redacted:([a-z]+):[0-9]+\]`)

// issued names a value by what it was and its text.
type issued struct {
	kind  kind
	value string
}

// Redactor cleans raw text with the rules of Text, but writes a numbered
// placeholder ([redacted:email:1]) for each distinct value, so that Restore can
// put the values back. The numbers count per kind, in the order the Redactor
// first sees each value, across every text it cleans. A value keeps its
// placeholder for the life of the Redactor.
//
// Clean reads its input as raw text. A numbered placeholder in that text
// ([redacted:secret:1]) is text of that input, never one this Redactor issued,
// even when it has the same number as one: Clean numbers it as a value of its
// own, of its kind, whose text is the placeholder as written, so Restore writes
// it back as written. A placeholder inside a larger value a rule redacts whole
// is part of that value's raw text. A fixed marker from Text ([redacted:secret])
// stays as it is, and Restore reports it. So every value the Redactor stores is
// raw text of an input, and Restore(Clean(x)) is x for every text x, after any
// number of texts cleaned by the same Redactor. Cleaning a cleaned text numbers
// its placeholders again as text.
//
// A Redactor belongs to one caller at a time.
type Redactor struct {
	placeholders map[issued]string
	values       map[string]string
	counts       map[kind]int
}

// NewRedactor returns a Redactor that has issued no placeholder.
func NewRedactor() *Redactor {
	return &Redactor{
		placeholders: map[issued]string{},
		values:       map[string]string{},
		counts:       map[kind]int{},
	}
}

// Clean returns the raw text s with each secret, key and email address, and
// each numbered placeholder s holds, replaced by its placeholder.
func (r *Redactor) Clean(s string) string {
	var b strings.Builder
	for _, p := range redactPieces(s) {
		if p.kind != "" {
			b.WriteString(r.placeholder(p.kind, p.text))
			continue
		}
		b.WriteString(r.numberPlaceholders(p.text))
	}
	return b.String()
}

// numberPlaceholders returns the plain text s with each numbered placeholder it
// holds replaced by the placeholder this Redactor issues for that text.
func (r *Redactor) numberPlaceholders(s string) string {
	if !strings.Contains(s, "[redacted:") {
		return s
	}
	return numberedMarker.ReplaceAllStringFunc(s, func(literal string) string {
		return r.placeholder(kind(numberedMarker.FindStringSubmatch(literal)[1]), literal)
	})
}

// placeholder returns the placeholder of a value, issuing the next number of its
// kind when the value has none.
func (r *Redactor) placeholder(k kind, value string) string {
	id := issued{k, value}
	if placeholder, ok := r.placeholders[id]; ok {
		return placeholder
	}
	r.counts[k]++
	placeholder := "[redacted:" + string(k) + ":" + strconv.Itoa(r.counts[k]) + "]"
	r.placeholders[id] = placeholder
	r.values[placeholder] = value
	return placeholder
}

// Restore returns s with each placeholder this Redactor issued replaced by its
// value, in one pass that does not read the values it puts back. Every other
// placeholder in s, numbered or not (a fixed marker from Text is never
// restorable), stays in the text and is listed in unknown, in the order it
// first appears and once.
func (r *Redactor) Restore(s string) (restored string, unknown []string) {
	seen := map[string]struct{}{}
	restored = markerAny.ReplaceAllStringFunc(s, func(placeholder string) string {
		if value, ok := r.values[placeholder]; ok {
			return value
		}
		if _, dup := seen[placeholder]; !dup {
			seen[placeholder] = struct{}{}
			unknown = append(unknown, placeholder)
		}
		return placeholder
	})
	return restored, unknown
}
