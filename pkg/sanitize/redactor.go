package sanitize

import "strconv"

// issued names a value by what it was and its text.
type issued struct {
	kind  kind
	value string
}

// Redactor cleans text with the rules of Text, but writes a numbered
// placeholder ([redacted:email:1]) for each distinct value, so that Restore can
// put the values back. The numbers count per kind, in the order the Redactor
// first sees each value, across every text it cleans. A value keeps its
// placeholder for the life of the Redactor.
//
// A Redactor belongs to one caller at a time. Text that already holds a
// numbered placeholder is taken to have been cleaned by this Redactor.
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

// Clean returns s with each secret, key and email address replaced by its
// placeholder. Cleaning a text that is already clean returns it unchanged.
func (r *Redactor) Clean(s string) string {
	return redact(s, func(k kind, value string) string {
		id := issued{k, value}
		if placeholder, ok := r.placeholders[id]; ok {
			return placeholder
		}
		r.counts[k]++
		placeholder := "[redacted:" + string(k) + ":" + strconv.Itoa(r.counts[k]) + "]"
		r.placeholders[id] = placeholder
		r.values[placeholder] = value
		return placeholder
	})
}

// Restore returns s with each placeholder this Redactor issued replaced by its
// value. Every other placeholder in s, numbered or not (a fixed marker from
// Text is never restorable), stays in the text and is listed in unknown, in the
// order it first appears and once.
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
