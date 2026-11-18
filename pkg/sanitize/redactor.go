package sanitize

import (
	"regexp"
	"strconv"
	"strings"
)

// maxReservedDigits is the most digits a literal placeholder's number may have
// for its count to be reserved. A longer number is kept as it is and moves no
// count, so the numbers the Redactor issues stay far from an overflow.
const maxReservedDigits = 9

// numberedMarker matches a numbered placeholder: its kind, then its number.
var numberedMarker = regexp.MustCompile(`\[redacted:([a-z]+):([0-9]+)\]`)

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
// A numbered placeholder a text holds that this Redactor has not issued is the
// text's own, as in a document that quotes one: Clean keeps it, never issues
// its number to a value, and Restore returns it unchanged. A placeholder this
// Redactor issued, found again in a later text, cannot be told from the text's
// own, so Clean takes that text for one it cleaned already.
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

// Clean returns s with each secret, key and email address replaced by its
// placeholder. Cleaning a text that is already clean returns it unchanged.
func (r *Redactor) Clean(s string) string {
	r.reserveLiterals(s)
	return render(redactPieces(s), r.placeholder)
}

// placeholder returns the placeholder of a value, issuing the next number of its
// kind that no literal placeholder holds when the value has none.
func (r *Redactor) placeholder(k kind, value string) string {
	id := issued{k, value}
	if placeholder, ok := r.placeholders[id]; ok {
		return placeholder
	}
	for {
		r.counts[k]++
		placeholder := "[redacted:" + string(k) + ":" + strconv.Itoa(r.counts[k]) + "]"
		if _, taken := r.values[placeholder]; taken {
			continue
		}
		r.placeholders[id] = placeholder
		r.values[placeholder] = value
		return placeholder
	}
}

// Restore returns s with each placeholder this Redactor issued replaced by its
// value. A numbered placeholder a cleaned text held as its own stays as it is.
// Every other placeholder in s, numbered or not (a fixed marker from Text is
// never restorable), stays in the text and is listed in unknown, in the order
// it first appears and once.
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

// reserveLiterals sets aside every numbered placeholder s holds that this
// Redactor did not issue: Restore maps it to itself, and the count of its kind
// is raised to its number, so no value is issued it. A number the Redactor
// would not write (a leading zero) or one of more than maxReservedDigits digits
// is reserved without moving the count.
func (r *Redactor) reserveLiterals(s string) {
	if !strings.Contains(s, "[redacted:") {
		return
	}
	for _, m := range numberedMarker.FindAllStringSubmatch(s, -1) {
		placeholder, k, digits := m[0], kind(m[1]), m[2]
		if _, known := r.values[placeholder]; known {
			continue
		}
		r.values[placeholder] = placeholder
		if n, err := strconv.Atoi(digits); err == nil && len(digits) <= maxReservedDigits && strconv.Itoa(n) == digits && n > r.counts[k] {
			r.counts[k] = n
		}
	}
}
