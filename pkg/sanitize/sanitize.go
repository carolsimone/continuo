// Package sanitize is the single redaction seam for untrusted text the system
// stores or forwards to an LLM: dbt logs on the remediation path and model
// source code on the graph-ingestion path.
//
// Text replaces every credential, private key and email address it finds with
// a fixed marker. A Redactor applies the same rules but gives each distinct
// value its own numbered placeholder and can put the values back, so a model
// can be shown a text without its secrets and its answer can be restored.
//
// The rules, in order, with the marker each one writes:
//
//  1. a private key block, from its BEGIN line to its END line, or to the end
//     of the text when the END line is missing (key);
//  2. the password of a URL, scheme://user:password@host (secret);
//  3. a token of a known shape: an AWS access key id, a GitHub token, a JWT or
//     a bearer token (secret);
//  4. the value of a key that names a secret, such as password=..., "token":
//     "..." or api_key: ... (secret);
//  5. an email address (email).
//
// Every rule reads the original text, never the output of an earlier rule. When
// two rules match overlapping text the earlier rule wins and the later match
// is dropped, so password=ops@acme.com is one secret and not a secret holding
// an email address. A marker is never matched by a rule, so cleaning a cleaned
// text changes nothing.
package sanitize

import (
	"regexp"
	"sort"
	"strings"
)

// kind is what a redacted value was, and names its marker.
type kind string

const (
	kindKey    kind = "key"
	kindSecret kind = "secret"
	kindEmail  kind = "email"
)

// markerPattern matches a marker, fixed ([redacted:secret]) or numbered
// ([redacted:secret:3]).
const markerPattern = `\[redacted:[a-z]+(?::[0-9]+)?\]`

var (
	markerAny   = regexp.MustCompile(markerPattern)
	markerExact = regexp.MustCompile(`^` + markerPattern + `$`)
)

// urlChars is what a URL's user and password cannot hold: whitespace, a slash
// that ends the authority, a quote or bracket that ends the URL in running
// text.
const urlChars = `\s/'"<>\x60`

// caseless writes a word as a pattern that matches it in any ASCII case.
func caseless(word string) string {
	var b strings.Builder
	for _, c := range word {
		b.WriteString("[" + strings.ToUpper(string(c)) + strings.ToLower(string(c)) + "]")
	}
	return b.String()
}

// secretKey is a key that names a secret. It may carry a prefix
// (DB_PASSWORD, client_secret) but not a suffix (password_hint, tokens).
var secretKey = `(?:` + strings.Join([]string{
	caseless("password"),
	caseless("passwd"),
	caseless("secret") + `(?:[_-]?` + caseless("key") + `)?`,
	caseless("token"),
	caseless("api") + `[_-]?` + caseless("key"),
	caseless("access") + `[_-]?` + caseless("key"),
	caseless("private") + `[_-]?` + caseless("key"),
}, "|") + `)`

// secretKeyHints are the lower case words a text must hold, in some case, for
// secretKey to match in it.
var secretKeyHints = []string{"password", "passwd", "secret", "token", "api", "access", "private"}

// secretValue is a secret key and its value. The value is one of: nothing
// (an empty quoted string), a double quoted string, a double quoted string a
// log escaped, a single quoted string, or an unquoted run that ends at a
// space or a separator. Each non-empty form is a capture group, in this order,
// and holds the value without its quotes. An unquoted value never starts with
// a colon (a cast, token::text) or an angle bracket (a hash rocket).
var secretValue = secretKey + `\\?["']?[ \t]*(?:=>|[=:]=?)[ \t]*(?:""|''` +
	`|"((?:[^"\\\r\n]|\\.)+)"` +
	`|\\"((?:[^"\\\r\n]|\\[^"])+)\\"` +
	`|'((?:[^'\\\r\n]|\\.)+)'` +
	`|((?:` + markerPattern + `|[^\s,;)\]}&:>])(?:` + markerPattern + `|[^\s,;)\]}&"'])*))`

// pattern is one regular expression of a rule and the capture groups that hold
// the text to replace. With no group the whole match is replaced; with groups
// the first one that took part in the match is. needs lists lower case words
// the pattern cannot match without: it is tried only on a text that holds one
// of them, in some case, which spares the slow patterns the most of a large
// text that has none.
type pattern struct {
	re     *regexp.Regexp
	groups []int
	needs  []string
}

// rule is one redaction rule.
type rule struct {
	kind     kind
	patterns []pattern
}

// rules are the redaction rules in the order they decide an overlap.
var rules = []rule{
	{kindKey, []pattern{
		{regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\z)`), nil, nil},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(`://[^` + urlChars + `:]*:([^` + urlChars + `]+)@[^` + urlChars + `]`), []int{1}, nil},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(`AKIA[0-9A-Z]{16}`), nil, nil},
		{regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{36,}`), nil, nil},
		{regexp.MustCompile(`eyJ[\w-]+\.eyJ[\w-]+\.[\w-]+`), nil, nil},
		{regexp.MustCompile(`\b[Bb]earer[ \t]+[A-Za-z0-9._~+/=-]{8,}`), nil, []string{"earer"}},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(secretValue), []int{1, 2, 3, 4}, secretKeyHints},
	}},
	{kindEmail, []pattern{
		{regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), nil, []string{"@"}},
	}},
}

// span is a stretch of the text a rule redacts.
type span struct {
	start, end int
	kind       kind
}

// candidates are the spans a rule finds in s, by position. lower is s in lower
// case.
func (r rule) candidates(s, lower string) []span {
	var found []span
	for _, p := range r.patterns {
		if len(p.needs) > 0 && !containsAny(lower, p.needs) {
			continue
		}
		for _, loc := range p.re.FindAllStringSubmatchIndex(s, -1) {
			start, end := loc[0], loc[1]
			if len(p.groups) > 0 {
				start, end = -1, -1
				for _, g := range p.groups {
					if loc[2*g] >= 0 {
						start, end = loc[2*g], loc[2*g+1]
						break
					}
				}
			}
			if end > start {
				found = append(found, span{start, end, r.kind})
			}
		}
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].start < found[j].start })
	return found
}

// findSpans returns the spans to replace, by position and without overlap.
// A rule's match is kept unless it overlaps text an earlier match holds. A
// match that is itself a marker holds its text and replaces nothing, so a
// cleaned text is left as it is and the rules that would have read it stay out.
// A match that lies inside a marker is dropped: the numbered marker
// [redacted:secret:1] reads like the key secret with the value 1.
func findSpans(s string) []span {
	markers := markerAny.FindAllStringIndex(s, -1)
	lower := strings.ToLower(s)
	var held []bool
	var spans []span
	for _, r := range rules {
		for _, c := range r.candidates(s, lower) {
			marker := isMarker(s[c.start:c.end])
			if !marker && insideAny(markers, c) {
				continue
			}
			if held == nil {
				held = make([]bool, len(s))
			}
			if anyHeld(held[c.start:c.end]) {
				continue
			}
			for i := c.start; i < c.end; i++ {
				held[i] = true
			}
			if !marker {
				spans = append(spans, c)
			}
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	return spans
}

// insideAny reports whether the span lies within one of the marker locations,
// which are in order and do not overlap.
func insideAny(markers [][]int, c span) bool {
	i := sort.Search(len(markers), func(i int) bool { return markers[i][1] > c.start })
	return i < len(markers) && markers[i][0] <= c.start && c.end <= markers[i][1]
}

func containsAny(s string, words []string) bool {
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

func anyHeld(held []bool) bool {
	for _, h := range held {
		if h {
			return true
		}
	}
	return false
}

func isMarker(s string) bool {
	return strings.HasPrefix(s, "[redacted:") && markerExact.MatchString(s)
}

// redact rewrites s, replacing each span with what replace returns for the
// span's kind and text. replace is called in the order of the spans.
func redact(s string, replace func(k kind, value string) string) string {
	spans := findSpans(s)
	if len(spans) == 0 {
		return s
	}
	var out strings.Builder
	out.Grow(len(s))
	last := 0
	for _, sp := range spans {
		out.WriteString(s[last:sp.start])
		out.WriteString(replace(sp.kind, s[sp.start:sp.end]))
		last = sp.end
	}
	out.WriteString(s[last:])
	return out.String()
}

// Text returns s with every private key, credential and email address replaced
// by its marker: [redacted:key], [redacted:secret] or [redacted:email]. It is a
// function of s alone.
func Text(s string) string {
	return redact(s, func(k kind, _ string) string {
		return "[redacted:" + string(k) + "]"
	})
}
