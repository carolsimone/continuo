// Package sanitize is the single redaction seam for untrusted text the system
// stores or forwards to an LLM. catalog stores the code versions of a release
// and the rejection excerpts of a failed one through Text. The remediation
// runner cleans every text it sends the model with a Redactor and restores the
// model's draft with the same Redactor.
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
//  2. the password of a URL, scheme://user:password@host, where the user and
//     the password end before the first slash, question mark or number sign
//     (secret);
//  3. a token of a known shape: an AWS access key id, a GitHub token, a JWT or
//     a bearer token (secret);
//  4. the value of a key that names a secret, such as password=..., "token":
//     "..." or api_key: ..., and the quoted literal of an annotated assignment,
//     password: str = "..." (secret). An unquoted value is a literal only, not
//     code that reads a value (os.environ["TOKEN"], settings.PASSWORD,
//     ${TOKEN}), not a null or a boolean, not a block scalar and not a type;
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
// text. userinfoChars adds the question mark and the number sign, which end
// the authority too: an at sign after them belongs to the query or the
// fragment.
const (
	urlChars      = `\s/'"<>\x60`
	userinfoChars = urlChars + `?#`
)

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

// secretValue is a secret key and its value. The separator is capture group 1.
// The value is one of: nothing (an empty quoted string), a double quoted
// string, a double quoted string a log escaped, a single quoted string, or an
// unquoted run that ends at a space or a separator. Each non-empty form is a
// capture group, groups 2 to 5 in this order, and holds the value without its
// quotes. An unquoted value never starts with a colon (a cast, token::text) or
// an angle bracket (a hash rocket).
var secretValue = secretKey + `\\?["']?[ \t]*(=>|[=:]=?)[ \t]*(?:""|''` +
	`|"((?:[^"\\\r\n]|\\.)+)"` +
	`|\\"((?:[^"\\\r\n]|\\[^"])+)\\"` +
	`|'((?:[^'\\\r\n]|\\.)+)'` +
	`|((?:` + markerPattern + `|[^\s,;)\]}&:>])(?:` + markerPattern + `|[^\s,;)\]}&"'])*))`

// annotation is a type annotation: a name, optionally subscripted, or several
// of them joined by a bar.
const annotation = `[A-Za-z_][\w.]*(?:\[(?:[^\[\]\r\n=]|\[[^\[\]\r\n=]*\])*\])?`

// secretAnnotated is a secret key, an annotation and a quoted literal, as in
// password: str = "hunter2". The literal is capture group 1 or 2.
var secretAnnotated = secretKey + `[ \t]*:[ \t]*` + annotation + `(?:[ \t]*\|[ \t]*` + annotation + `)*` +
	`[ \t]*=[ \t]*[rRbBuUfF]{0,2}(?:"((?:[^"\\\r\n]|\\.)+)"|'((?:[^'\\\r\n]|\\.)+)')`

var (
	// A block scalar indicator opens a YAML value that is on the next lines.
	blockScalar = regexp.MustCompile(`^[|>](?:[0-9][+-]?|[+-][0-9]?)?$`)
	// A call or a subscript, possibly of a dotted name: os.getenv(, os.environ[.
	callOrSubscript = regexp.MustCompile(`^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*[(\[]`)
	// A dotted name with no digit: self.password, settings.DB_PASSWORD. Tokens
	// and passwords that hold dots hold digits too.
	dottedName = regexp.MustCompile(`^[A-Za-z_]+(?:\.[A-Za-z_]+)+$`)
	// The opening of a template or a shell expansion: {{ env_var(...) }}, ${TOKEN}.
	expansion = regexp.MustCompile(`^(?:\{\{|\{%|\$\{|\$\()`)
)

// notAValue are the words that stand for no value.
var notAValue = map[string]struct{}{"null": {}, "none": {}, "true": {}, "false": {}, "nil": {}, "~": {}}

// typeWords are the type names a field or a parameter is declared with.
var typeWords = map[string]struct{}{
	"str": {}, "int": {}, "float": {}, "bool": {}, "bytes": {}, "bytearray": {}, "complex": {}, "object": {},
	"list": {}, "dict": {}, "set": {}, "tuple": {}, "frozenset": {}, "Any": {}, "Final": {}, "ClassVar": {},
	"SecretStr": {}, "SecretBytes": {}, "string": {}, "number": {}, "boolean": {},
	"String": {}, "Integer": {}, "Boolean": {},
}

// keepSecretValue decides whether a match of secretValue holds a value. A
// quoted value always does. An unquoted one does when it is a literal: it is
// not code that reads a value, not an absent value, not a block scalar, and,
// after a colon, not a type.
func keepSecretValue(s string, loc []int) bool {
	if loc[10] < 0 {
		return true
	}
	value := s[loc[10]:loc[11]]
	if _, absent := notAValue[strings.ToLower(value)]; absent {
		return false
	}
	if blockScalar.MatchString(value) || callOrSubscript.MatchString(value) || dottedName.MatchString(value) ||
		expansion.MatchString(value) {
		return false
	}
	if s[loc[2]:loc[3]] == ":" {
		if _, isType := typeWords[value]; isType {
			return false
		}
		// A name followed by an equals sign is an annotation of what follows.
		if strings.HasPrefix(strings.TrimLeft(s[loc[11]:], " \t"), "=") {
			return false
		}
	}
	return true
}

// pattern is one regular expression of a rule and the capture groups that hold
// the text to replace. With no group the whole match is replaced; with groups
// the first one that took part in the match is. needs lists lower case words
// the pattern cannot match without: it is tried only on a text that holds one
// of them, in some case, which spares the slow patterns the most of a large
// text that has none. keep, when set, is asked about each match, with its
// submatch locations, and a match it refuses is dropped.
type pattern struct {
	re     *regexp.Regexp
	groups []int
	needs  []string
	keep   func(s string, loc []int) bool
}

// rule is one redaction rule.
type rule struct {
	kind     kind
	patterns []pattern
}

// rules are the redaction rules in the order they decide an overlap.
var rules = []rule{
	{kindKey, []pattern{
		{regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\z)`), nil, nil, nil},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(`://[^` + userinfoChars + `:]*:([^` + userinfoChars + `]+)@[^` + urlChars + `]`), []int{1}, nil, nil},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(`AKIA[0-9A-Z]{16}`), nil, nil, nil},
		{regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{36,}`), nil, nil, nil},
		{regexp.MustCompile(`eyJ[\w-]+\.eyJ[\w-]+\.[\w-]+`), nil, nil, nil},
		{regexp.MustCompile(`\b[Bb]earer[ \t]+[A-Za-z0-9._~+/=-]{8,}`), nil, []string{"earer"}, nil},
	}},
	{kindSecret, []pattern{
		{regexp.MustCompile(secretValue), []int{2, 3, 4, 5}, secretKeyHints, keepSecretValue},
		{regexp.MustCompile(secretAnnotated), []int{1, 2}, secretKeyHints, nil},
	}},
	{kindEmail, []pattern{
		{regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), nil, []string{"@"}, nil},
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
			if p.keep != nil && !p.keep(s, loc) {
				continue
			}
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
