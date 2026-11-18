// Package sanitize is the single redaction seam for untrusted text the system
// stores or forwards to an LLM. catalog stores the code versions of a release
// and the rejection excerpts of a failed one through Text. The remediation
// runner cleans every text it sends the model with a Redactor and restores the
// model's draft with the same Redactor.
//
// Text replaces every credential, private key and email address it finds with
// a fixed marker. A Redactor applies the same rules to raw text but gives each
// distinct value its own numbered placeholder and can put the values back, so a
// model can be shown a text without its secrets and its answer can be
// restored. A numbered placeholder a raw text holds is that text's own, and a
// Redactor numbers it as a value of its own.
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
//     password: str = "..." (secret). A quoted value is the whole literal: a
//     quote written twice, as SQL escapes one, or after a backslash stays
//     inside it, a triple quoted literal runs over lines to its closing
//     quotes, and a literal cut by the end of the text runs to that end. An
//     unquoted value is a literal only, not code that reads a value
//     (os.environ["TOKEN"], ${TOKEN}, or settings.PASSWORD written after an
//     operator with a space on each side), not a null or a boolean, not a
//     block scalar and not a type;
//  5. an email address (email).
//
// Every rule reads the text as Text writes it: the rules run over the text,
// each stretch they redact is replaced by its marker, and the result is
// searched again until nothing is found. When two rules match overlapping text
// the earlier match stands and the later one is dropped, so
// password=ops@acme.com is one secret and not a secret holding an email
// address. Two kinds of match take in what they reach. The value of a secret
// key takes in the earlier matches inside it, so password=postgres://u:pw@h is
// one secret and no part of a value is left beside the marker of another part.
// A match that reaches into a marker takes it in whole. A marker, fixed or
// numbered, is never matched by a rule, so Text of a text Text wrote changes
// nothing.
package sanitize

import (
	"regexp"
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

var markerAny = regexp.MustCompile(markerPattern)

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

// tripleQuoted is a literal between triple quotes q, as Python writes one: it
// runs over lines to the first three quotes q in a row, or to the end of the
// text when they are missing, and a backslash escapes the character after it.
// Its body is one capture group.
func tripleQuoted(q string) string {
	return q + q + q + `((?:` + q + `{0,2}(?:[^` + q + `\\]|\\(?s:.)))*` + q + `{0,2})(?:` + q + q + q + `|\z)`
}

// quoted is a literal between quotes q on one line, in which a quote is written
// twice, as SQL escapes one, or after a backslash. It runs to its closing
// quote, or to the end of the text when that is missing. Its body, which is
// not empty, is one capture group.
func quoted(q string) string {
	return q + `((?:[^` + q + `\\\r\n]|\\.|` + q + q + `)+)(?:` + q + `|\z)`
}

// stringPrefix is the prefix a Python string literal may carry: b"...",
// r'...', rb"""...""".
const stringPrefix = `[rRbBuUfF]{0,2}`

// secretValue is a secret key and its value. The separator is capture group 1.
// The value is one of, tried in this order: a triple quoted literal, nothing
// (an empty quoted string), a double or a single quoted literal, a double
// quoted literal a log escaped, or an unquoted run that ends at a space or a
// separator. A quoted value is matched as the whole literal, its escaped
// quotes included, after the string prefix it may carry. Each non-empty form
// holds the value without its quotes and prefix in a capture group of its own:
// a triple quoted literal in group 2 (double quotes) or 3 (single quotes), a
// quoted literal in group 4 or 5, a literal a log escaped in group 6 and an
// unquoted value in group 7, named bare. An unquoted value never starts with a
// colon (a cast, token::text) or an angle bracket (a hash rocket).
var secretValue = secretKey + `\\?["']?[ \t]*(=>|[=:]=?)[ \t]*(?:` +
	stringPrefix + `(?:` + tripleQuoted(`"`) + `|` + tripleQuoted(`'`) + `)` +
	`|""|''` +
	`|` + stringPrefix + `(?:` + quoted(`"`) + `|` + quoted(`'`) + `)` +
	`|\\"((?:[^"\\\r\n]|\\[^"])+)\\"` +
	`|(?P<bare>(?:` + markerPattern + `|[^\s,;)\]}&:>])(?:` + markerPattern + `|[^\s,;)\]}&"'])*))`

var (
	secretValueRE = regexp.MustCompile(secretValue)
	// bareGroup is the capture group of secretValue that holds an unquoted value.
	bareGroup = secretValueRE.SubexpIndex("bare")
)

// annotation is a type annotation: a name, optionally subscripted, or several
// of them joined by a bar.
const annotation = `[A-Za-z_][\w.]*(?:\[(?:[^\[\]\r\n=]|\[[^\[\]\r\n=]*\])*\])?`

// secretAnnotated is a secret key, an annotation and a quoted literal, as in
// password: str = "hunter2". The literal is matched whole, as in secretValue,
// and its body is capture group 1, 2, 3 or 4.
var secretAnnotated = secretKey + `[ \t]*:[ \t]*` + annotation + `(?:[ \t]*\|[ \t]*` + annotation + `)*` +
	`[ \t]*=[ \t]*` + stringPrefix +
	`(?:` + tripleQuoted(`"`) + `|` + tripleQuoted(`'`) + `|` + quoted(`"`) + `|` + quoted(`'`) + `)`

var (
	// A block scalar indicator opens a YAML value that is on the next lines.
	blockScalar = regexp.MustCompile(`^[|>](?:[0-9][+-]?|[+-][0-9]?)?$`)
	// The start of a call or a subscript: its name, possibly dotted, and its
	// opening bracket. os.getenv(, os.environ[.
	callStart = regexp.MustCompile(`^([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)[(\[]`)
	// A dotted name with no digit: self.password, settings.DB_PASSWORD. A
	// statement that ends with one may close on a colon (if x == cfg.token:).
	dottedName = regexp.MustCompile(`^[A-Za-z_]+(?:\.[A-Za-z_]+)+:?$`)
	// A name, possibly dotted, as a type is written: Custom, typing.Any.
	typeName = regexp.MustCompile(`^[A-Za-z_][\w.]*$`)
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

// maxCallLength bounds how far past its opening bracket a call is followed to
// its closing one.
const maxCallLength = 256

// readsAValue reports whether the unquoted value that starts at start is a call
// or a subscript that reads a value from somewhere else, such as
// os.environ["TOKEN"] or self.token_from(x), and not a secret that holds a
// bracket. It is a read when the name before the bracket is followed by a
// bracket and either a quote comes next, or the name holds no digit and the
// bracket closes on the same line, at the end of a word.
func readsAValue(s string, start int) bool {
	m := callStart.FindStringSubmatchIndex(s[start:])
	if m == nil {
		return false
	}
	open := start + m[1] - 1
	if next := open + 1; next < len(s) && (s[next] == '"' || s[next] == '\'') {
		return true
	}
	if strings.ContainsAny(s[start:start+m[3]], "0123456789") {
		return false
	}
	return closesAtTheEndOfAWord(s, open)
}

// closesAtTheEndOfAWord reports whether the bracket at open is closed on its
// line by the bracket that matches it, and what follows that closing bracket is
// the end of the text, a space or one of , ; ) ] } & . Quoted text inside the
// brackets is skipped.
func closesAtTheEndOfAWord(s string, open int) bool {
	var closers []byte
	limit := min(len(s), open+maxCallLength)
	for i := open; i < limit; i++ {
		switch c := s[i]; c {
		case '(':
			closers = append(closers, ')')
		case '[':
			closers = append(closers, ']')
		case '{':
			closers = append(closers, '}')
		case ')', ']', '}':
			if len(closers) == 0 || closers[len(closers)-1] != c {
				return false
			}
			closers = closers[:len(closers)-1]
			if len(closers) == 0 {
				return i+1 == len(s) || strings.IndexByte(" \t\r\n,;)]}&.", s[i+1]) >= 0
			}
		case '"', '\'':
			end := strings.IndexByte(s[i+1:limit], c)
			if end < 0 {
				return false
			}
			i += end + 1
		case '\n', '\r':
			return false
		}
	}
	return false
}

// operatorOfCode reports whether the separator s[start:end] of a match of
// secretValue is written as code writes an assignment or a comparison: an
// equals sign, a double equals sign, a walrus or a hash rocket, with a space
// or a tab on each side (a.token = b.token, token = self.token,
// x == cfg.token). A colon is how YAML and a JSON or JavaScript object write a
// value, and an equals sign with no space around it how a .env file, a shell,
// a connection string or a keyword argument does: both write a literal.
func operatorOfCode(s string, start, end int) bool {
	if s[start:end] == ":" {
		return false
	}
	return start > 0 && isBlank(s[start-1]) && end < len(s) && isBlank(s[end])
}

func isBlank(c byte) bool { return c == ' ' || c == '\t' }

// keepSecretValue decides whether a match of secretValue holds a value. A
// quoted value always does. An unquoted one does when it is a literal: it is
// not code that reads a value, not an absent value, not a block scalar, and,
// after a colon, not a type. A dotted name reads a value only after an
// operator of code (operatorOfCode). After any other separator it is a
// literal, since a password may be dotted words (correct.horse), and a dotted
// reference written there, as a JavaScript object or a keyword argument
// writes one, is redacted with it.
func keepSecretValue(s string, loc []int) bool {
	start, end := loc[2*bareGroup], loc[2*bareGroup+1]
	if start < 0 {
		return true
	}
	value := s[start:end]
	if _, absent := notAValue[strings.ToLower(value)]; absent {
		return false
	}
	if blockScalar.MatchString(value) || readsAValue(s, start) || expansion.MatchString(value) ||
		(dottedName.MatchString(value) && operatorOfCode(s, loc[2], loc[3])) {
		return false
	}
	if s[loc[2]:loc[3]] == ":" {
		if _, isType := typeWords[value]; isType {
			return false
		}
		// A name followed by an equals sign is an annotation of what follows.
		if typeName.MatchString(value) && strings.HasPrefix(strings.TrimLeft(s[end:], " \t"), "=") {
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

// rule is one redaction rule. A rule that takes in absorbs the earlier matches
// that lie inside one of its matches, which is how a value is redacted whole.
type rule struct {
	kind     kind
	patterns []pattern
	takesIn  bool
}

// rules are the redaction rules, in the order that decides which match stands
// when two overlap.
var rules = []rule{
	{kindKey, []pattern{
		{regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\z)`), nil, nil, nil},
	}, false},
	{kindSecret, []pattern{
		{regexp.MustCompile(`://(?:` + markerPattern + `|[^` + userinfoChars + `:])*:([^` + userinfoChars + `]+)@[^` + urlChars + `]`), []int{1}, nil, nil},
	}, false},
	{kindSecret, []pattern{
		{regexp.MustCompile(`AKIA[0-9A-Z]{16}`), nil, nil, nil},
		{regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{36,}`), nil, nil, nil},
		{regexp.MustCompile(`eyJ[\w-]+\.eyJ[\w-]+\.[\w-]+`), nil, nil, nil},
		{regexp.MustCompile(`\b[Bb]earer[ \t]+[A-Za-z0-9._~+/=-]{8,}`), nil, []string{"earer"}, nil},
	}, false},
	{kindSecret, []pattern{
		{secretValueRE, []int{2, 3, 4, 5, 6, bareGroup}, secretKeyHints, keepSecretValue},
		{regexp.MustCompile(secretAnnotated), []int{1, 2, 3, 4}, secretKeyHints, nil},
	}, true},
	{kindEmail, []pattern{
		{regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), nil, []string{"@"}, nil},
	}, false},
}

// span is a stretch of the text a rule redacts.
type span struct {
	start, end int
	kind       kind
}

// candidates are the spans the pattern finds in text, by position, as spans of
// kind k. lower is the lower case of the text.
func (p pattern) candidates(k kind, text, lower string) []span {
	if len(p.needs) > 0 && !containsAny(lower, p.needs) {
		return nil
	}
	var found []span
	for _, loc := range p.re.FindAllStringSubmatchIndex(text, -1) {
		if p.keep != nil && !p.keep(text, loc) {
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
			found = append(found, span{start, end, k})
		}
	}
	return found
}

func containsAny(s string, words []string) bool {
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// Text returns s with every private key, credential and email address replaced
// by its marker: [redacted:key], [redacted:secret] or [redacted:email]. It is a
// function of s alone.
func Text(s string) string {
	return render(redactPieces(s))
}
