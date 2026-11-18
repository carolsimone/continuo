package sanitize_test

import (
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each distinct value gets one placeholder, numbered per kind in the order the
// Redactor first sees it, and the same value keeps its placeholder across
// texts.
func TestRedactor_NumbersEachDistinctValueOnce(t *testing.T) {
	r := sanitize.NewRedactor()

	assert.Equal(t,
		"[redacted:email:1] [redacted:email:2] [redacted:email:1]",
		r.Clean("a@x.org b@x.org a@x.org"))

	// A later text continues the numbering, and a value already seen keeps its
	// number.
	assert.Equal(t,
		"[redacted:email:3] [redacted:email:1]",
		r.Clean("c@x.org a@x.org"))

	// Each kind counts on its own.
	assert.Equal(t,
		"password=[redacted:secret:1] mail [redacted:email:2]\n[redacted:key:1]",
		r.Clean("password=hunter2 mail b@x.org\n"+pemKey))
	assert.Equal(t,
		"password=[redacted:secret:2] token: [redacted:secret:1]",
		r.Clean("password=other token: hunter2"))
}

// The number follows the position in the text, not the position of the rule
// that found the value.
func TestRedactor_NumbersByPositionInTheText(t *testing.T) {
	r := sanitize.NewRedactor()

	assert.Equal(t,
		"[redacted:secret:1] then password=[redacted:secret:2] then [redacted:secret:3]",
		r.Clean(awsKeyID+" then password=hunter2 then "+githubPAT))
}

func TestRedactor_TheRulesAreTheRulesOfText(t *testing.T) {
	in := "dsn postgres://app:s3cr3t@db/x password=ops@acme.com mail bob@acme.com\n" + pemKey + "\n" + awsKeyID
	got := sanitize.NewRedactor().Clean(in)

	assert.Equal(t,
		"dsn postgres://app:[redacted:secret:1]@db/x password=[redacted:secret:2] mail [redacted:email:1]\n"+
			"[redacted:key:1]\n[redacted:secret:3]",
		got)
}

func TestRedactor_RestoreReturnsTheOriginal(t *testing.T) {
	for name, in := range map[string]string{
		"every kind": "dsn postgres://app:s3cr3t@db/x password=hunter2 mail bob@acme.com\n" +
			pemKey + "\n" + awsKeyID + " " + jwt + " " + githubPAT,
		"a value used twice":       "a@x.org and again a@x.org",
		"a quoted value":           `{"api_key": "abc 123", "owner": "ops@acme.com"}`,
		"an escaped JSON value":    `{"msg":"{\"password\":\"hunter2\"}"}`,
		"a value with a dollar":    "password=$1abc&token=${x}",
		"a value with a backslash": `password=ab\1cd`,
		"nothing to clean":         "select 1 from t",
		"empty":                    "",
	} {
		t.Run(name, func(t *testing.T) {
			r := sanitize.NewRedactor()
			cleaned := r.Clean(in)

			restored, unknown := r.Restore(cleaned)
			assert.Equal(t, in, restored)
			assert.Empty(t, unknown)
		})
	}
}

// A placeholder stands for the same value in every text that carries it, so a
// draft that moves, repeats or drops placeholders restores each to its value.
func TestRedactor_RestorePutsEachPlaceholderBackWhereTheTextHasIt(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("owner a@x.org, admin b@x.org")

	restored, unknown := r.Restore("-- [redacted:email:2] first, [redacted:email:1] twice [redacted:email:1]")

	assert.Equal(t, "-- b@x.org first, a@x.org twice a@x.org", restored)
	assert.Empty(t, unknown)
}

// A placeholder the Redactor never issued is reported, in order of first
// appearance and once, and stays in the text. A fixed marker from Text is
// never restorable.
func TestRedactor_RestoreReportsAnUnknownPlaceholder(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("owner a@x.org")

	restored, unknown := r.Restore(
		"[redacted:email:9] [redacted:email:1] [redacted:secret] [redacted:email:9] [redacted:secret:1] [redacted:thing:4] [redacted:email]")

	assert.Equal(t,
		"[redacted:email:9] a@x.org [redacted:secret] [redacted:email:9] [redacted:secret:1] [redacted:thing:4] [redacted:email]",
		restored)
	assert.Equal(t,
		[]string{"[redacted:email:9]", "[redacted:secret]", "[redacted:secret:1]", "[redacted:thing:4]", "[redacted:email]"},
		unknown)
}

func TestRedactor_RestoreIgnoresTextThatOnlyLooksLikeAPlaceholder(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("owner a@x.org")

	for _, in := range []string{
		"[redacted]", "[redacted:email:]", "[redacted:email:x]", "[redacted:EMAIL:1]", "redacted:email:1", "[redacted:email:1",
	} {
		restored, unknown := r.Restore(in)
		assert.Equal(t, in, restored)
		assert.Empty(t, unknown, in)
	}
}

// A number is not the issued number unless it is the same digits.
func TestRedactor_RestoreDoesNotReadANumberLoosely(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("owner a@x.org")

	restored, unknown := r.Restore("[redacted:email:01] [redacted:email:1]")

	assert.Equal(t, "[redacted:email:01] a@x.org", restored)
	assert.Equal(t, []string{"[redacted:email:01]"}, unknown)
}

// A placeholder of one Redactor means nothing to another: each try and cluster
// owns its own.
func TestRedactor_APlaceholderBelongsToTheRedactorThatIssuedIt(t *testing.T) {
	first := sanitize.NewRedactor()
	cleaned := first.Clean("owner a@x.org")
	require.Equal(t, "owner [redacted:email:1]", cleaned)

	restored, unknown := sanitize.NewRedactor().Restore(cleaned)

	assert.Equal(t, cleaned, restored)
	assert.Equal(t, []string{"[redacted:email:1]"}, unknown)
}

// A numbered placeholder in a raw text is text of that text, even when it reads
// like one this Redactor issued for another text: Clean numbers it as a value
// of its own, so Restore of the second text gives the second text, never the
// first text's password.
func TestRedactor_APlaceholderInARawTextIsThatTextsOwnValue(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "password=[redacted:secret:1]", r.Clean("password=hunter2"))

	for _, tc := range []struct{ in, want string }{
		{"[redacted:secret:1]", "[redacted:secret:2]"},
		{"password=[redacted:secret:1]x", "password=[redacted:secret:3]"},
		{"see [redacted:secret:1] and password=[redacted:secret:1]", "see [redacted:secret:2] and password=[redacted:secret:2]"},
	} {
		cleaned := r.Clean(tc.in)

		assert.Equal(t, tc.want, cleaned, tc.in)
		restored, unknown := r.Restore(cleaned)
		assert.Equal(t, tc.in, restored)
		assert.NotContains(t, restored, "hunter2")
		assert.Empty(t, unknown)
	}

	// The first text still restores to its own password.
	restored, _ := r.Restore("password=[redacted:secret:1]")
	assert.Equal(t, "password=hunter2", restored)
}

// A value that takes in a placeholder of its raw text stands for the raw text,
// that placeholder included.
func TestRedactor_AValueThatTakesInAPlaceholderRestoresItsRawText(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "password=[redacted:secret:1]", r.Clean("password=hunter2"))
	in := "password=[redacted:secret:1]x\nurl=postgres://app:[redacted:secret:1]9@db/x"

	cleaned := r.Clean(in)

	assert.Equal(t, "password=[redacted:secret:2]\nurl=postgres://app:[redacted:secret:3]@db/x", cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// A private key block that holds a placeholder is taken whole and restored with
// its raw text.
func TestRedactor_AKeyBlockThatHoldsAPlaceholderRestoresItsRawText(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "owner [redacted:email:1]", r.Clean("owner a@x.org"))
	in := "key -----BEGIN PRIVATE KEY-----\n[redacted:email:1]\n-----END PRIVATE KEY-----"

	cleaned := r.Clean(in)

	assert.Equal(t, "key [redacted:key:1]", cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// What Restore puts back is not read again. The secret 1] glued after
// "[redacted:secret:" makes a key block whose raw text holds
// [redacted:secret:1]: Restore writes that text as it is, and does not read it
// as the placeholder of the secret 1].
func TestRedactor_RestoreDoesNotReadTheValuesItPutsBack(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, `password="[redacted:secret:1]"`, r.Clean(`password="1]"`))
	in := "key -----BEGIN PRIVATE KEY-----\n[redacted:secret:[redacted:secret:1]\n-----END PRIVATE KEY-----"
	cleaned := r.Clean(in)
	require.Equal(t, "key [redacted:key:1]", cleaned)

	restored, unknown := r.Restore(cleaned)

	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// No rule reads a numbered placeholder as a secret key with a value
// ([redacted:secret:1] as the key secret holding 1): Text leaves it, and a
// Redactor numbers the whole placeholder as one value.
func TestRedactor_NoRuleMatchesAPlaceholder(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"[redacted:secret:1]", "[redacted:secret:1]"},
		{"[redacted:key:12]", "[redacted:key:1]"},
		{"[redacted:email:3]", "[redacted:email:1]"},
		{"x [redacted:secret:7], [redacted:secret:2]; y", "x [redacted:secret:1], [redacted:secret:2]; y"},
		{`{"v": "[redacted:secret:4]"}`, `{"v": "[redacted:secret:1]"}`},
	} {
		assert.Equal(t, tc.in, sanitize.Text(tc.in))

		r := sanitize.NewRedactor()
		cleaned := r.Clean(tc.in)
		assert.Equal(t, tc.want, cleaned, tc.in)
		restored, unknown := r.Restore(cleaned)
		assert.Equal(t, tc.in, restored)
		assert.Empty(t, unknown)
	}
}

// Each numbered placeholder of a raw text, whatever its number (a leading zero,
// more digits than any count reaches), gets the next number of its kind in its
// place in the text, beside the values the rules redact.
func TestRedactor_NumbersAPlaceholderOfARawTextInItsPlace(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"doc literal [redacted:email:1] and owner a@x.org", "doc literal [redacted:email:1] and owner [redacted:email:2]"},
		{
			"owner a@x.org, then the doc says [redacted:email:1] and [redacted:secret:3] and password=hunter2",
			"owner [redacted:email:1], then the doc says [redacted:email:2] and [redacted:secret:1] and password=[redacted:secret:2]",
		},
		{"[redacted:email:01] [redacted:email:99999999999999999999999] a@x.org", "[redacted:email:1] [redacted:email:2] [redacted:email:3]"},
		{"[redacted:email:999999999] [redacted:email:1000000000] a@x.org", "[redacted:email:1] [redacted:email:2] [redacted:email:3]"},
		{"[redacted:thing:4] a@x.org", "[redacted:thing:1] [redacted:email:1]"},
	} {
		r := sanitize.NewRedactor()

		cleaned := r.Clean(tc.in)

		assert.Equal(t, tc.want, cleaned, tc.in)
		restored, unknown := r.Restore(cleaned)
		assert.Equal(t, tc.in, restored)
		assert.Empty(t, unknown)
	}
}

// The same placeholder text keeps its number in the texts after it, and a
// value issued later takes the next number.
func TestRedactor_APlaceholderOfARawTextKeepsItsNumberInLaterTexts(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "see [redacted:email:1]", r.Clean("see [redacted:email:7]"))

	assert.Equal(t, "again [redacted:email:1], owner [redacted:email:2]", r.Clean("again [redacted:email:7], owner a@x.org"))
	restored, unknown := r.Restore("[redacted:email:1] / [redacted:email:2]")
	assert.Equal(t, "[redacted:email:7] / a@x.org", restored)
	assert.Empty(t, unknown)
}

// Clean reads its input as raw text, so cleaning a cleaned text numbers its
// placeholders as text of their own, and Restore of that gives the cleaned
// text back.
func TestRedactor_CleaningACleanedTextNumbersItsPlaceholdersAsText(t *testing.T) {
	r := sanitize.NewRedactor()
	once := r.Clean("password=hunter2 mail bob@acme.com")
	require.Equal(t, "password=[redacted:secret:1] mail [redacted:email:1]", once)

	twice := r.Clean(once)

	assert.Equal(t, "password=[redacted:secret:2] mail [redacted:email:2]", twice)
	restored, unknown := r.Restore(twice)
	assert.Equal(t, once, restored)
	assert.Empty(t, unknown)
}

// A value made of placeholders issued before is stored as its raw text, so what
// a Redactor holds grows with its input, not with the values its placeholders
// stand for.
func TestRedactor_StoresNoMoreThanItsInput(t *testing.T) {
	r := sanitize.NewRedactor()
	first := "password=" + strings.Repeat("a", 1024)
	placeholder := strings.TrimPrefix(r.Clean(first), "password=")
	input := len(first)
	for range 3 {
		in := "password=" + strings.Repeat(placeholder, 64) + "x"
		input += len(in)
		cleaned := r.Clean(in)
		restored, _ := r.Restore(cleaned)
		require.Equal(t, in, restored)

		placeholder = strings.TrimPrefix(cleaned, "password=")
		value, _ := r.Restore(placeholder)
		require.LessOrEqual(t, len(value), len(in), "the value of %s", placeholder)
	}
	stored := 0
	for n := 1; ; n++ {
		p := fmt.Sprintf("[redacted:secret:%d]", n)
		value, unknown := r.Restore(p)
		if len(unknown) > 0 {
			break
		}
		stored += len(value)
	}
	assert.LessOrEqual(t, stored, input)
}

// A secret that a redacted part of a value takes whole is cleaned and restored
// in one piece: the Redactor numbers the whole value, never the part.
func TestRedactor_AValueThatTakesInARedactedPartIsNumberedWhole(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a key id and a tail":  {"password=AKIAIOSFODNN7EXAMPLEfoo", "password=[redacted:secret:1]"},
		"a bearer and a tail":  {"token:Bearer 00000000!", "token:[redacted:secret:1]"},
		"a marker and a tail":  {"password=[redacted:secret]hunter2", "password=[redacted:secret:1]"},
		"a URL under a secret": {"password=postgres://app:s3cr3t@db/x", "password=[redacted:secret:1]"},
	} {
		t.Run(name, func(t *testing.T) {
			r := sanitize.NewRedactor()

			cleaned := r.Clean(tc.in)

			assert.Equal(t, tc.want, cleaned)
			restored, unknown := r.Restore(cleaned)
			assert.Equal(t, tc.in, restored)
			assert.Empty(t, unknown)
		})
	}
}

// Text, then a Redactor over the stored text, is as clean as a Redactor over the
// text itself: the value is whole in both.
func TestRedactor_CleaningTheOutputOfTextLeavesNoTailOfAValue(t *testing.T) {
	for _, in := range []string{
		"password=AKIAIOSFODNN7EXAMPLEfoo",
		"token:Bearer 00000000!",
		"token=" + githubPAT + ".tail",
		"password=[redacted:secret]hunter2",
		"postgres://app:s3[redacted:secret]cr@db/x",
	} {
		cleaned := sanitize.NewRedactor().Clean(sanitize.Text(in))

		for _, tail := range []string{"foo", "!", ".tail", "hunter2", "s3", "cr@"} {
			assert.NotContains(t, strings.ReplaceAll(cleaned, "[redacted:secret:1]", ""), tail, in)
		}
	}
}

// A fixed marker from Text is left as it is and is not restorable.
func TestRedactor_LeavesAFixedMarkerFromTextAlone(t *testing.T) {
	r := sanitize.NewRedactor()
	cleaned := r.Clean(sanitize.Text("password=hunter2 mail a@x.org") + " and b@x.org")
	require.Equal(t, "password=[redacted:secret] mail [redacted:email] and [redacted:email:1]", cleaned)

	restored, unknown := r.Restore(cleaned)

	assert.Equal(t, "password=[redacted:secret] mail [redacted:email] and b@x.org", restored)
	assert.Equal(t, []string{"[redacted:secret]", "[redacted:email]"}, unknown)
}

// Two Redactors that clean the same texts in the same order issue the same
// placeholders.
func TestRedactor_IsDeterministic(t *testing.T) {
	texts := []string{
		"password=hunter2 mail a@x.org",
		"mail b@x.org password=hunter2",
		pemKey,
	}
	run := func() []string {
		r := sanitize.NewRedactor()
		out := make([]string, 0, len(texts))
		for _, text := range texts {
			out = append(out, r.Clean(text))
		}
		return out
	}

	want := []string{
		"password=[redacted:secret:1] mail [redacted:email:1]",
		"mail [redacted:email:2] password=[redacted:secret:1]",
		"[redacted:key:1]",
	}
	assert.Equal(t, want, run())
	assert.Equal(t, want, run())
}

func TestRedactor_CleanLeavesATextWithNoMatchUntouched(t *testing.T) {
	in := "select 1 as x -- a comment\nfrom \"analytics\".orders\n"
	r := sanitize.NewRedactor()

	assert.Equal(t, in, r.Clean(in))
	restored, unknown := r.Restore(in)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// Whatever the text, Text is idempotent, and a Redactor restores each raw text
// it cleaned, one that holds placeholders of its own included, also after it
// cleaned later texts that hold the placeholders it issued.
func FuzzCleanAndRestore(f *testing.F) {
	for _, seed := range []string{
		"", "select 1", pemKey, awsKeyID, githubPAT, jwt,
		"password=ops@acme.com", `{"token": "abc", "owner": "a@b.co"}`,
		"https://u:p@ss@h.example.com/x", "Bearer abcdefgh1234", "token: Bearer abcdefgh1234",
		`{"msg":"{\"password\":\"hunter2\"}"}`, "password=\"unterminated", "-----BEGIN PRIVATE KEY-----\nabc",
		"password=[redacted:secret]x", "a@b.co a@b.co", "secret_key==\"x\"", "token::text", "api-key:\t'k'",
		"doc literal [redacted:email:1] and owner a@x.org",
		"[redacted:secret:1] password=hunter2 [redacted:secret] a@b.co [redacted:email:2] [redacted:key:1]",
		"[redacted:email:01] [redacted:email:99999999999999999999999] a@b.co",
		`password: str = "hunter2"`, `api_key: Optional[str] = "sk-live"`, `token = os.environ["TOKEN"]`,
		"password: |\n  x", "https://example.com:443?email=a@b.com",
		"password: xK9(mP2q", "password=xK9(mP2)q", "PGPASSWORD=Ab[cd]9 psql", `token = os.getenv(KEY, "d")`,
		"[redacted:email:9223372036854775806] a@x.org b@x.org", "[redacted:email:999999999] a@x.org",
		"password=[redacted:secret:1]x", "password=hunter2", "password=hunter2 [redacted:secret:1]x url=postgres://app:[redacted:secret:1]9@db/x",
		"password = 'first''second'", `password = """hunter2"""`, "password = '''hunter2'''", `token = "a""b"`,
		"password = \"\"\"line\nline\"\"\" tail", `password = """cut`, "password='cut''tail", `password = b"x9"`,
		`password: str = """hunter2"""`, `password = """"""`, `token = """a"""" b`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		once := sanitize.Text(in)
		if twice := sanitize.Text(once); twice != once {
			t.Fatalf("Text is not idempotent on %q: %q then %q", in, once, twice)
		}

		r := sanitize.NewRedactor()
		cleaned, msg := cleansAndRestores(r, in)
		if msg != "" {
			t.Fatal(msg)
		}

		// Later texts on the same Redactor that hold the placeholders issued so
		// far, alone and glued to more value text, restore to themselves.
		if issued := issuedMarker.FindAllString(cleaned, -1); len(issued) > 0 {
			for _, later := range []string{gluedToIssued(issued) + "\n" + in, "password=" + issued[0] + in, cleaned} {
				if _, msg := cleansAndRestores(r, later); msg != "" {
					t.Fatal(msg)
				}
			}
			if restored, _ := r.Restore(cleaned); restored != in {
				t.Fatalf("after the later texts, Restore(Clean(%q)) = %q", in, restored)
			}
		}

		// A secret in front of the text is redacted whatever the text holds.
		for _, probe := range glueProbes {
			if strings.Contains(in, probe.secret) {
				continue
			}
			text := probe.text + " " + in
			for name, out := range map[string]string{
				"Text":             sanitize.Text(text),
				"Clean":            sanitize.NewRedactor().Clean(text),
				"Clean after Text": sanitize.NewRedactor().Clean(sanitize.Text(text)),
			} {
				if strings.Contains(out, probe.secret) {
					t.Fatalf("%s leaves %q of %q in %q", name, probe.secret, text, out)
				}
			}
		}
	})
}

// glueProbes are secrets a redacted part of a value sits against: a marker, a
// key id, a bearer token. The text after the probe cannot change what the
// rules make of it.
var glueProbes = []struct{ text, secret string }{
	{"password=[redacted:secret]Zq7canaryA", "Zq7canaryA"},
	{"password=[redacted:secret:1]Zq7canaryB", "Zq7canaryB"},
	{"postgres://app:Zq[redacted:secret]7canaryC@db/x", "7canaryC"},
	{"postgres://app:Zq7canaryD[redacted:secret:2]@db/x", "Zq7canaryD"},
	{"password=" + awsKeyID + "Zq7canaryE", "Zq7canaryE"},
	{"password=Zq7canaryF" + awsKeyID, "Zq7canaryF"},
	{"token=" + githubPAT + ".Zq7canaryG", "Zq7canaryG"},
	{"secret: " + jwt + "-Zq7canaryH", "Zq7canaryH"},
	{"token:Bearer 00000000Zq7canaryI!", "Zq7canaryI"},
	{"token:Bearer abcdefgh1234 Zq7canaryJ", "abcdefgh1234"},
	{"api_key: [redacted:email]Zq7canaryK", "Zq7canaryK"},
	{"password = 'Zq7''canaryL'", "canaryL"},
	{`token = """Zq7canaryM"""`, "Zq7canaryM"},
	{"secret = '''Zq7canaryN'''", "Zq7canaryN"},
}

// leakyPieces are texts that hold a secret, and the secret. The composition
// test checks that none of them is left in what it makes of a text.
var leakyPieces = []struct{ text, secret string }{
	{"password=hunter2", "hunter2"},
	{"PGPASSWORD=s3cr3t psql", "s3cr3t"},
	{"postgres://app:pw9xQ@db/x", "pw9xQ"},
	{"token: Bearer abcdefgh1234", "abcdefgh1234"},
	{"id " + awsKeyID, awsKeyID},
	{"clone " + githubPAT, githubPAT},
	{"session " + jwt, jwt},
	{"owner ops@acme.com", "ops@acme.com"},
	{"password=" + awsKeyID + "FooTailQ", "FooTailQ"},
	{"token=" + githubPAT + ".BarTailQ", "BarTailQ"},
	{"password=[redacted:secret]hunter2tail", "hunter2tail"},
	{"postgres://app:Zs3[redacted:secret:4]Qcr@db/x", "Zs3"},
	{"token:Bearer 00000000BazTailQ!", "BazTailQ"},
	{`{"password": "hunter2quoted"}`, "hunter2quoted"},
	{`password: str = "hunter2annotated"`, "hunter2annotated"},
	{"api_key=xK9(mP2q", "xK9(mP2q"},
	{"password = 'first''Zq7sqlTail'", "Zq7sqlTail"},
	{`token = "x""Zq7dqTail"`, "Zq7dqTail"},
	{`password = """Zq7tripleD"""`, "Zq7tripleD"},
	{"password = '''Zq7tripleS'''", "Zq7tripleS"},
	{`api_key = b"Zq7bytesLit"`, "Zq7bytesLit"},
	{`password: str = """Zq7annotTriple"""`, "Zq7annotTriple"},
	{pemKey, "MIIEowIBAAKCAQEAxJ5g1c0Zk3lQ"},
}

// neutralPieces are texts that hold no secret.
var neutralPieces = []string{
	"select", "from", "analytics.orders", "where", "a = b", "-- a comment", "x", "42",
	"password_hint", "token::text", "os.getenv(\"T\")", "[redacted:secret]", "[redacted:email:2]",
}

// Texts made of secrets and of text that holds none, in any order, set apart by
// separators: no secret is left in what Text, a Redactor, or a Redactor over
// the output of Text makes of them. One Redactor cleans several texts in turn,
// later ones holding the placeholders it issued for earlier ones, alone and
// glued to a secret, and restores each to its raw text.
func TestRedactor_LeavesNoSecretInATextMadeOfPieces(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5)) //nolint:gosec // G404: a fixed seed makes the texts the same on every run.
	for range 1000 {
		r := sanitize.NewRedactor()
		var issued []string
		var texts, cleaned []string
		for range 1 + rng.IntN(5) {
			var in strings.Builder
			var secrets []string
			for i := range 2 + rng.IntN(8) {
				if i > 0 {
					in.WriteString(separatorsOfText[rng.IntN(len(separatorsOfText))])
				}
				switch n := rng.IntN(5); {
				case n < 2:
					leaky := leakyPieces[rng.IntN(len(leakyPieces))]
					in.WriteString(leaky.text)
					secrets = append(secrets, leaky.secret)
				case n == 2 && len(issued) > 0:
					in.WriteString("password=" + issued[rng.IntN(len(issued))] + "Zq7glued")
					secrets = append(secrets, "Zq7glued")
				default:
					in.WriteString(neutralPieces[rng.IntN(len(neutralPieces))])
				}
			}
			text := in.String()

			outputs := map[string]string{
				"Text":             sanitize.Text(text),
				"Clean":            r.Clean(text),
				"Clean after Text": sanitize.NewRedactor().Clean(sanitize.Text(text)),
			}
			for name, out := range outputs {
				for _, secret := range secrets {
					require.NotContains(t, out, secret, "%s of %q", name, text)
				}
			}
			restored, _ := r.Restore(outputs["Clean"])
			require.Equal(t, text, restored)
			texts, cleaned = append(texts, text), append(cleaned, outputs["Clean"])
			issued = append(issued, issuedMarker.FindAllString(outputs["Clean"], -1)...)
		}
		for i := range texts {
			restored, _ := r.Restore(cleaned[i])
			require.Equal(t, texts[i], restored, "after the later texts")
		}
	}
}

// piecesOfText are the fragments the composition test strings together: the
// words, separators and secrets the rules read, and the markers they leave.
var piecesOfText = []string{
	"password", "PGPASSWORD", "token", "secret_key", "api-key", "=", ":", "=>", " ", "  ", "\t", "\n", ",", ";", "&", "?", "#", "/",
	"(", ")", "[", "]", "{", "}", "{{", "$(", "\"", "'", "''", `"""`, "'''", `b"`, "\\\"", ".", "|", ">", "-", "x", "hunter2", "Tr0ub4dor", "abc.def", "os.getenv", "str",
	"None", "null", "Bearer ", "Bearer 00000000", awsKeyID, githubPAT, jwt, "ops@acme.com", "a@b.co", "@", "://", "https://u:pw@h.example.com/x",
	"-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----", "body",
	"[redacted:secret]", "[redacted:secret:1]", "[redacted:email:2]", "[redacted:key:1]", "[redacted:email:9223372036854775806]",
}

// separatorsOfText join the pieces: the whitespace and punctuation that set one
// value of a log or a file apart from the next.
var separatorsOfText = []string{" ", "\n", ", ", " ; ", " & ", "\t"}

// Sequences of texts made of pieces in any order, set apart by separators, the
// later ones holding placeholders the Redactor issued for the earlier ones,
// alone and glued to more pieces: Text is idempotent on each, and one Redactor
// restores each to its raw text, also after it cleaned the texts after it.
func TestRedactor_HoldsItsPropertiesOnTextsMadeOfPieces(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // G404: a fixed seed makes the texts the same on every run.
	for range 5000 {
		r := sanitize.NewRedactor()
		var issued, texts, cleaned []string
		for range 1 + rng.IntN(4) {
			var in strings.Builder
			for i := range 1 + rng.IntN(14) {
				if i > 0 && rng.IntN(4) > 0 {
					in.WriteString(separatorsOfText[rng.IntN(len(separatorsOfText))])
				}
				if len(issued) > 0 && rng.IntN(4) == 0 {
					in.WriteString(issued[rng.IntN(len(issued))])
				}
				in.WriteString(piecesOfText[rng.IntN(len(piecesOfText))])
			}
			text := in.String()

			once := sanitize.Text(text)
			require.Equal(t, once, sanitize.Text(once), "Text is not idempotent on %q", text)

			out, msg := cleansAndRestores(r, text)
			require.Empty(t, msg)
			texts, cleaned = append(texts, text), append(cleaned, out)
			issued = append(issued, issuedMarker.FindAllString(out, -1)...)
		}
		for i := range texts {
			restored, _ := r.Restore(cleaned[i])
			require.Equal(t, texts[i], restored, "after the later texts")
		}
	}
}

var issuedMarker = regexp.MustCompile(`\[redacted:[a-z]+:[0-9]+\]`)

// gluedToIssued writes each placeholder glued to more value text, in the
// places the rules read a value: the value of a secret key, the password of a
// URL, a bearer token, an email address, and a placeholder glued to another.
func gluedToIssued(issued []string) string {
	var b strings.Builder
	for i, p := range issued {
		next := issued[(i+1)%len(issued)]
		b.WriteString("password=" + p + "x\nurl=postgres://app:" + p + "9@db/x\n" +
			"token: Bearer 00000000" + p + "\nsecret=" + p + next + "\nmail " + p + "ops@acme.com " + p + "\n")
	}
	return b.String()
}

// cleansAndRestores cleans a raw text with r and checks that Restore gives the
// text back, that every numbered placeholder of the cleaned text was issued by
// r and stands for a part of the text, so no stored value is larger than the
// text, and that the only placeholders Restore reports are fixed markers the
// text holds. It returns the cleaned text and what failed, or "".
func cleansAndRestores(r *sanitize.Redactor, text string) (string, string) {
	cleaned := r.Clean(text)
	restored, unknown := r.Restore(cleaned)
	if restored != text {
		return cleaned, fmt.Sprintf("Restore(Clean(%q)) = %q", text, restored)
	}
	for _, placeholder := range unknown {
		if strings.Count(placeholder, ":") != 1 || !strings.Contains(text, placeholder) {
			return cleaned, fmt.Sprintf("Restore(Clean(%q)) reports %q, which is not a fixed marker of the text", text, placeholder)
		}
	}
	for _, placeholder := range issuedMarker.FindAllString(cleaned, -1) {
		value, unknown := r.Restore(placeholder)
		if len(unknown) > 0 || !strings.Contains(text, value) {
			return cleaned, fmt.Sprintf("Clean(%q) holds %q, which stands for %q, not a part of the text", text, placeholder, value)
		}
	}
	return cleaned, ""
}
