package sanitize_test

import (
	"math/rand/v2"
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

// What Restore puts back is not read again: a value that holds text that looks
// like a placeholder of the Redactor comes back as it was. Only a private key
// block is taken whole when it holds such text.
func TestRedactor_RestoreDoesNotReadTheValuesItPutsBack(t *testing.T) {
	r := sanitize.NewRedactor()
	r.Clean("owner a@x.org")
	key := "-----BEGIN PRIVATE KEY-----\n[redacted:email:1]\n-----END PRIVATE KEY-----"
	cleaned := r.Clean("key " + key)
	require.Equal(t, "key [redacted:key:1]", cleaned)

	restored, unknown := r.Restore(cleaned)

	assert.Equal(t, "key "+key, restored)
	assert.Empty(t, unknown)
}

// A text that was cleaned already is its own clean: its placeholders stay, so
// cleaning twice changes nothing and Restore still inverts it.
func TestRedactor_CleaningACleanedTextChangesNothing(t *testing.T) {
	in := "dsn postgres://app:s3cr3t@db/x password=ops@acme.com mail bob@acme.com\n" + pemKey
	r := sanitize.NewRedactor()

	once := r.Clean(in)
	assert.Equal(t, once, r.Clean(once))

	restored, unknown := r.Restore(r.Clean(once))
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// A numbered placeholder is no secret key with a value: [redacted:secret:1]
// reads like the key secret holding 1, and no rule may take it so.
func TestRedactor_NoRuleMatchesAPlaceholder(t *testing.T) {
	for _, in := range []string{
		"[redacted:secret:1]", "[redacted:key:12]", "[redacted:email:3]",
		"x [redacted:secret:1], [redacted:secret:2]; y",
		`{"v": "[redacted:secret:1]"}`,
	} {
		assert.Equal(t, in, sanitize.NewRedactor().Clean(in))
		assert.Equal(t, in, sanitize.Text(in))
	}
}

// A text that holds a numbered placeholder of its own, a document that quotes
// one, keeps it: no later value is issued that number, so Restore never turns
// the literal into a value or a value into the literal.
func TestRedactor_ALiteralPlaceholderIsNeverTakenForAnIssuedOne(t *testing.T) {
	r := sanitize.NewRedactor()
	in := "doc literal [redacted:email:1] and owner a@x.org"

	cleaned := r.Clean(in)

	assert.Equal(t, "doc literal [redacted:email:1] and owner [redacted:email:2]", cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

func TestRedactor_ALiteralLaterInTheTextStillReservesItsNumber(t *testing.T) {
	r := sanitize.NewRedactor()
	in := "owner a@x.org, then the doc says [redacted:email:1] and [redacted:secret:3] and password=hunter2"

	cleaned := r.Clean(in)

	assert.Equal(t,
		"owner [redacted:email:2], then the doc says [redacted:email:1] and [redacted:secret:3] and password=[redacted:secret:4]",
		cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// A number the Redactor would never write (a leading zero, too large to count)
// is a literal too, and costs the numbering nothing.
func TestRedactor_ALiteralWithAnOddNumberIsKeptAndDoesNotMoveTheNumbering(t *testing.T) {
	r := sanitize.NewRedactor()
	in := "[redacted:email:01] [redacted:email:99999999999999999999999] a@x.org"

	cleaned := r.Clean(in)

	assert.Equal(t, "[redacted:email:01] [redacted:email:99999999999999999999999] [redacted:email:1]", cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// A literal whose number is large is kept as it is and moves no count, so the
// numbers issued after it never reach an overflow, and Restore stays exact.
func TestRedactor_ALiteralWithAHugeNumberMovesNoCount(t *testing.T) {
	for _, number := range []string{"1234567890", "9223372036854775806", "9223372036854775807", "18446744073709551615"} {
		r := sanitize.NewRedactor()
		in := "[redacted:email:" + number + "] a@x.org b@x.org"

		cleaned := r.Clean(in)

		assert.Equal(t, "[redacted:email:"+number+"] [redacted:email:1] [redacted:email:2]", cleaned, number)
		restored, unknown := r.Restore(cleaned)
		assert.Equal(t, in, restored, number)
		assert.Empty(t, unknown, number)
	}
}

// The largest number a literal reserves is nine digits; the number issued after
// it has ten and still restores.
func TestRedactor_ALiteralOfNineDigitsReservesItsNumber(t *testing.T) {
	r := sanitize.NewRedactor()
	in := "[redacted:email:999999999] a@x.org"

	cleaned := r.Clean(in)

	assert.Equal(t, "[redacted:email:999999999] [redacted:email:1000000000]", cleaned)
	restored, unknown := r.Restore(cleaned)
	assert.Equal(t, in, restored)
	assert.Empty(t, unknown)
}

// The literals of one text reserve their numbers for the texts after it.
func TestRedactor_ALiteralReservesItsNumberForTheNextTexts(t *testing.T) {
	r := sanitize.NewRedactor()
	require.Equal(t, "see [redacted:email:1]", r.Clean("see [redacted:email:1]"))

	cleaned := r.Clean("owner a@x.org")

	assert.Equal(t, "owner [redacted:email:2]", cleaned)
	restored, _ := r.Restore("see [redacted:email:1] / " + cleaned)
	assert.Equal(t, "see [redacted:email:1] / owner a@x.org", restored)
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

// Whatever the text, cleaning it twice changes nothing more than cleaning it
// once, and a Redactor restores the text it cleaned, one that holds
// placeholders of its own included. The only placeholders Restore may report
// are fixed markers (from Text), which the text held already.
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
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		once := sanitize.Text(in)
		if twice := sanitize.Text(once); twice != once {
			t.Fatalf("Text is not idempotent on %q: %q then %q", in, once, twice)
		}

		r := sanitize.NewRedactor()
		cleaned := r.Clean(in)
		if again := r.Clean(cleaned); again != cleaned {
			t.Fatalf("Clean is not idempotent on %q: %q then %q", in, cleaned, again)
		}
		restored, unknown := r.Restore(cleaned)
		if restored != in {
			t.Fatalf("Restore(Clean(%q)) = %q", in, restored)
		}
		for _, placeholder := range unknown {
			if strings.Count(placeholder, ":") != 1 || !strings.Contains(in, placeholder) {
				t.Fatalf("Restore(Clean(%q)) reports %q, which is not a fixed marker of the text", in, placeholder)
			}
		}
	})
}

// piecesOfText are the fragments the composition test strings together: the
// words, separators and secrets the rules read, and the markers they leave.
var piecesOfText = []string{
	"password", "PGPASSWORD", "token", "secret_key", "api-key", "=", ":", "=>", " ", "  ", "\t", "\n", ",", ";", "&", "?", "#", "/",
	"(", ")", "[", "]", "{", "}", "{{", "$(", "\"", "'", "\\\"", ".", "|", ">", "-", "x", "hunter2", "Tr0ub4dor", "abc.def", "os.getenv", "str",
	"None", "null", "Bearer ", "Bearer 00000000", awsKeyID, githubPAT, jwt, "ops@acme.com", "a@b.co", "@", "://", "https://u:pw@h.example.com/x",
	"-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----", "body",
	"[redacted:secret]", "[redacted:secret:1]", "[redacted:email:2]", "[redacted:key:1]", "[redacted:email:9223372036854775806]",
}

// separatorsOfText join the pieces: the whitespace and punctuation that set one
// value of a log or a file apart from the next.
var separatorsOfText = []string{" ", "\n", ", ", " ; ", " & ", "\t"}

// A text made of pieces in any order, set apart by separators: cleaning it twice
// changes nothing more, and a Redactor restores what it cleaned.
func TestRedactor_HoldsItsPropertiesOnTextsMadeOfPieces(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // G404: a fixed seed makes the texts the same on every run.
	for range 20000 {
		var in strings.Builder
		for i := range 1 + rng.IntN(14) {
			if i > 0 {
				in.WriteString(separatorsOfText[rng.IntN(len(separatorsOfText))])
			}
			in.WriteString(piecesOfText[rng.IntN(len(piecesOfText))])
		}
		text := in.String()

		once := sanitize.Text(text)
		require.Equal(t, once, sanitize.Text(once), "Text is not idempotent on %q", text)

		r := sanitize.NewRedactor()
		cleaned := r.Clean(text)
		require.Equal(t, cleaned, r.Clean(cleaned), "Clean is not idempotent on %q", text)
		restored, unknown := r.Restore(cleaned)
		require.Equal(t, text, restored, "Restore(Clean(%q))", text)
		for _, placeholder := range unknown {
			require.Equal(t, 1, strings.Count(placeholder, ":"), "%q reports %q", text, placeholder)
			require.Contains(t, text, placeholder)
		}
	}
}
