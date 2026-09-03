package failure

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realLog reads one of the captured dbt logs under testdata. Every file there
// is a verbatim log a real dbt Job wrote (three from production compile
// rejections, one from a local run in dbt's file-log format), never a
// hand-typed approximation: the banner defect this package guards against
// was invisible to every test that fed the classifier a one-line string.
func realLog(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name)) //nolint:gosec // G304: name is one of this package's own testdata fixtures, not external input.
	require.NoError(t, err)
	return string(b)
}

// TestKeyErrorLine_RealLogsYieldTheMessageNotTheBanner pins, per captured
// log, the exact text the signature and excerpt are derived from. dbt prints
// `[ERROR]: Encountered an error:` before every error it reports; the key
// line must be the message block that follows it, not that banner, or every
// failure of a node hashes to the same signature.
func TestKeyErrorLine_RealLogsYieldTheMessageNotTheBanner(t *testing.T) {
	cases := []struct {
		fixture string
		want    string
	}{
		{"compile_finance_rel-50ae918-135.log",
			`Compilation Error in model fx_transactions_eur (models/fx_transactions_eur.sql) unexpected char "'" at 38 line 1`},
		{"compile_service-2_rel-e67a66f-133.log",
			`Compilation Error in model table_gg (models/table_gg.sql) unexpected char "'" at 38 line 1`},
		{"compile_core_rel-3425c17-130.log",
			`Compilation Error in model dbt_daily_kpis (models/dbt_daily_kpis.sql) unexpected '}', expected ']' line 1`},
		{"compile_database_error_devlog.log",
			`Database Error connection to server at "localhost" (::1), port 5432 failed: Connection refused Is the server running on that host and accepting TCP/IP connections?`},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			got := keyErrorLine(realLog(t, tc.fixture))
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.want, Classify(realLog(t, tc.fixture)).Excerpt,
				"the excerpt recorded in the case base is the key line")
		})
	}
}

// TestKeyErrorLine_NeverReturnsBoilerplate is the guard for the defect class:
// whatever the log, the key line is never one of dbt's lead-in lines.
func TestKeyErrorLine_NeverReturnsBoilerplate(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, e := range entries {
		line := strings.ToLower(keyErrorLine(realLog(t, e.Name())))
		assert.False(t, strings.HasSuffix(line, "encountered an error:"), "%s: key line is the banner: %q", e.Name(), line)
		assert.False(t, strings.HasPrefix(line, "traceback"), "%s: key line is a traceback header: %q", e.Name(), line)
		assert.NotContains(t, line, "\x1b[", "%s: key line carries ANSI escapes: %q", e.Name(), line)
	}
}

// TestSignature_DistinctRealFailuresAreDistinct: the three production compile
// rejections are three different errors on three different models and must
// not share a signature — sharing one is what exhausted the attempt cap for
// unrelated failures.
func TestSignature_DistinctRealFailuresAreDistinct(t *testing.T) {
	fixtures := []string{
		"compile_finance_rel-50ae918-135.log",
		"compile_service-2_rel-e67a66f-133.log",
		"compile_core_rel-3425c17-130.log",
	}
	seen := map[string]string{}
	for _, f := range fixtures {
		sig := Classify(realLog(t, f)).Signature
		if prev, dup := seen[sig]; dup {
			t.Fatalf("%s and %s share signature %s", prev, f, sig)
		}
		seen[sig] = f
	}
}

// TestSignature_SameFailureIsStableAcrossRuns: the same error on a later run
// differs only in its timestamps, and must keep its signature — the property
// that lets the attempt cap and precedent lookup recognise a true repeat.
func TestSignature_SameFailureIsStableAcrossRuns(t *testing.T) {
	first := realLog(t, "compile_finance_rel-50ae918-135.log")
	require.Contains(t, first, "13:28:04")
	later := strings.NewReplacer("13:28:03", "09:01:58", "13:28:04", "09:01:59").Replace(first)
	require.NotEqual(t, first, later)
	assert.Equal(t, Classify(first).Signature, Classify(later).Signature)
}

// TestKeyErrorLine_BannerAloneFallsBackToTheBanner: a log that ends at the
// banner (the Job died before dbt printed the message) still yields a
// non-empty key line — the banner itself, with its ANSI colour codes removed.
func TestKeyErrorLine_BannerAloneFallsBackToTheBanner(t *testing.T) {
	log := "\x1b[0m13:28:03  Running with dbt=1.12.0-b1\n\x1b[0m13:28:04  [\x1b[31mERROR\x1b[0m]: Encountered an error:\n"
	assert.Equal(t, "13:28:04  [ERROR]: Encountered an error:", keyErrorLine(log))
}

// TestKeyErrorLine_MessageBlockStopsAtTheNextLogLine: the message block after
// the banner runs to the next timestamped log line, a blank line, or three
// lines, whichever comes first — so a debug line dbt prints afterwards never
// leaks into the signature.
func TestKeyErrorLine_MessageBlockStopsAtTheNextLogLine(t *testing.T) {
	log := "13:28:04  [ERROR]: Encountered an error:\n" +
		"Compilation Error in model a (models/a.sql)\n" +
		"13:28:05  Resource report: {\"command_success\": false}\n"
	assert.Equal(t, "Compilation Error in model a (models/a.sql)", keyErrorLine(log))
}

// TestExtractDbtFilePath_RealCompileLog: the offending path the compile fixer
// reads is still found in the captured production log.
func TestExtractDbtFilePath_RealCompileLog(t *testing.T) {
	assert.Equal(t, "models/fx_transactions_eur.sql", ExtractDbtFilePath(realLog(t, "compile_finance_rel-50ae918-135.log")))
}
