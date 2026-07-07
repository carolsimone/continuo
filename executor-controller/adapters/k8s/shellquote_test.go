package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShellQuote(t *testing.T) {
	assert.Equal(t, "dbt", shellQuote("dbt"), "shell-safe strings stay unquoted")
	assert.Equal(t, "--profiles-dir", shellQuote("--profiles-dir"))
	assert.Equal(t, "/project/target/manifest.json", shellQuote("/project/target/manifest.json"))
	assert.Equal(t, "'has space'", shellQuote("has space"))
	assert.Equal(t, `'it'\''s'`, shellQuote("it's"), "embedded single quotes escaped")
	assert.Equal(t, "'a;b'", shellQuote("a;b"), "shell metacharacters force quoting")
	assert.Equal(t, "''", shellQuote(""), "empty element survives as empty arg")
}

func TestShellJoin_DefaultCompileLineByteIdentical(t *testing.T) {
	assert.Equal(t, "dbt compile --profiles-dir /project",
		shellJoin([]string{"dbt", "compile", "--profiles-dir", "/project"}),
		"the built-in compile argv must join to today's exact command line")
}
