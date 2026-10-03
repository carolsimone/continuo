package redis

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scripts/bench/outage.sh counts the messages consumers abandon by grepping
// service logs. The consumer must log exactly one line it matches per
// dead-lettered message, and no other line it matches.
func TestDeadLetterLogLineMatchesBenchOutageCounter(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "bench", "outage.sh"))
	require.NoError(t, err, "scripts/bench/outage.sh must exist")
	m := regexp.MustCompile(`grep -cE '([^']+)'`).FindSubmatch(script)
	require.NotNil(t, m, "outage.sh no longer counts abandoned messages with grep -cE '<pattern>'")
	pattern := regexp.MustCompile(string(m[1]))

	assert.True(t, pattern.MatchString(logDeadLettered))
	assert.False(t, pattern.MatchString(logInfraPause))

	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)
	matches := 0
	for _, f := range sources {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		body, err := os.ReadFile(f) //nolint:gosec // G304: f comes from filepath.Glob over this package's own directory, not external input
		require.NoError(t, err)
		matches += len(pattern.FindAll(body, -1))
	}
	assert.Equal(t, 1, matches, "only the logDeadLettered constant may match the bench counter")
}
