package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readmeRow returns the README table row that documents the command with the
// given path: a line that starts with "| `<path>" followed by a space or the
// closing backtick, so "run rebase" does not match the "run rebase-preview" row.
func readmeRow(readme, path string) (string, bool) {
	prefix := "| `" + path
	for _, line := range strings.Split(readme, "\n") {
		if len(line) <= len(prefix) || !strings.HasPrefix(line, prefix) {
			continue
		}
		if next := line[len(prefix)]; next == ' ' || next == '`' {
			return line, true
		}
	}
	return "", false
}

// The README's command tables are the reader's list of commands. Every command
// in the catalog needs a row, and the "Changes state" column of the row must
// agree with the catalog's mutating flag, so a new or changed command cannot be
// left out of the README or described as read-only when it writes.
func TestReadme_HasARowForEveryCommandAndMarksTheOnesThatChangeState(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	require.NoError(t, err)
	readme := string(raw)

	p := runDescribe(t)
	require.NotEmpty(t, p.Commands)
	for _, c := range p.Commands {
		row, ok := readmeRow(readme, c.Path)
		if !assert.True(t, ok, "README.md has no table row for command %q", c.Path) {
			continue
		}
		marked := strings.HasSuffix(strings.TrimSpace(row), "| yes |")
		assert.Equal(t, c.Mutating, marked,
			"README.md row for %q: the Changes state column must be %q", c.Path, map[bool]string{true: "yes", false: "empty"}[c.Mutating])
	}
}
