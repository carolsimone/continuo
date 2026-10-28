package run

import (
	"bytes"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestRunGroup_HoldsTheReadCommands(t *testing.T) {
	cmd := NewCommand(&config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})

	var names []string
	for _, c := range cmd.Commands() {
		names = append(names, c.Name())
	}
	assert.Subset(t, names, []string{"show", "graph", "attempts"})
}
