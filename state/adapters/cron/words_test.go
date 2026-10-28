package cron

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_DescribeTurnsAPanicOfTheWordsLibraryIntoAnError(t *testing.T) {
	p := &Parser{words: func(string) (string, error) { panic("index out of range [-1]") }}

	got, err := p.Describe("0 1 * * *")

	require.Error(t, err)
	assert.Empty(t, got)
	assert.Contains(t, err.Error(), "cannot be put in words")
}
