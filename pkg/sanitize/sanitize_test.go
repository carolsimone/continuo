package sanitize_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/stretchr/testify/assert"
)

func TestText_PreservesContentByteForByte(t *testing.T) {
	in := "select 1 as x -- a comment\nfrom \"analytics\".orders\n"
	assert.Equal(t, in, sanitize.Text(in))
}

func TestText_EmptyStringIsEmpty(t *testing.T) {
	assert.Equal(t, "", sanitize.Text(""))
}
