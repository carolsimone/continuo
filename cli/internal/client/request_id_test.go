package client

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestRequestID_IsStableWithinAnInvocation(t *testing.T) {
	t.Setenv(requestIDEnv, "")
	first := RequestID()
	assert.Regexp(t, uuidV4, first, "a request id is a UUIDv4")
	assert.Equal(t, first, RequestID(), "every command of one process sends the same id")
}

func TestRequestID_ComesFromTheEnvironmentWhenSet(t *testing.T) {
	t.Setenv(requestIDEnv, "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b")
	assert.Equal(t, "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", RequestID())
}
