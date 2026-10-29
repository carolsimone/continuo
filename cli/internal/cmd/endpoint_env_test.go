package cmd

import (
	"bytes"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CONTINUO_RUN_CONTROLLER_ADDR is the address every run command dials: with no
// --endpoint, a command pointed at an address nothing listens on fails with
// the dial error naming that address.
func TestRunControllerAddrEnv_IsTheAddressRunCommandsDial(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	t.Setenv("CONTINUO_RUN_CONTROLLER_ADDR", addr)
	t.Setenv("CONTINUO_TIMEOUT", "1s")

	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"run", "show", "00000000-0000-0000-0000-000000000000"}, &out, &errBuf)

	assert.Equal(t, 5, exit, "stdout: %s", out.String())
	assert.Contains(t, out.String(), addr)
}
