package http

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorRoundTrip(t *testing.T) {
	in := &repository.ListCursor{CreatedAt: time.Unix(100, 500).UTC(), RunID: "r-x"}
	out, err := decodeCursor(encodeCursor(in))
	require.NoError(t, err)
	assert.Equal(t, in.RunID, out.RunID)
	assert.True(t, in.CreatedAt.Equal(out.CreatedAt))
}

func TestDecodeCursorEmpty(t *testing.T) {
	c, err := decodeCursor("")
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestDecodeCursorMalformed(t *testing.T) {
	// valid base64 but no "|" separator
	bad := base64.RawURLEncoding.EncodeToString([]byte("nodelimiter"))
	_, err := decodeCursor(bad)
	require.Error(t, err)
}

func TestDecodeCursorBadTimestamp(t *testing.T) {
	// has "|" but left part is not an integer
	bad := base64.RawURLEncoding.EncodeToString([]byte("notanint|r-1"))
	_, err := decodeCursor(bad)
	require.Error(t, err)
}
