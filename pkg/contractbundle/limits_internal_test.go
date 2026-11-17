package contractbundle

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gzipped(t *testing.T, raw string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write([]byte(raw))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func checksum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestDecode_RefusesAnOversizedObjectAndDocument(t *testing.T) {
	t.Run("an object over the real object limit", func(t *testing.T) {
		obj := make([]byte, MaxObjectBytes+1)
		_, err := Decode(obj, checksum(obj))
		assert.ErrorIs(t, err, ErrTooLarge)
		assert.NotErrorIs(t, err, ErrMalformed)
	})

	// The document limit is lowered so the test does not build 512 MiB of JSON.
	const limit = 1024
	document := func(size int) string {
		const body = `{"schema_version":1,"release_id":"r"}`
		return body + strings.Repeat(" ", size-len(body))
	}
	t.Run("an object over a lowered object limit", func(t *testing.T) {
		obj := gzipped(t, document(limit))
		_, err := decode(obj, checksum(obj), len(obj)-1, limit)
		assert.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("a document at the lowered limit", func(t *testing.T) {
		obj := gzipped(t, document(limit))
		doc, err := decode(obj, checksum(obj), len(obj), limit)
		require.NoError(t, err)
		assert.Equal(t, "r", doc.ReleaseID)
	})
	t.Run("a document one byte over the lowered limit", func(t *testing.T) {
		obj := gzipped(t, document(limit+1))
		_, err := decode(obj, checksum(obj), len(obj), limit)
		assert.ErrorIs(t, err, ErrTooLarge)
		assert.NotErrorIs(t, err, ErrMalformed)
	})
}
