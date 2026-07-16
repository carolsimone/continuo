package s3_test

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newPresigner() *s3.Presigner {
	return s3.NewPresigner("http://localstack:4566", "us-east-1", "test-key", "test-secret", testLogger())
}

// TestPresigner_PresignGetAddressesTheExactObject pins that the signed URL
// points at the one object it was asked for, path-style, so LocalStack and S3
// resolve it the same way.
func TestPresigner_PresignGetAddressesTheExactObject(t *testing.T) {
	signed, err := newPresigner().PresignGet(
		context.Background(), "s3://continuo/dbt-runs/artifacts/manifest.msgpack", 15*time.Minute)
	require.NoError(t, err)

	u, err := url.Parse(signed)
	require.NoError(t, err)
	assert.Equal(t, "localstack:4566", u.Host)
	assert.Equal(t, "/continuo/dbt-runs/artifacts/manifest.msgpack", u.Path)
	assert.NotEmpty(t, u.Query().Get("X-Amz-Signature"))
	assert.Equal(t, "900", u.Query().Get("X-Amz-Expires"))
}

// TestPresigner_PresignPutIsScopedToAnUploadOfTheExactObject pins that an
// upload URL carries the write it was minted for and nothing wider.
func TestPresigner_PresignPutIsScopedToAnUploadOfTheExactObject(t *testing.T) {
	signed, err := newPresigner().PresignPut(
		context.Background(), "s3://continuo/dbt-runs/s1/t1/l1/dbt.log", "text/plain", 15*time.Minute)
	require.NoError(t, err)

	u, err := url.Parse(signed)
	require.NoError(t, err)
	assert.Equal(t, "/continuo/dbt-runs/s1/t1/l1/dbt.log", u.Path)
	assert.NotEmpty(t, u.Query().Get("X-Amz-Signature"))
	assert.Equal(t, "PutObject", u.Query().Get("x-id"))
}

// TestPresigner_SignaturesDoNotCrossOperations pins that the HTTP method is
// inside the signature: a URL minted to read cannot be replayed to write.
func TestPresigner_SignaturesDoNotCrossOperations(t *testing.T) {
	ctx := context.Background()
	uri := "s3://continuo/dbt-runs/s1/t1/l1/dbt.log"

	get, err := newPresigner().PresignGet(ctx, uri, 15*time.Minute)
	require.NoError(t, err)
	put, err := newPresigner().PresignPut(ctx, uri, "text/plain", 15*time.Minute)
	require.NoError(t, err)

	getURL, err := url.Parse(get)
	require.NoError(t, err)
	putURL, err := url.Parse(put)
	require.NoError(t, err)

	assert.Equal(t, "GetObject", getURL.Query().Get("x-id"))
	assert.Equal(t, "PutObject", putURL.Query().Get("x-id"))
	assert.NotEqual(t, getURL.Query().Get("X-Amz-Signature"), putURL.Query().Get("X-Amz-Signature"))
}

// TestPresigner_ExpiryIsCarriedIntoTheSignature keeps the capability short
// lived: the TTL the caller asks for is the TTL the URL carries.
func TestPresigner_ExpiryIsCarriedIntoTheSignature(t *testing.T) {
	signed, err := newPresigner().PresignGet(
		context.Background(), "s3://continuo/a/b.json", 30*time.Second)
	require.NoError(t, err)

	u, err := url.Parse(signed)
	require.NoError(t, err)
	assert.Equal(t, "30", u.Query().Get("X-Amz-Expires"))
}

// TestPresigner_RejectsAnythingButAnObjectURI stops a malformed or
// bucket-only reference from being signed into a capability.
func TestPresigner_RejectsAnythingButAnObjectURI(t *testing.T) {
	cases := map[string]string{
		"empty":            "",
		"not s3":           "https://example.com/a/b",
		"no key":           "s3://continuo",
		"trailing slash":   "s3://continuo/",
		"no bucket":        "s3:///key",
		"scheme only":      "s3://",
		"directory prefix": "s3://continuo/dir/",
	}
	for name, uri := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newPresigner().PresignGet(context.Background(), uri, time.Minute)
			assert.Error(t, err)

			_, err = newPresigner().PresignPut(context.Background(), uri, "text/plain", time.Minute)
			assert.Error(t, err)
		})
	}
}

// TestPresigner_ErrorNeverEchoesTheSignedURL keeps a capability out of an error
// an operator or a client might see.
func TestPresigner_ErrorNeverEchoesTheSignedURL(t *testing.T) {
	_, err := newPresigner().PresignGet(context.Background(), "s3://continuo", time.Minute)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "X-Amz-Signature")
}
