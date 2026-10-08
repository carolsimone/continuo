package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestClient points an S3Client at a local HTTP server, with retries off so
// a 5xx answer reaches the caller on the first attempt.
func newTestClient(t *testing.T, endpoint string) *S3Client {
	t.Helper()
	return &S3Client{
		client: awss3.NewFromConfig(awsConfig("us-east-1", "key", "secret"), func(o *awss3.Options) {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String(endpoint)
			o.RetryMaxAttempts = 1
		}),
		bucket: "continuo",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestS3Client_GetObjectReportsAMissingKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
	}))
	defer srv.Close()

	_, err := newTestClient(t, srv.URL).GetObject(context.Background(), "tenants/default/topologies/r1/topology.json.gz", 1024)
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

// An S3 outage must stay an infrastructure error all the way to the consumer,
// which then pauses and retries the message instead of counting a delivery.
func TestS3Client_GetObjectServerErrorIsInfrastructure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>ServiceUnavailable</Code><Message>slow down</Message></Error>`))
	}))
	defer srv.Close()

	_, err := newTestClient(t, srv.URL).GetObject(context.Background(), "k", 1024)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrObjectNotFound))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestS3Client_GetObjectRefusesAnObjectOverTheCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer srv.Close()

	_, err := newTestClient(t, srv.URL).GetObject(context.Background(), "k", 4)
	assert.ErrorIs(t, err, ErrObjectTooLarge)
}

func TestS3Client_PutObjectSendsTheBodyAndContentType(t *testing.T) {
	var gotPath, gotType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotType = r.URL.Path, r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload := []byte("\x1f\x8b gzipped topology")
	require.NoError(t, newTestClient(t, srv.URL).PutObject(context.Background(), "tenants/default/topologies/r1/topology.json.gz", payload, "application/gzip"))
	assert.Equal(t, "/continuo/tenants/default/topologies/r1/topology.json.gz", gotPath)
	assert.Equal(t, "application/gzip", gotType)
	assert.True(t, bytes.Contains(gotBody, payload), "the object body must be sent")
}

// An install running under an IAM role or workload identity supplies no static
// keys on purpose. A static provider built from empty strings fails every
// request instead of deferring to the SDK's default credential chain, so the
// config must leave Credentials unset.
func TestAWSConfig_OmitsStaticProviderWhenKeysAreAbsent(t *testing.T) {
	for _, tc := range []struct{ name, key, secret string }{
		{"both empty", "", ""},
		{"only key id", "AKIA", ""},
		{"only secret", "", "shh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Nil(t, awsConfig("us-east-1", tc.key, tc.secret).Credentials)
		})
	}
}

func TestAWSConfig_UsesStaticProviderWhenBothKeysArePresent(t *testing.T) {
	cfg := awsConfig("us-east-1", "AKIA", "shh")
	require.NotNil(t, cfg.Credentials)
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIA", creds.AccessKeyID)
}
