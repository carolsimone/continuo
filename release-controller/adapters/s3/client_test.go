package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	cfg, err := awsConfig(context.Background(), "us-east-1", "key", "secret")
	require.NoError(t, err)
	return &S3Client{
		client: awss3.NewFromConfig(cfg, func(o *awss3.Options) {
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

// A response that ends before its declared Content-Length is a transport
// failure, not a bad object: it must classify as infrastructure so the
// consumer pauses for storage recovery instead of dead-lettering the load.
func TestS3Client_GetObjectTruncatedBodyIsInfrastructure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("only ten b"))
		// Returning with fewer bytes than declared makes the server close the connection.
	}))
	defer srv.Close()

	_, err := newTestClient(t, srv.URL).GetObject(context.Background(), "k", 1024)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.False(t, errors.Is(err, ErrObjectNotFound))
	assert.False(t, errors.Is(err, ErrObjectTooLarge))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
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

// isolateAWSEnvironment keeps a test from reading the developer's shared
// config files or probing the EC2 metadata endpoint.
func isolateAWSEnvironment(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
}

// An install running under an IAM role or workload identity supplies no static
// keys on purpose. With the keys absent the config must resolve credentials
// through the SDK's default chain; the environment provider stands in for the
// chain here, and retrieving from it proves the chain is wired in.
func TestAWSConfig_ResolvesTheDefaultChainWhenKeysAreAbsent(t *testing.T) {
	for _, tc := range []struct{ name, key, secret string }{
		{"both empty", "", ""},
		{"only key id", "AKIASTATIC", ""},
		{"only secret", "", "static-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateAWSEnvironment(t)
			t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMCHAIN")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")

			cfg, err := awsConfig(context.Background(), "us-east-1", tc.key, tc.secret)
			require.NoError(t, err)
			require.NotNil(t, cfg.Credentials)
			creds, err := cfg.Credentials.Retrieve(context.Background())
			require.NoError(t, err)
			assert.Equal(t, "AKIAFROMCHAIN", creds.AccessKeyID)
			assert.Equal(t, "chain-secret", creds.SecretAccessKey)
			assert.Equal(t, "us-east-1", cfg.Region)
		})
	}
}

// Explicit keys (the compose / MinIO path) take precedence over whatever the
// default chain would find.
func TestAWSConfig_StaticKeysOverrideTheDefaultChain(t *testing.T) {
	isolateAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMCHAIN")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")

	cfg, err := awsConfig(context.Background(), "us-east-1", "AKIASTATIC", "static-secret")
	require.NoError(t, err)
	require.NotNil(t, cfg.Credentials)
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIASTATIC", creds.AccessKeyID)
	assert.Equal(t, "static-secret", creds.SecretAccessKey)
}

// A default chain that cannot be loaded stops startup instead of yielding a
// client that signs nothing: a profile the shared config does not define is
// such a failure.
func TestAWSConfig_FailsClosedWhenTheDefaultConfigCannotLoad(t *testing.T) {
	isolateAWSEnvironment(t)
	cfgFile := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(cfgFile, []byte("[profile other]\nregion = us-east-1\n"), 0o600))
	t.Setenv("AWS_CONFIG_FILE", cfgFile)
	t.Setenv("AWS_PROFILE", "missing")

	_, err := awsConfig(context.Background(), "us-east-1", "", "")
	assert.Error(t, err)
}
