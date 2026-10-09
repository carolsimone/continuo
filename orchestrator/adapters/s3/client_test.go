package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolateAWSEnvironment keeps the default credential chain from reading the
// developer's shared config files or probing the EC2 metadata endpoint.
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
	_, err = NewCodeBundleReader(context.Background(), "", "continuo", "us-east-1", "", "")
	assert.Error(t, err)
	_, err = NewTopologyArtifactReader(context.Background(), "", "continuo", "us-east-1", "", "")
	assert.Error(t, err)
}

// recordingS3 answers every request with a NoSuchKey error and remembers the
// SigV4 credential scope each request was signed with.
type recordingS3 struct {
	*httptest.Server
	mu    sync.Mutex
	auths []string
}

func newRecordingS3(t *testing.T) *recordingS3 {
	t.Helper()
	r := &recordingS3{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.auths = append(r.auths, req.Header.Get("Authorization"))
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`))
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recordingS3) authorizations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auths...)
}

// Both readers are built by the one client constructor, so an install with no
// static keys signs every request with the credentials the default chain
// resolves, and a missing object keeps mapping to its not-found sentinel.
func TestReaders_SignWithTheDefaultChainWhenKeysAreAbsent(t *testing.T) {
	isolateAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMCHAIN")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
	srv := newRecordingS3(t)
	ctx := context.Background()

	bundles, err := NewCodeBundleReader(ctx, srv.URL, "continuo", "us-east-1", "", "")
	require.NoError(t, err)
	_, err = bundles.Fetch(ctx, "s3://continuo/code-bundles/rel-1/bundle.json")
	assert.ErrorIs(t, err, ports.ErrBundleNotFound)

	artifacts, err := NewTopologyArtifactReader(ctx, srv.URL, "continuo", "us-east-1", "", "")
	require.NoError(t, err)
	_, err = artifacts.Load(ctx, "s3://continuo/tenants/default/topologies/rel-1/topology.json.gz", "ab")
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactNotFound)

	auths := srv.authorizations()
	require.Len(t, auths, 2)
	for _, a := range auths {
		assert.Contains(t, a, "Credential=AKIAFROMCHAIN/")
	}
}
