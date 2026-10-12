package objectstore_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

func TestConfigFromS3_CarriesEverySetting(t *testing.T) {
	got := objectstore.ConfigFromS3(pkgconfig.S3Config{
		EndpointURL:     "http://minio:9000",
		Bucket:          "continuo",
		Region:          "eu-west-1",
		AccessKeyID:     "AKIASTATIC",
		SecretAccessKey: "static-secret",
	})
	assert.Equal(t, objectstore.Config{
		EndpointURL:     "http://minio:9000",
		Region:          "eu-west-1",
		Bucket:          "continuo",
		AccessKeyID:     "AKIASTATIC",
		SecretAccessKey: "static-secret",
	}, got)
}

// An install running under an IAM role or workload identity supplies no static
// keys on purpose. With either key absent the config resolves credentials
// through the SDK's default chain; environment credentials stand in for the
// chain here, and retrieving them proves the chain is wired in.
func TestAWSConfig_ResolvesTheDefaultChainWhenKeysAreAbsent(t *testing.T) {
	for _, tc := range []struct{ name, key, secret string }{
		{"both empty", "", ""},
		{"only key id", "AKIASTATIC", ""},
		{"only secret", "", "static-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyID := objectstoretest.UseChainCredentials(t)

			cfg, err := objectstore.AWSConfig(context.Background(), "us-east-1", tc.key, tc.secret)
			require.NoError(t, err)
			require.NotNil(t, cfg.Credentials)
			creds, err := cfg.Credentials.Retrieve(context.Background())
			require.NoError(t, err)
			assert.Equal(t, keyID, creds.AccessKeyID)
			assert.Equal(t, "us-east-1", cfg.Region)
		})
	}
}

// Explicit keys (the compose / MinIO path) take precedence over whatever the
// default chain would find.
func TestAWSConfig_StaticKeysOverrideTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)

	cfg, err := objectstore.AWSConfig(context.Background(), "us-east-1", "AKIASTATIC", "static-secret")
	require.NoError(t, err)
	require.NotNil(t, cfg.Credentials)
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIASTATIC", creds.AccessKeyID)
	assert.Equal(t, "static-secret", creds.SecretAccessKey)
}

// A default chain that cannot be loaded stops startup instead of yielding a
// client that signs nothing: a profile the shared config does not define is
// such a failure. Static keys never consult the shared config.
func TestNew_FailsClosedWhenTheDefaultConfigCannotLoad(t *testing.T) {
	objectstoretest.IsolateAWSEnvironment(t)
	cfgFile := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(cfgFile, []byte("[profile other]\nregion = us-east-1\n"), 0o600))
	t.Setenv("AWS_CONFIG_FILE", cfgFile)
	t.Setenv("AWS_PROFILE", "missing")

	_, err := objectstore.New(context.Background(), objectstore.Config{Region: "us-east-1", Bucket: "continuo"})
	assert.Error(t, err)

	_, err = objectstore.New(context.Background(), objectstore.Config{
		Region: "us-east-1", Bucket: "continuo", AccessKeyID: "AKIASTATIC", SecretAccessKey: "static-secret",
	})
	assert.NoError(t, err)
}

// Static keys name no region, so a client built from them without one would
// sign for nowhere; the default chain may resolve a region itself and is not
// held to this.
func TestNew_StaticCredentialsRequireARegion(t *testing.T) {
	objectstoretest.IsolateAWSEnvironment(t)
	cfg := objectstore.Config{Bucket: "continuo", AccessKeyID: "AKIASTATIC", SecretAccessKey: "static-secret"}

	_, err := objectstore.New(context.Background(), cfg)
	assert.Error(t, err)

	cfg.Region = "us-east-1"
	_, err = objectstore.New(context.Background(), cfg)
	assert.NoError(t, err)
}

func TestNew_RequiresADefaultBucket(t *testing.T) {
	objectstoretest.IsolateAWSEnvironment(t)

	_, err := objectstore.New(context.Background(), objectstore.Config{Region: "us-east-1", AccessKeyID: "AKIASTATIC", SecretAccessKey: "static-secret"})
	assert.Error(t, err)
}
