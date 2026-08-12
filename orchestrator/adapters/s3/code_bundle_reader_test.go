package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An install running under an IAM role or workload identity supplies no static
// keys on purpose. A static provider built from empty strings fails every
// request with "static credentials are empty" instead of deferring to the SDK's
// default credential chain, so the config must leave Credentials unset.
func TestAWSConfig_OmitsStaticProviderWhenKeysAreAbsent(t *testing.T) {
	for _, tc := range []struct{ name, key, secret string }{
		{"both empty", "", ""},
		{"only key id", "AKIA", ""},
		{"only secret", "", "shh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Nil(t, awsConfig("us-east-1", tc.key, tc.secret).Credentials,
				"an incomplete key pair must leave the default credential chain reachable")
		})
	}
}

func TestAWSConfig_UsesStaticProviderWhenBothKeysArePresent(t *testing.T) {
	cfg := awsConfig("us-east-1", "AKIA", "shh")
	require.NotNil(t, cfg.Credentials)
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIA", creds.AccessKeyID)
	assert.Equal(t, "shh", creds.SecretAccessKey)
}

func TestParseS3URI(t *testing.T) {
	for _, tc := range []struct{ in, bucket, key string }{
		{"s3://continuo/code-bundles/rel-1/bundle.json", "continuo", "code-bundles/rel-1/bundle.json"},
		{"code-bundles/rel-1/bundle.json", "", "code-bundles/rel-1/bundle.json"},
		{"s3://continuo", "continuo", ""},
	} {
		b, k := parseS3URI(tc.in)
		assert.Equal(t, tc.bucket, b, tc.in)
		assert.Equal(t, tc.key, k, tc.in)
	}
}
