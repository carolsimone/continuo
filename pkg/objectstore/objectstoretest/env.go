package objectstoretest

import (
	"path/filepath"
	"testing"
)

// ChainAccessKeyID is the access key id UseChainCredentials places where the
// SDK's default credential chain finds it.
const ChainAccessKeyID = "AKIAFROMCHAIN"

// IsolateAWSEnvironment keeps the SDK's default credential chain from reading
// the machine's shared config files, a profile, role, web-identity or
// container-credentials setting, an endpoint override, or the EC2 metadata
// endpoint.
func IsolateAWSEnvironment(t testing.TB) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, key := range []string{
		"AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_CA_BUNDLE",
	} {
		t.Setenv(key, "")
	}
}

// UseChainCredentials isolates the environment, then sets environment
// credentials, the first link of the SDK's default chain. A client built
// without static keys signs with them. It returns the access key id they
// carry, ChainAccessKeyID.
func UseChainCredentials(t testing.TB) string {
	t.Helper()
	IsolateAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", ChainAccessKeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
	return ChainAccessKeyID
}
