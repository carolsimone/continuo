package objectstore

// Test-only access to unexported identifiers, for this package's external
// tests (package objectstore_test).
var (
	// NewClientWithOptions is New with extra S3 client options applied last.
	NewClientWithOptions = newClient
	// AWSConfig resolves the SDK configuration a Client signs with.
	AWSConfig = awsConfig
)
