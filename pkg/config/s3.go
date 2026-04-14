package config

// S3Config holds S3/object-storage connection parameters.
type S3Config struct {
	EndpointURL    string
	Bucket         string
	Region         string
	AccessKeyID    string
	SecretAccessKey string
}

// LoadS3 reads S3 connection config from standard env vars.
func LoadS3() S3Config {
	return S3Config{
		EndpointURL:    env("S3_ENDPOINT_URL", "http://localstack:4566"),
		Bucket:         env("S3_BUCKET", "continuo"),
		Region:         env("AWS_DEFAULT_REGION", "us-east-1"),
		AccessKeyID:    env("AWS_ACCESS_KEY_ID", "test"),
		SecretAccessKey: env("AWS_SECRET_ACCESS_KEY", "test"),
	}
}
