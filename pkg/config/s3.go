package config

// S3Config holds S3/object-storage connection parameters.
type S3Config struct {
	EndpointURL     string
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

// LoadS3 reads S3 connection config from standard env vars.
// Tier 1 (required): S3_ENDPOINT_URL, S3_BUCKET, AWS_DEFAULT_REGION.
// Tier 2 (optional): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (empty = use IAM role).
func LoadS3(v *Validator) S3Config {
	return S3Config{
		EndpointURL:     v.Require("S3_ENDPOINT_URL"),
		Bucket:          v.Require("S3_BUCKET"),
		Region:          v.Require("AWS_DEFAULT_REGION"),
		AccessKeyID:     env("AWS_ACCESS_KEY_ID", ""),
		SecretAccessKey: env("AWS_SECRET_ACCESS_KEY", ""),
	}
}
