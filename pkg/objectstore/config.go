package objectstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config names the store a Client talks to and the credentials it signs with.
type Config struct {
	EndpointURL     string // empty: AWS's endpoint; set: requests go to it, path-style (MinIO)
	Region          string
	Bucket          string // the default bucket
	AccessKeyID     string
	SecretAccessKey string
}

// ConfigFromS3 maps the object-storage settings pkg/config.LoadS3 reads
// (S3_ENDPOINT_URL, S3_BUCKET, AWS_DEFAULT_REGION, and the optional
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) onto a Config.
func ConfigFromS3(s pkgconfig.S3Config) Config {
	return Config{
		EndpointURL:     s.EndpointURL,
		Region:          s.Region,
		Bucket:          s.Bucket,
		AccessKeyID:     s.AccessKeyID,
		SecretAccessKey: s.SecretAccessKey,
	}
}

// awsConfig resolves the SDK configuration a Client signs with. When both key
// values are set it carries a static credential provider and loads nothing
// else (compose, MinIO). Otherwise it is the SDK's default configuration,
// whose credential chain resolves credentials from the environment, shared
// config, an IAM role or workload identity. The chain resolves lazily on first
// use, so startup does not depend on the identity endpoint being reachable. A
// default configuration that cannot be loaded is an error, so a service
// refuses to start.
func awsConfig(ctx context.Context, region, accessKeyID, secretKey string) (aws.Config, error) {
	if accessKeyID != "" && secretKey != "" {
		return aws.Config{
			Region:      region,
			Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
		}, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return aws.Config{}, fmt.Errorf("objectstore: load the default AWS configuration: %w", err)
	}
	return cfg, nil
}
