// Package s3 implements orchestrator's object-storage ports — the code-bundle
// reader and the topology-artifact reader — over AWS SDK v2 S3 (AWS, or MinIO in
// dev and single-node installs).
package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// newClient builds the S3 client every reader in this package uses. endpointURL
// empty → AWS default; non-empty (e.g. http://minio:9000) → path-style
// addressing against that endpoint.
//
// Static credentials are used when both accessKeyID and secretKey are set
// (compose / MinIO). Otherwise credentials come from the SDK's default
// credential chain: environment, shared config, IAM role, workload identity.
// A default chain that cannot be loaded is an error, so the service refuses to
// start rather than run a client that signs nothing.
func newClient(ctx context.Context, endpointURL, region, accessKeyID, secretKey string) (*awss3.Client, error) {
	cfg, err := awsConfig(ctx, region, accessKeyID, secretKey)
	if err != nil {
		return nil, err
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return awss3.NewFromConfig(cfg, opts...), nil
}

// awsConfig builds the SDK config. When both key values are present it carries
// a static credential provider and loads nothing else. Otherwise it is the SDK's
// default configuration, whose credential chain resolves environment variables,
// shared config, an IAM role or workload identity.
func awsConfig(ctx context.Context, region, accessKeyID, secretKey string) (aws.Config, error) {
	if accessKeyID != "" && secretKey != "" {
		return aws.Config{
			Region:      region,
			Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
		}, nil
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return aws.Config{}, fmt.Errorf("load the default AWS configuration: %w", err)
	}
	return cfg, nil
}

// parseS3URI splits "s3://bucket/key" into (bucket, key). A bare value with no
// scheme is treated as a key with an empty bucket, so the caller's default
// bucket applies.
func parseS3URI(uri string) (bucket, key string) {
	if rest, ok := strings.CutPrefix(uri, "s3://"); ok {
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			return rest[:i], rest[i+1:]
		}
		return rest, ""
	}
	return "", uri
}
