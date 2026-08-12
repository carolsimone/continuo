// Package s3 implements ports.CodeBundleReader over AWS SDK v2 S3 (LocalStack
// or MinIO in dev).
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/carolsimone/continuo/orchestrator/domain/codebundle"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
)

// CodeBundleReader fetches code-bundle documents from S3.
type CodeBundleReader struct {
	client        *awss3.Client
	defaultBucket string
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.CodeBundleReader = (*CodeBundleReader)(nil)

// NewCodeBundleReader builds an S3-backed CodeBundleReader. endpointURL empty →
// AWS default; non-empty (e.g. http://localstack:4566) → path-style addressing.
func NewCodeBundleReader(endpointURL, bucket, region, accessKeyID, secretKey string) *CodeBundleReader {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &CodeBundleReader{client: awss3.NewFromConfig(cfg, opts...), defaultBucket: bucket}
}

// Fetch reads and decodes the bundle at uri. A missing object yields
// ports.ErrBundleNotFound; an uninterpretable one yields ports.ErrBundleMalformed.
func (r *CodeBundleReader) Fetch(ctx context.Context, uri string) (codebundle.Bundle, error) {
	if strings.TrimSpace(uri) == "" {
		return codebundle.Bundle{}, ports.ErrBundleNotFound
	}
	bucket, key := parseS3URI(uri)
	if bucket == "" {
		bucket = r.defaultBucket
	}
	out, err := r.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return codebundle.Bundle{}, fmt.Errorf("%w: %s", ports.ErrBundleNotFound, uri)
		}
		return codebundle.Bundle{}, fmt.Errorf("get code bundle %s: %w", uri, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("read code bundle %s: %w", uri, err)
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("%w: %s: %v", ports.ErrBundleMalformed, uri, err)
	}
	return bundle, nil
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
