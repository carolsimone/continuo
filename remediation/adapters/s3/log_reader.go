// Package s3 implements ports.LogReader over AWS SDK v2 S3 (LocalStack in dev).
package s3

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/carolsimone/continuo/remediation/service/ports"
)

// LogReader fetches dbt log objects from S3.
type LogReader struct {
	client        *awss3.Client
	defaultBucket string
}

var _ ports.LogReader = (*LogReader)(nil)

// NewLogReader builds an S3-backed LogReader. endpointURL empty → AWS default;
// non-empty (e.g. http://localstack:4566) → path-style addressing.
func NewLogReader(endpointURL, bucket, region, accessKeyID, secretKey string) *LogReader {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &LogReader{client: awss3.NewFromConfig(cfg, opts...), defaultBucket: bucket}
}

// Fetch returns the object body as a string. A missing object yields
// ports.ErrLogNotFound (callers classify it as log_unavailable).
func (r *LogReader) Fetch(ctx context.Context, uri string) (string, error) {
	if strings.TrimSpace(uri) == "" {
		return "", ports.ErrLogNotFound
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
			return "", ports.ErrLogNotFound
		}
		return "", err
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// parseS3URI splits "s3://bucket/key" into (bucket, key). A bare value with no
// scheme is treated as a key with an empty bucket (caller's default applies).
func parseS3URI(uri string) (bucket, key string) {
	if rest, ok := strings.CutPrefix(uri, "s3://"); ok {
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			return rest[:i], rest[i+1:]
		}
		return rest, ""
	}
	return "", uri
}
