// Package s3 implements ports.EvidenceReader and ports.ArtifactWriter over AWS SDK v2 S3.
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

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// S3 reads evidence objects and writes proposal artifacts to S3 (or LocalStack in dev).
type S3 struct {
	client *awss3.Client
	bucket string
}

var _ ports.EvidenceReader = (*S3)(nil)
var _ ports.ArtifactWriter = (*S3)(nil)

// NewS3 builds an S3 client for the given bucket.
// endpointURL empty → AWS default; non-empty (e.g. http://localstack:4566) → path-style addressing.
func NewS3(endpointURL, bucket, region, accessKeyID, secretKey string) *S3 {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &S3{client: awss3.NewFromConfig(cfg, opts...), bucket: bucket}
}

// Fetch retrieves an S3 object by URI and returns its contents as a string.
// An empty URI or a missing object returns ports.ErrNotFound.
func (s *S3) Fetch(ctx context.Context, uri string) (string, error) {
	if strings.TrimSpace(uri) == "" {
		return "", ports.ErrNotFound
	}
	bucket, key := parseS3URI(uri)
	if bucket == "" {
		bucket = s.bucket
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return "", ports.ErrNotFound
		}
		return "", err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Write stores body at key in the configured bucket with the given contentType
// and returns the object URI as "s3://bucket/key".
func (s *S3) Write(ctx context.Context, key, body, contentType string) (string, error) {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("s3 PutObject key=%s: %w", key, err)
	}
	return "s3://" + s.bucket + "/" + key, nil
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
