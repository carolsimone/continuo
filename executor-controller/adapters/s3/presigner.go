// Package s3 signs time-limited URLs for objects in an S3-compatible store.
package s3

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
)

// Presigner implements ports.ObjectURLSigner against AWS S3 or LocalStack.
type Presigner struct {
	client *awss3.PresignClient
	logger *slog.Logger
}

var _ ports.ObjectURLSigner = (*Presigner)(nil)

// NewPresigner builds a presigner. endpointURL empty means the AWS default
// endpoint; non-empty (e.g. http://localstack:4566) is addressed path-style, so
// a bucket name never has to resolve as a hostname.
func NewPresigner(endpointURL, region, accessKeyID, secretKey string, logger *slog.Logger) *Presigner {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &Presigner{
		client: awss3.NewPresignClient(awss3.NewFromConfig(cfg, opts...)),
		logger: logger,
	}
}

// PresignGet signs a read of exactly the object s3URI names.
func (p *Presigner) PresignGet(ctx context.Context, s3URI string, ttl time.Duration) (string, error) {
	bucket, key, err := parseObjectURI(s3URI)
	if err != nil {
		return "", err
	}
	req, err := p.client.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, awss3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign get %s: %w", s3URI, err)
	}
	return req.URL, nil
}

// PresignPut signs an upload to exactly the object s3URI names. The signature
// covers the operation, the bucket and key, and the expiry, so the URL can only
// write that one object and only until it expires.
//
// contentType describes the upload the URL is minted for, but S3's query
// signing puts only the host in the signed header set, so it is advisory: the
// store does not reject an upload that declares a different type.
func (p *Presigner) PresignPut(ctx context.Context, s3URI, contentType string, ttl time.Duration) (string, error) {
	bucket, key, err := parseObjectURI(s3URI)
	if err != nil {
		return "", err
	}
	req, err := p.client.PresignPutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, awss3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign put %s: %w", s3URI, err)
	}
	return req.URL, nil
}

// parseObjectURI splits s3://bucket/key. It accepts only a reference to one
// object: a bucket with no key, or a key that names a prefix rather than an
// object, is rejected rather than signed into a capability wider than intended.
func parseObjectURI(s3URI string) (bucket, key string, err error) {
	rest, found := strings.CutPrefix(s3URI, "s3://")
	if !found {
		return "", "", fmt.Errorf("object URI must be s3://bucket/key")
	}
	bucket, key, found = strings.Cut(rest, "/")
	if !found || bucket == "" || key == "" || strings.HasSuffix(key, "/") {
		return "", "", fmt.Errorf("object URI must be s3://bucket/key")
	}
	return bucket, key, nil
}
