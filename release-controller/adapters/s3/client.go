package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// S3Client is release-controller's S3 access: prefix deletion for prune, and object reads and writes for topology artifacts.
type S3Client struct {
	client *s3.Client
	bucket string
	logger *slog.Logger
}

// NewS3Client creates an S3Client for the given bucket.
// endpointURL: e.g. "http://minio:9000" (empty string → AWS default).
// Static credentials are attached only when both key values are supplied, so an
// install running under an IAM role or workload identity reaches the SDK's
// default credential chain.
func NewS3Client(endpointURL, bucket, region, accessKeyID, secretKey string, logger *slog.Logger) *S3Client {
	cfg := awsConfig(region, accessKeyID, secretKey)

	opts := []func(*s3.Options){
		func(o *s3.Options) { o.UsePathStyle = true },
	}
	if endpointURL != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
		})
	}

	return &S3Client{
		client: s3.NewFromConfig(cfg, opts...),
		bucket: bucket,
		logger: logger,
	}
}

// awsConfig builds the SDK config, attaching a static credential provider only
// when both key values are present.
func awsConfig(region, accessKeyID, secretKey string) aws.Config {
	cfg := aws.Config{Region: region}
	if accessKeyID != "" && secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, "")
	}
	return cfg
}

var _ ports.CandidateSQLDeleter = (*S3Client)(nil)

// DeletePrefix removes every object whose key begins with prefix. It
// paginates ListObjectsV2 and sends batched DeleteObjects requests. When
// nothing matches the prefix, it returns nil without issuing any delete calls.
// On a partial failure the error is logged as a warning and returned; the
// caller is expected to soft-fail.
func (c *S3Client) DeletePrefix(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			c.logger.Warn("s3 ListObjectsV2 failed", "prefix", prefix, "error", err)
			return fmt.Errorf("list objects prefix=%s: %w", prefix, err)
		}
		if len(page.Contents) == 0 {
			continue
		}

		toDelete := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: obj.Key})
		}

		out, err := c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &types.Delete{
				Objects: toDelete,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			c.logger.Warn("s3 DeleteObjects failed", "prefix", prefix, "error", err)
			return fmt.Errorf("delete objects prefix=%s: %w", prefix, err)
		}
		if len(out.Errors) > 0 {
			// Report the first error for context; remaining keys will be reclaimed by the lifecycle rule.
			e := out.Errors[0]
			c.logger.Warn("s3 DeleteObjects partial failure", "prefix", prefix, "failed_keys", len(out.Errors), "first_key", aws.ToString(e.Key), "first_code", aws.ToString(e.Code))
			return fmt.Errorf("delete objects prefix=%s key=%s: %s", prefix, aws.ToString(e.Key), aws.ToString(e.Message))
		}
	}
	return nil
}

// ErrObjectNotFound reports a read of a key that holds no object.
var ErrObjectNotFound = errors.New("s3 object not found")

// ErrObjectTooLarge reports an object larger than the reader accepts.
var ErrObjectTooLarge = errors.New("s3 object too large")

// ObjectStore is the object read/write surface the topology artifact store
// needs; *S3Client implements it and tests substitute an in-memory one.
type ObjectStore interface {
	GetObject(ctx context.Context, key string, maxBytes int64) ([]byte, error)
	PutObject(ctx context.Context, key string, body []byte, contentType string) error
}

var _ ObjectStore = (*S3Client)(nil)

// GetObject reads the object at key in the configured bucket, refusing one
// larger than maxBytes before holding it in memory. A missing key is
// ErrObjectNotFound; any transport or server error is returned wrapped, so a
// 5xx keeps its status code for the caller's classification.
func (c *S3Client) GetObject(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return nil, fmt.Errorf("s3 GetObject key=%s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	if out.ContentLength != nil && *out.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, over the %d-byte ceiling", ErrObjectTooLarge, key, *out.ContentLength, maxBytes)
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("s3 read key=%s: %w", key, err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: %s exceeds the %d-byte ceiling", ErrObjectTooLarge, key, maxBytes)
	}
	return body, nil
}

// PutObject writes body at key in the configured bucket.
func (c *S3Client) PutObject(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3 PutObject key=%s: %w", key, err)
	}
	return nil
}
