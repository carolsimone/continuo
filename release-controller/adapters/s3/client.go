package s3

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// S3Client implements ports.CandidateSQLDeleter backed by AWS S3 / MinIO.
type S3Client struct {
	client *s3.Client
	bucket string
	logger *slog.Logger
}

// NewS3Client creates an S3Client for the given bucket.
// endpointURL: e.g. "http://minio:9000" (empty string → AWS default)
func NewS3Client(endpointURL, bucket, region, accessKeyID, secretKey string, logger *slog.Logger) *S3Client {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}

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
