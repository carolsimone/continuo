package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/k8s-controller/service/ports"
)

// S3Client implements ports.LogUploader backed by AWS S3 / MinIO.
type S3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates an S3Client for the given bucket.
// endpointURL: e.g. "http://minio:9000" (empty string → AWS default)
func NewS3Client(endpointURL, bucket, region, accessKeyID, secretKey string) *S3Client {
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
	}
}

var _ ports.LogUploader = (*S3Client)(nil)

// UploadLog puts content at key in the configured bucket.
func (c *S3Client) UploadLog(ctx context.Context, key, content string) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(content),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("s3 PutObject key=%s: %w", key, err)
	}
	return nil
}
