package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
)

// Archiver writes whole threads as JSON objects under chat-archive/ in the
// configured bucket.
type Archiver struct {
	client *awss3.Client
	bucket string
}

var _ ports.Archiver = (*Archiver)(nil)

// NewArchiver creates an Archiver backed by S3 or a LocalStack endpoint.
// endpointURL: e.g. "http://localstack:4566" (empty string → AWS default).
func NewArchiver(endpointURL, bucket, region, accessKeyID, secretKey string) *Archiver {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){
		func(o *awss3.Options) { o.UsePathStyle = true },
	}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &Archiver{client: awss3.NewFromConfig(cfg, opts...), bucket: bucket}
}

type archivedThread struct {
	Thread   domain.Thread    `json:"thread"`
	Messages []domain.Message `json:"messages"`
}

// ArchiveThread serialises the thread and its messages to S3 as a JSON object
// at chat-archive/<userID>/<threadID>.json.
func (a *Archiver) ArchiveThread(ctx context.Context, t domain.Thread, msgs []domain.Message) error {
	body, err := json.Marshal(archivedThread{Thread: t, Messages: msgs})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("chat-archive/%s/%s.json", t.UserID, t.ID)
	_, err = a.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("s3 PutObject key=%s: %w", key, err)
	}
	return nil
}
