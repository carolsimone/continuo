package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/serialization"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/google/uuid"
)

// Archiver writes whole threads as JSON objects under chat-archive/ in the
// configured bucket.
type Archiver struct {
	client *awss3.Client
	bucket string
}

var _ ports.Archiver = (*Archiver)(nil)

// NewArchiver creates an Archiver backed by S3 or a MinIO endpoint.
// endpointURL: e.g. "http://minio:9000" (empty string → AWS default).
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
	Thread   domain.Thread     `json:"thread"`
	Messages []archivedMessage `json:"messages"`
}

// archivedMessage is the archive DTO for one message. Its content is the same
// JSON bytes the messages.content column stores (via serialization.Encode), so
// the domain Content union never reaches the archive as a bare interface and
// each archived message keeps its role-tagged content shape.
type archivedMessage struct {
	ID        uuid.UUID       `json:"ID"`
	ThreadID  uuid.UUID       `json:"ThreadID"`
	Seq       int             `json:"Seq"`
	Role      domain.Role     `json:"Role"`
	Content   json.RawMessage `json:"Content"`
	CreatedAt time.Time       `json:"CreatedAt"`
}

// ArchiveThread serialises the thread and its messages to S3 as a JSON object
// at chat-archive/<userID>/<threadID>.json.
func (a *Archiver) ArchiveThread(ctx context.Context, t domain.Thread, msgs []domain.Message) error {
	body, err := encodeArchive(t, msgs)
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

// encodeArchive builds the JSON body written to S3, encoding each message's
// content to its stored bytes. It is separated from the S3 call so the archive
// layout can be asserted without a bucket.
func encodeArchive(t domain.Thread, msgs []domain.Message) ([]byte, error) {
	archived := make([]archivedMessage, 0, len(msgs))
	for _, m := range msgs {
		content, err := serialization.Encode(m.Content)
		if err != nil {
			return nil, fmt.Errorf("encode archived message %s content: %w", m.ID, err)
		}
		archived = append(archived, archivedMessage{
			ID:        m.ID,
			ThreadID:  m.ThreadID,
			Seq:       m.Seq,
			Role:      m.Role,
			Content:   content,
			CreatedAt: m.CreatedAt,
		})
	}
	return json.Marshal(archivedThread{Thread: t, Messages: archived})
}
