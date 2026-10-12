package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/serialization"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/google/uuid"
)

// Archiver implements ports.Archiver over an object-store writer: it writes
// whole threads as JSON objects under chat-archive/ in the store's default
// bucket.
type Archiver struct {
	objects objectstore.Writer
}

var _ ports.Archiver = (*Archiver)(nil)

// NewArchiver builds an Archiver over objects.
func NewArchiver(objects objectstore.Writer) *Archiver {
	return &Archiver{objects: objects}
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

// ArchiveThread serialises the thread and its messages to object storage as a
// JSON object at chat-archive/<userID>/<threadID>.json. A failed write is
// returned with its cause; the store's error names the object, and the
// retention job adds the thread id.
func (a *Archiver) ArchiveThread(ctx context.Context, t domain.Thread, msgs []domain.Message) error {
	body, err := encodeArchive(t, msgs)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("chat-archive/%s/%s.json", t.UserID, t.ID)
	return a.objects.Put(ctx, objectstore.Ref{Key: key}, body, "application/json")
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
