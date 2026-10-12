package s3

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/google/uuid"
)

// TestArchivedMessage_MirrorsDomainMessageFields fails if domain.Message gains a
// field that archivedMessage does not carry. archivedMessage is a hand-written
// mirror so the archive keeps Message's default JSON layout while swapping the
// Content type for its stored bytes; a new domain field would otherwise be
// silently dropped from every archive.
func TestArchivedMessage_MirrorsDomainMessageFields(t *testing.T) {
	fieldNames := func(v any) map[string]bool {
		typ := reflect.TypeOf(v)
		out := make(map[string]bool, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			out[typ.Field(i).Name] = true
		}
		return out
	}
	domainFields := fieldNames(domain.Message{})
	archiveFields := fieldNames(archivedMessage{})

	for name := range domainFields {
		if !archiveFields[name] {
			t.Errorf("archivedMessage is missing domain.Message field %q — add it (and its json tag) so the archive keeps every field", name)
		}
	}
	for name := range archiveFields {
		if !domainFields[name] {
			t.Errorf("archivedMessage carries field %q that domain.Message no longer has — remove it", name)
		}
	}
}

// TestEncodeArchive_ContentStaysRoleTagged proves the archive body carries each
// message's content in its role-tagged JSON shape (via serialization.Encode),
// not the bare domain.Content interface. A domain value marshaled directly would
// emit its exported Go field names (e.g. `{"Text":...}`); the archive must keep
// the persisted `{"text":...}` / `{"call_id":...}` shape so archived threads
// stay readable with the same schema as the messages table.
func TestEncodeArchive_ContentStaysRoleTagged(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	th := domain.Thread{ID: uuid.New(), UserID: "alice", CreatedAt: now, UpdatedAt: now}
	msgs := []domain.Message{
		{ID: uuid.New(), ThreadID: th.ID, Seq: 1, Role: domain.RoleUser, Content: domain.TextContent{Text: "hi"}, CreatedAt: now},
		{ID: uuid.New(), ThreadID: th.ID, Seq: 2, Role: domain.RoleToolCall, Content: domain.ToolCallContent{CallID: "c1", Tool: "t", Args: map[string]string{}}, CreatedAt: now},
	}

	body, err := encodeArchive(th, msgs)
	if err != nil {
		t.Fatalf("encodeArchive: %v", err)
	}
	got := string(body)

	for _, want := range []string{
		`"Content":{"text":"hi"}`,
		`"Content":{"call_id":"c1","tool":"t","args":{}}`,
		`"Role":"user"`,
		`"Seq":1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("archive body missing %s\ngot: %s", want, got)
		}
	}

	// The bare domain field name must never appear — its presence means the
	// Content interface was marshaled directly instead of through the DTO.
	if strings.Contains(got, `"Text":`) {
		t.Errorf("archive leaked the domain Content shape (found \"Text\":)\ngot: %s", got)
	}
}

// TestArchiveThread_PutsTheArchiveAsJSON proves the archive lands at
// chat-archive/<user>/<thread>.json in the store's default bucket, as JSON
// carrying each message's role-tagged content.
func TestArchiveThread_PutsTheArchiveAsJSON(t *testing.T) {
	mem := objectstoretest.NewMemory("continuo")
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	th := domain.Thread{ID: uuid.New(), UserID: "alice", CreatedAt: now, UpdatedAt: now}
	msgs := []domain.Message{
		{ID: uuid.New(), ThreadID: th.ID, Seq: 1, Role: domain.RoleUser, Content: domain.TextContent{Text: "hi"}, CreatedAt: now},
	}

	if err := NewArchiver(mem).ArchiveThread(context.Background(), th, msgs); err != nil {
		t.Fatalf("ArchiveThread: %v", err)
	}

	puts := mem.Puts()
	if len(puts) != 1 {
		t.Fatalf("want 1 write, got %d", len(puts))
	}
	if want := (objectstore.Ref{Bucket: "continuo", Key: "chat-archive/alice/" + th.ID.String() + ".json"}); puts[0].Ref != want {
		t.Errorf("ref = %+v, want %+v", puts[0].Ref, want)
	}
	if puts[0].ContentType != "application/json" {
		t.Errorf("content type = %q, want application/json", puts[0].ContentType)
	}
	if !strings.Contains(string(puts[0].Body), `"Content":{"text":"hi"}`) {
		t.Errorf("archive body missing the message content: %s", puts[0].Body)
	}
}

// TestArchiveThread_ReturnsARefusedWrite proves a refused write reaches the
// retention job with its cause, so the thread is not deleted unarchived.
func TestArchiveThread_ReturnsARefusedWrite(t *testing.T) {
	mem := objectstoretest.NewMemory("continuo")
	th := domain.Thread{ID: uuid.New(), UserID: "alice"}
	refused := errors.New("access denied")
	mem.FailPut(objectstore.Ref{Key: "chat-archive/alice/" + th.ID.String() + ".json"}, refused)

	err := NewArchiver(mem).ArchiveThread(context.Background(), th, nil)
	if !errors.Is(err, refused) {
		t.Fatalf("ArchiveThread error = %v, want it to wrap %v", err, refused)
	}
	if len(mem.Puts()) != 0 {
		t.Errorf("a refused write stored %d objects", len(mem.Puts()))
	}
}
