package s3

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/google/uuid"
)

// TestArchiver_ArchivesWithTheDefaultChain proves the wiring main uses: an
// *objectstore.Client built without static keys, passed to the archiver as
// its objectstore.Writer role, signs the archive write with the credentials
// the SDK's default chain resolves instead of failing on empty static keys.
func TestArchiver_ArchivesWithTheDefaultChain(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.OK)
	client, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: "continuo",
	})
	if err != nil {
		t.Fatalf("objectstore.New: %v", err)
	}
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	th := domain.Thread{ID: uuid.New(), UserID: "alice", CreatedAt: now, UpdatedAt: now}

	if err := NewArchiver(client).ArchiveThread(context.Background(), th, nil); err != nil {
		t.Fatalf("ArchiveThread: %v", err)
	}

	reqs := srv.Requests()
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", r.Method)
	}
	if want := "/continuo/chat-archive/alice/" + th.ID.String() + ".json"; r.Path != want {
		t.Errorf("path = %s, want %s", r.Path, want)
	}
	if !strings.Contains(r.Authorization, "Credential="+objectstoretest.ChainAccessKeyID+"/") {
		t.Errorf("request not signed with the default chain's credentials: %q", r.Authorization)
	}
}
