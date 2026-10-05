//go:build integration

package redis

import (
	"context"
	"os"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func TestRedrivePublisher_PublishXAddsTheStoredFields(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	ctx := context.Background()
	stream := "test.redrive:" + uuid.NewString()
	t.Cleanup(func() { rc.Del(ctx, stream) })

	dl := deadletter.DeadLetter{ID: uuid.New(), Source: deadletter.SourceConsumer, Group: "g",
		Fields: map[string]string{"k": "v", "outbox_entry_id": "original"}}
	body, _ := serialization.EncodeRedrive(dl)
	entry := &pkgoutbox.Entry{ID: uuid.New(), EventType: serialization.EventTypeRedrive, Payload: body, StreamName: stream}
	if err := NewRedrivePublisher(rc).Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	msgs, err := rc.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("msgs = %v, err = %v", msgs, err)
	}
	v := msgs[0].Values
	if v["k"] != "v" || v["outbox_entry_id"] != entry.ID.String() ||
		v[pkgredis.RedrivenFromField] != dl.ID.String() || v[pkgredis.RedriveGroupField] != "g" {
		t.Fatalf("values = %v", v)
	}
}
