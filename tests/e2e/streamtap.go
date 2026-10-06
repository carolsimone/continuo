package e2e

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// streamTap records every entry appended to a stream from the moment it starts.
// dead-letter-controller trims consumed entries every few minutes, so a test
// that inspects what a stream received starts a tap before triggering the work
// rather than reading the stream's history afterwards.
type streamTap struct {
	mu      sync.Mutex
	entries []goredis.XMessage
}

func startStreamTap(t *testing.T, ctx context.Context, rc *goredis.Client, stream string) *streamTap {
	t.Helper()
	from := "0-0"
	if info, err := rc.XInfoStream(ctx, stream).Result(); err == nil {
		from = info.LastGeneratedID
	}
	tap := &streamTap{}
	tctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for tctx.Err() == nil {
			res, err := rc.XRead(tctx, &goredis.XReadArgs{Streams: []string{stream, from}, Block: time.Second, Count: 500}).Result()
			if err != nil {
				if !errors.Is(err, goredis.Nil) && tctx.Err() == nil {
					time.Sleep(200 * time.Millisecond)
				}
				continue
			}
			for _, s := range res {
				tap.mu.Lock()
				tap.entries = append(tap.entries, s.Messages...)
				tap.mu.Unlock()
				if n := len(s.Messages); n > 0 {
					from = s.Messages[n-1].ID
				}
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	return tap
}

// Entries returns a copy of the entries recorded so far, in stream order.
func (s *streamTap) Entries() []goredis.XMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]goredis.XMessage(nil), s.entries...)
}
